"""OpenAI-hosted execution and recoverable observation of surveillance tasks.

The application API key stays in the caller. The sandbox receives only an
allowlisted code bundle, pinned dependencies, and the requested task. An observer
disconnect never causes a task to be resubmitted or labelled as cancelled.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import re
import tempfile
import zipfile
from collections.abc import Mapping
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4


DEFAULT_MODEL = "gpt-6-astra"
REMOTE_ROOT = "/workspace/app"
REMOTE_OUTPUTS = "/workspace/outputs"
MAX_BUNDLE_BYTES = 5 * 1024 * 1024
TERMINAL = frozenset({"completed", "failed", "cancelled", "deleted"})
RUNTIME_PACKAGES = frozenset(
    {"pandas", "requests", "beautifulsoup4", "python-dateutil", "openpyxl", "pypdf"}
)
DEFAULT_PACKAGES = (
    "pandas==2.2.3", "requests==2.34.2", "beautifulsoup4==4.15.0",
    "python-dateutil==4.0.0", "openpyxl==3.1.5", "pypdf==6.21.4",
)
OFFICIAL_DOMAINS = (
    "api.fda.gov", "www.fda.gov", "www.accessdata.fda.gov",
    "apps.tga.gov.au", "www.tga.gov.au", "tga.gov.au",
    "hpr-rps.hres.ca", "recalls-rappels.canada.ca", "www.canada.ca",
    "fsca.swissmedic.ch", "www.swissmedic.ch", "swissmedic.ch",
    "www.gov.uk", "assets.publishing.service.gov.uk", "www.mhra.gov.uk",
    "www.bfarm.de", "bfarm.de",
)
NETWORK_DOMAINS = (*OFFICIAL_DOMAINS, "pypi.org", "files.pythonhosted.org")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get(value: Any, name: str, default: Any = None) -> Any:
    return value.get(name, default) if isinstance(value, Mapping) else getattr(value, name, default)


def _safe_error(error: BaseException) -> dict[str, Any]:
    """Do not retain exception text: HTTP errors can contain submitted content."""
    result: dict[str, Any] = {"type": type(error).__name__}
    status = getattr(error, "status_code", None)
    if isinstance(status, int):
        result["http_status"] = status
    code = getattr(error, "code", None)
    if isinstance(code, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", code):
        result["code"] = code
    return result


def _state_file(path: Path) -> Path:
    path = Path(path).expanduser().absolute()
    if path.is_symlink():
        raise ValueError("The state file cannot be a symlink")
    return path


def _save_state(path: Path, state: dict[str, Any]) -> None:
    path = _state_file(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = _now()
    fd, temporary = tempfile.mkstemp(prefix=".agent-state-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _create_state(path: Path, state: dict[str, Any]) -> None:
    """Reserve the run before a remote POST; never overwrite another run's IDs."""
    path = _state_file(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _load_state(path: Path) -> dict[str, Any]:
    state = json.loads(_state_file(path).read_text(encoding="utf-8"))
    if not isinstance(state, dict) or state.get("schema_version") != 1 or not state.get("run_id"):
        raise ValueError("Invalid hosted run state")
    return state


def _allowed_bundle_path(name: str) -> bool:
    path = PurePosixPath(name)
    if (not name or "\\" in name or path.is_absolute() or ".." in path.parts
            or any(part.startswith(".") for part in path.parts)
            or str(path) != name):
        return False
    if name == "requirements-cloud.txt" or name == "cloud_agent/instructions.md":
        return True
    return len(path.parts) > 1 and path.parts[0] in {"cloud_agent", "pipeline"} and path.suffix == ".py"


def _bundle_files(bundle: Mapping[str, bytes | str] | Path) -> dict[str, bytes]:
    if isinstance(bundle, Mapping):
        files = {}
        for name, value in bundle.items():
            if not isinstance(name, str) or not _allowed_bundle_path(name):
                raise ValueError("The bundle contains a path outside the code allowlist")
            if not isinstance(value, (str, bytes)):
                raise TypeError("Bundle content must be text or bytes")
            files[name] = value.encode("utf-8") if isinstance(value, str) else value
    else:
        root = Path(bundle).resolve(strict=True)
        if not root.is_dir():
            raise ValueError("The bundle root must be a directory")
        files = {}
        for directory in ("cloud_agent", "pipeline"):
            folder = root / directory
            if not folder.is_dir() or folder.is_symlink():
                continue
            for path in sorted(folder.rglob("*.py")):
                if path.is_symlink() or any(p.is_symlink() for p in path.parents if p != root):
                    continue
                name = path.relative_to(root).as_posix()
                if path.is_file() and _allowed_bundle_path(name):
                    files[name] = path.read_bytes()
        for name in ("cloud_agent/instructions.md", "requirements-cloud.txt"):
            path = root / name
            if path.is_file() and not path.is_symlink():
                if any(p.is_symlink() for p in path.parents if p != root):
                    continue
                files[name] = path.read_bytes()
    if not files:
        raise ValueError("The code bundle is empty")
    if sum(map(len, files.values())) > 20 * 1024 * 1024:
        raise ValueError("The uncompressed code bundle exceeds 20 MiB")
    return files


def _packages(files: Mapping[str, bytes]) -> list[str]:
    raw = files.get("requirements-cloud.txt")
    if raw is None:
        return list(DEFAULT_PACKAGES)
    requirements = []
    for line in raw.decode("utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"([A-Za-z0-9_.-]+)==([A-Za-z0-9_.+!-]+)", line)
        if not match:
            raise ValueError("Cloud dependencies must use exact name==version pins")
        name = match.group(1).lower().replace("_", "-")
        if name in RUNTIME_PACKAGES:
            requirements.append(line)
    if {r.split("==", 1)[0].lower().replace("_", "-") for r in requirements} != RUNTIME_PACKAGES:
        raise ValueError("requirements-cloud.txt must pin all six surveillance runtime packages")
    return requirements


def build_environment(bundle: Mapping[str, bytes | str] | Path) -> tuple[dict[str, Any], str]:
    """Return the exact supported hosted-environment contract and bundle digest."""
    files = _bundle_files(bundle)
    content = io.BytesIO()
    with zipfile.ZipFile(content, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in sorted(files.items()):
            item = zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))
            item.compress_type = zipfile.ZIP_DEFLATED
            item.external_attr = 0o100644 << 16
            archive.writestr(item, data)
    payload = content.getvalue()
    if len(payload) > MAX_BUNDLE_BYTES:
        raise ValueError("The code archive exceeds the hosted inline-file limit of 5 MiB")
    return {
        "type": "openai_hosted",
        "container_size": "medium",
        "network": {"access": "restricted", "allowed_domains": list(NETWORK_DOMAINS)},
        "packages": {"python": _packages(files)},
        "files": [{
            "type": "inline", "path": "/workspace/ai_pro_search.zip",
            "data": base64.b64encode(payload).decode("ascii"),
        }],
        "setup_commands": [{"command": (
            "python -c \"import zipfile; from pathlib import Path; "
            "zipfile.ZipFile('/workspace/ai_pro_search.zip').extractall('/workspace/app'); "
            "Path('/workspace/outputs').mkdir(exist_ok=True)\""
        )}],
    }, hashlib.sha256(payload).hexdigest()


@contextmanager
def _client_scope(client: Any = None):
    if client is not None:
        yield client
        return
    from openai import OpenAI
    # A timed-out creation must be reconciled, never automatically duplicated.
    with OpenAI(max_retries=0, timeout=60.0) as owned:
        yield owned


def _apply_session(state: dict[str, Any], session: Any) -> None:
    session_id = _get(session, "id")
    if not session_id:
        raise ValueError("OpenAI returned no session ID")
    state["session_id"] = session_id
    environment = _get(session, "environment")
    if environment is not None:
        kind = _get(environment, "type")
        if kind != "openai_hosted":
            state["status"] = "failed"
            raise ValueError("The returned environment is not OpenAI-hosted")
        state["environment_id"] = _get(environment, "id")
    remote_status = _get(session, "status")
    state["session_status"] = remote_status
    if state.get("status") not in TERMINAL:
        if remote_status in {"failed", "requires_action"}:
            state["status"] = remote_status
        elif remote_status in {"in_progress", "queued", "provisioning"}:
            state["status"] = "in_progress"
    actions = _get(session, "required_actions", []) or []
    state["required_action_types"] = sorted({_get(action, "type", "unknown") for action in actions})


def _apply_turn(state: dict[str, Any], turn: Any) -> bool:
    if turn is None or _get(turn, "subagent_id") is not None:
        return False
    turn_id = _get(turn, "id")
    if not turn_id or state.get("turn_id") not in (None, turn_id):
        return False
    state["turn_id"] = turn_id
    state["status"] = _get(turn, "status", state.get("status", "submitted"))
    if state["status"] == "failed":
        state["error"] = {"code": "hosted_turn_failed"}
        code = _get(_get(turn, "error"), "code")
        if isinstance(code, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", code):
            state["error"]["code"] = code
    elif state["status"] == "completed":
        state.pop("error", None)
    if state["status"] in TERMINAL:
        state["finished_at"] = _now()
    return True


def _read_output(client: Any, state: dict[str, Any]) -> None:
    if state.get("status") != "completed" or not state.get("turn_id"):
        return
    messages = []
    for item in client.beta.agents.sessions.turns.items.list(
        state["turn_id"], session_id=state["session_id"], order="asc", limit=100,
    ):
        if (_get(item, "type") != "message" or _get(item, "status") != "completed"
                or _get(item, "role") != "assistant" or _get(item, "phase") == "commentary"
                or _get(item, "turn_id") != state["turn_id"]):
            continue
        for part in _get(item, "content", []) or []:
            if _get(part, "type") == "output_text":
                messages.append(_get(part, "text", ""))
    state["output_text"] = "\n".join(messages)


def _recover_session_id(client: Any, state: dict[str, Any]) -> bool:
    if state.get("session_id"):
        return True
    if state.get("status") == "failed":
        return False
    matches = list(client.beta.agents.sessions.list(metadata={"run_id": state["run_id"]}, limit=100))
    if len(matches) > 1:
        state["status"] = "ambiguous_submission"
        state["error"] = {"code": "multiple_sessions_for_run"}
        return False
    if not matches:
        state["status"] = "launch_unknown"
        return False
    _apply_session(state, matches[0])
    return True


def _refresh(client: Any, state: dict[str, Any]) -> dict[str, Any]:
    if state.get("status") == "deleted":
        return state
    if not _recover_session_id(client, state):
        return state
    session = client.beta.agents.sessions.retrieve(state["session_id"])
    _apply_session(state, session)
    if state.get("turn_id"):
        turn = client.beta.agents.sessions.turns.retrieve(state["turn_id"], session_id=state["session_id"])
        _apply_turn(state, turn)
    else:
        # A new session belongs exclusively to this run. Select its FIRST root
        # turn, not a later turn someone might have submitted during recovery.
        for turn in client.beta.agents.sessions.turns.list(state["session_id"], order="asc", limit=100):
            if _apply_turn(state, turn):
                break
    if _get(session, "status") == "failed" and state["status"] not in TERMINAL:
        state["status"] = "failed"
        state["error"] = {"code": "hosted_session_failed"}
    _read_output(client, state)
    return state


def _event(state: dict[str, Any], event: Any) -> None:
    kind = _get(event, "type", "")
    session = _get(event, "session")
    if session is not None:
        _apply_session(state, session)
    _apply_turn(state, _get(event, "turn"))
    if kind in {"agent.session.failed", "agent.session.environment.failed"}:
        state["status"] = "failed"
        state["error"] = {"code": kind}
    elif kind == "agent.session.requires_action":
        state["status"] = "requires_action"
    elif kind == "error":
        # An event-level error does not establish that the remote turn failed.
        state["error"] = {"code": "stream_error_event"}


def start_task(
    task: str,
    bundle: Mapping[str, bytes | str] | Path,
    state_path: Path,
    *,
    instructions: str,
    client: Any = None,
    model: str = DEFAULT_MODEL,
    agent_id: str | None = None,
    wait: bool = True,
) -> dict[str, Any]:
    """Start exactly one hosted task and save enough state to recover it.

    With ``wait=False`` this returns after creation; the agent continues in
    OpenAI's cloud. Reuse ``resume_task`` after connection errors, never call
    ``start_task`` again with a fresh state file for the same uncertain run.
    """
    if not task.strip() or not instructions.strip():
        raise ValueError("A task and agent instructions are required")
    environment, digest = build_environment(bundle)
    state: dict[str, Any] = {
        "schema_version": 1, "run_id": uuid4().hex, "created_at": _now(),
        "status": "submitting", "session_id": None, "turn_id": None,
        "environment_id": None, "model": model, "agent_id": agent_id,
        "bundle_sha256": digest, "task_sha256": hashlib.sha256(task.encode()).hexdigest(),
        "observer_status": "connected", "output_text": "", "submission_attempted": False,
    }
    _create_state(state_path, state)
    arguments: dict[str, Any] = {
        "environment": environment,
        "agent": {
            "model": model, "instructions": instructions,
            "tools": [{"type": "web_search", "mode": "live", "allowed_domains": list(OFFICIAL_DOMAINS)}],
        },
        "input": task,
        "metadata": {"application": "ai-pro-search", "run_id": state["run_id"]},
        "stream": wait,
    }
    if agent_id:
        arguments["agent_id"] = agent_id
    try:
        with _client_scope(client) as active:
            # Also disable a caller-injected SDK client's automatic POST retries.
            create_client = active.with_options(max_retries=0) if hasattr(active, "with_options") else active
            state["submission_attempted"] = True
            _save_state(state_path, state)
            if not wait:
                session = create_client.beta.agents.sessions.create(**arguments)
                _apply_session(state, session)
                if state["status"] == "submitting":
                    state["status"] = "submitted"
                state["observer_status"] = "detached"
            else:
                with create_client.beta.agents.sessions.create(**arguments).with_result_collection() as events:
                    for event in events:
                        _event(state, event)
                        _save_state(state_path, state)
                    result = events.get_final_result()
                    _apply_turn(state, result.turn)
                    state["output_text"] = result.output_text
                state["observer_status"] = "finished"
    except Exception as error:
        state["error"] = _safe_error(error)
        state["observer_status"] = "disconnected"
        if not state.get("session_id"):
            status = getattr(error, "status_code", None)
            state["status"] = "failed" if (
                not state["submission_attempted"] or status in {400, 401, 403, 404, 422, 429}
            ) else "launch_unknown"
    _save_state(state_path, state)
    return state


def get_status(state_path: Path, *, client: Any = None) -> dict[str, Any]:
    """Inspect the persisted intended turn without sending any new input."""
    state = _load_state(state_path)
    try:
        with _client_scope(client) as active:
            _refresh(active, state)
    except Exception as error:
        state["error"] = _safe_error(error)
        state["observer_status"] = "disconnected"
    _save_state(state_path, state)
    return state


def resume_task(state_path: Path, *, client: Any = None, wait: bool = True) -> dict[str, Any]:
    """Reconnect to the same task; this never sends the user input again."""
    if not wait:
        return get_status(state_path, client=client)
    state = _load_state(state_path)
    if state.get("status") == "deleted":
        return state
    try:
        with _client_scope(client) as active:
            if not _recover_session_id(active, state):
                _save_state(state_path, state)
                return state
            # Subscribe first, then reconcile persisted state. The open stream
            # buffers events generated while the read requests are in flight.
            with active.beta.agents.sessions.events.stream(state["session_id"]) as events:
                _refresh(active, state)
                _save_state(state_path, state)
                if state["status"] in TERMINAL or state["status"] == "requires_action":
                    state["observer_status"] = "finished" if state["status"] in TERMINAL else "detached"
                    _save_state(state_path, state)
                    return state
                state["observer_status"] = "connected"
                for event in events:
                    _event(state, event)
                    _save_state(state_path, state)
                    if state["status"] in TERMINAL or state["status"] == "requires_action":
                        break
                _refresh(active, state)
                state["observer_status"] = "finished" if state["status"] in TERMINAL else "disconnected"
    except Exception as error:
        state["error"] = _safe_error(error)
        state["observer_status"] = "disconnected"
    _save_state(state_path, state)
    return state


def cancel_task(state_path: Path, *, client: Any = None) -> dict[str, Any]:
    state = _load_state(state_path)
    with _client_scope(client) as active:
        _refresh(active, state)
        if state["status"] not in TERMINAL and state.get("session_id"):
            active.beta.agents.sessions.events.create(
                state["session_id"], events=[{"type": "agent.session.input.cancel"}],
            )
            state["cancellation_requested_at"] = _now()
            # A request is not proof of cancellation. Preserve the last known
            # turn status until the server reports its terminal outcome.
            _refresh(active, state)
    _save_state(state_path, state)
    return state


def _safe_component(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", value):
        raise ValueError("Unsafe remote resource identifier")
    return value


def collect_artifacts(state_path: Path, output_dir: Path, *, client: Any = None) -> list[Path]:
    """Download only artifacts belonging to this run's completed root turn."""
    state = _load_state(state_path)
    downloaded: list[Path] = []
    manifest = []
    with _client_scope(client) as active:
        _refresh(active, state)
        _save_state(state_path, state)
        if state["status"] != "completed" or not state.get("turn_id"):
            raise RuntimeError("Artifacts require a verified completed root turn")
        root = Path(output_dir).absolute() / _safe_component(state["session_id"]) / _safe_component(state["turn_id"])
        if any(p.is_symlink() for p in [root, *root.parents]):
            raise ValueError("Artifact destinations cannot contain symlinks")
        seen = set()
        for artifact in active.beta.agents.sessions.artifacts.list(state["session_id"], limit=100):
            if _get(artifact, "turn_id") != state["turn_id"]:
                continue
            if _get(artifact, "session_id") != state["session_id"]:
                continue
            remote = PurePosixPath(_get(artifact, "path", ""))
            if not remote.is_absolute() or ".." in remote.parts or "\\" in str(remote):
                raise ValueError("Unsafe artifact path")
            try:
                relative = remote.relative_to(REMOTE_OUTPUTS)
            except ValueError as error:
                raise ValueError("Artifact is outside /workspace/outputs") from error
            if not relative.parts or relative in seen:
                raise ValueError("Missing or duplicate artifact path for the completed turn")
            seen.add(relative)
            target = root.joinpath(*relative.parts)
            if any(p.is_symlink() for p in [target, *target.parents]):
                raise ValueError("Artifact destinations cannot contain symlinks")
            target.parent.mkdir(parents=True, exist_ok=True)
            fd, temporary = tempfile.mkstemp(prefix=".download-", dir=target.parent)
            os.close(fd)
            try:
                with active.beta.agents.sessions.artifacts.with_streaming_response.content(
                    _get(artifact, "id"), session_id=state["session_id"],
                ) as response:
                    response.stream_to_file(temporary)
                temp_path = Path(temporary)
                if temp_path.stat().st_size != _get(artifact, "size_bytes"):
                    raise RuntimeError("Downloaded artifact size does not match its metadata")
                digest = hashlib.sha256(temp_path.read_bytes()).hexdigest()
                if target.exists():
                    if hashlib.sha256(target.read_bytes()).hexdigest() != digest:
                        raise FileExistsError("An existing artifact has different content")
                else:
                    # Link a completed download atomically without overwriting a
                    # concurrent export or an existing file.
                    os.link(temporary, target)
                downloaded.append(target)
                manifest.append({"id": _get(artifact, "id"), "path": str(remote), "sha256": digest,
                                 "size_bytes": temp_path.stat().st_size, "local_path": str(target)})
            finally:
                Path(temporary).unlink(missing_ok=True)
    if not downloaded:
        raise FileNotFoundError("The completed turn has no published artifacts; do not treat it as a complete report")
    state["artifacts"] = manifest
    state["artifacts_downloaded_at"] = _now()
    _save_state(state_path, state)
    return downloaded


def delete_session(state_path: Path, *, client: Any = None, allow_without_download: bool = False) -> dict[str, Any]:
    """Explicit cleanup only. Keep the session until needed artifacts are saved."""
    state = _load_state(state_path)
    if state.get("status") == "deleted":
        return state
    if not allow_without_download:
        from cloud_agent.integrity import sha256_file, verify_downloads

        if not state.get("artifacts_downloaded_at") or not state.get("artifacts"):
            raise RuntimeError("Download the artifacts before deleting the hosted session")
        paths = []
        for artifact in state["artifacts"]:
            path = Path(artifact["local_path"])
            if not path.is_file() or path.is_symlink() or sha256_file(path) != artifact["sha256"]:
                raise RuntimeError("Retained artifact is unavailable or changed; download and verify before cleanup")
            paths.append(path)
        verify_downloads(paths)
    with _client_scope(client) as active:
        _refresh(active, state)
        if not state.get("session_id"):
            raise RuntimeError("The original session must be identified before cleanup")
        if state["status"] not in TERMINAL:
            raise RuntimeError("Cancel active work and confirm its outcome before cleanup")
        # A 409 is intentionally surfaced. The caller can retry after setup or
        # cleanup settles; do not spin or silently cancel another operation.
        active.beta.agents.sessions.delete(state["session_id"])
    state["status"] = "deleted"
    state["deleted_at"] = _now()
    _save_state(state_path, state)
    return state
