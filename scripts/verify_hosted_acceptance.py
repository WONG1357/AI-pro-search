"""Recover the prescribed public acceptance task, verify it, and retain its evidence.

Run on the same private cloud controller checkout as the detached launch.
This module never starts a session, submits input, cancels work, or deletes a session.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import re
import shlex
import stat
import subprocess
import sys
import time
import zipfile

from cloud_agent import hosted
from cloud_agent.integrity import verify_downloads


class AcceptanceError(ValueError):
    """A public error message containing no submitted or retrieved text."""


def reviewed_bundle(repo: Path) -> tuple[str, str, set[str]]:
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    archive = subprocess.check_output(
        ["git", "archive", "--format=zip", commit, "--", "cloud_agent", "pipeline", "requirements-cloud.txt"],
        cwd=repo,
    )
    with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
        files = {
            item.filename: zipped.read(item)
            for item in zipped.infolist()
            if not item.is_dir() and not stat.S_ISLNK(item.external_attr >> 16)
            and hosted._allowed_bundle_path(item.filename)
        }
    environment, digest = hosted.build_environment(files)
    return commit, digest, set(environment["packages"]["python"])


def command_words(command: str, cwd: str | None) -> list[str]:
    # Accept standalone commands and the documented working-directory prefix.
    try:
        words = shlex.split(command)
        if len(words) == 3 and Path(words[0]).name in {"sh", "bash"} and words[1] in {"-c", "-lc"}:
            words = shlex.split(words[2])
    except ValueError:
        return []
    if words[:3] == ["cd", hosted.REMOTE_ROOT, "&&"]:
        return words[3:]
    return words if cwd == hosted.REMOTE_ROOT else []


def check_trace(items, task: str, turn_id: str, folder_name: str, digest: str, pins: set[str]) -> None:
    # The task file specifies each observed command on its own line.
    commands = [
        line.replace("RUN_DIR", hosted.REMOTE_OUTPUTS + "/" + folder_name)
        for line in task.splitlines() if line.startswith(("sha256sum ", "python "))
    ]
    expected = [shlex.split(line) for line in commands]
    if len(expected) != 5:
        raise AcceptanceError("Unexpected acceptance task command contract")
    seen = []
    for item in items:
        if item.turn_id != turn_id:
            raise AcceptanceError("Unexpected turn in the execution trace")
        if item.type in {"web_search_call", "create_subagent_call"}:
            raise AcceptanceError("The bounded acceptance task performed extra discovery or delegation")
        if item.type != "command_execution":
            continue
        words = command_words(item.command, item.cwd)
        if words not in expected:
            if re.search(r"\bcloud_agent\s+(search|supplement)\b", item.command):
                raise AcceptanceError("An additional or unexpected retrieval command was observed")
            continue
        pos = expected.index(words)
        if item.status != "completed" or item.exit_code != 0:
            raise AcceptanceError("An acceptance command did not complete successfully")
        if pos == 0 and (item.output or "").strip().split() != [digest, "/workspace/ai_pro_search.zip"]:
            raise AcceptanceError("The hosted ZIP hash differs from the reviewed bundle")
        if pos == 1 and set((item.output or "").splitlines()) != pins:
            raise AcceptanceError("The observed installed package versions differ from the reviewed pins")
        seen.append(pos)
    if seen != list(range(len(expected))):
        raise AcceptanceError("Missing, repeated or out-of-order acceptance commands in the root trace")


def check_public_report(folder: Path, task: str) -> dict:
    def read(name: str):
        return json.loads((folder / name).read_text(encoding="utf-8"))

    request = read("request.json")
    expected = json.loads(next(line for line in task.splitlines() if line.startswith('{"keywords"')))
    if any(request.get(key) != value for key, value in expected.items()):
        raise AcceptanceError("The retrieved request differs from the prescribed public FDA query")
    overview = read("overview.json")
    evidence = read("evidence.json")
    assessment = read("assessment.json")
    rows = overview["source_status"]
    if (overview["status"] != "incomplete" or len(rows) != 1 or rows[0]["source_name"] != "FDA_MAUDE"
            or rows[0]["success"] is not True or rows[0]["completeness"] != "partial"
            or not 0 < len(evidence) <= 2 or rows[0]["exported_count"] != len(evidence)
            or overview.get("supplemental_documents") or (folder / "supplemental_errors.json").exists()):
        raise AcceptanceError("Expected one usable capped FDA search with partial coverage")
    coverage = rows[0]
    counts = [coverage[key] for key in ("retrieved_count", "returned_count", "exported_count")]
    if not (2 >= counts[0] >= counts[1] >= counts[2] >= 1):
        raise AcceptanceError("FDA retrieval, return and export counts are inconsistent with the cap")
    if not any("cap" in w.get("warning_message", "").lower() for w in overview["source_warnings"]):
        raise AcceptanceError("The source ledger does not disclose the retrieval cap")
    for record in evidence:
        if (record.get("record_kind") != "adverse_event_report" or record.get("source") != "FDA_MAUDE"
                or not record.get("raw_record") or not record.get("native_id")
                or record.get("search_date_basis") != "report_date"
                or not "2024-01-01" <= record.get("search_date", "")[:10] <= "2024-12-31"):
            raise AcceptanceError("An exported FDA record lacks native evidence or the requested date basis")
    if not assessment.get("claims") or not assessment.get("limitations"):
        raise AcceptanceError("The usable FDA result lacks an evidence-bound assessment or limitations")
    return dict(zip(("retrieved", "returned", "exported"), counts))


def finish(repo: Path, state_path: Path, output: Path, client) -> dict:
    task = (repo / "examples" / "hosted-acceptance.txt").read_text(encoding="utf-8")
    state = hosted._load_state(state_path)
    commit, digest, pins = reviewed_bundle(repo)
    if (state["task_sha256"] != hashlib.sha256(task.encode()).hexdigest()
            or state["bundle_sha256"] != digest):
        raise AcceptanceError("Use the original public task state and its reviewed checkout")
    session_id, turn_id = state.get("session_id"), state.get("turn_id")
    deadline = time.monotonic() + 900
    while True:
        state = hosted.resume_task(state_path, client=client, wait=False)
        if ((session_id and state.get("session_id") != session_id)
                or (turn_id and state.get("turn_id") != turn_id)):
            raise AcceptanceError("The original session or root turn changed")
        session_id, turn_id = state.get("session_id"), state.get("turn_id")
        if state["status"] == "completed":
            break
        if state["status"] in hosted.TERMINAL | {"requires_action", "ambiguous_submission", "environment_failed"}:
            raise AcceptanceError("The original task requires inspection in the private controller state")
        if time.monotonic() >= deadline:
            raise AcceptanceError("Observation timed out. Resume with the same state file")
        time.sleep(10)
    if not session_id or not turn_id:
        raise AcceptanceError("The completed run lacks the session or intended root turn ID")
    sessions = client.beta.agents.sessions
    session = sessions.retrieve(session_id)
    turn = sessions.turns.retrieve(turn_id, session_id=session_id)
    if (session.id != session_id or session.environment.type != "openai_hosted"
            or session.environment.id != state["environment_id"]
            or (session.metadata or {}).get("run_id") != state["run_id"]
            or turn.id != turn_id or turn.session_id != session_id
            or turn.subagent_id is not None or turn.status != "completed"):
        raise AcceptanceError("The remote session, run identity, environment or root turn does not match")
    # Retain every available file, even when subsequent quality checks reject the run.
    files = hosted.collect_artifacts(state_path, output, client=client)
    reports = verify_downloads(files, require_assessment=True)
    folders = [file.parent for file in files if file.name == "overview.json"]
    if len(reports) != 1 or len(folders) != 1:
        raise AcceptanceError("The bounded task must publish exactly one report")
    folder = folders[0]
    if folder != output.absolute() / session_id / turn_id / ("run-" + reports[0]["run_id"]):
        raise AcceptanceError("The downloaded report directory differs from the exact root-turn path")
    counts = check_public_report(folder, task)
    items = sessions.turns.items.list(turn_id, session_id=session_id, order="asc", limit=100)
    check_trace(items, task, turn_id, folder.name, digest, pins)
    return {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "implementation_commit": commit, "bundle_sha256": digest, "model": state["model"],
        "environment": "openai_hosted", "completed_root_turn": True,
        "reconnected_using_saved_state": True, "installed_pins_checked": sorted(pins),
        "observed_toolkit_commands": ["search", "finalize", "verify"],
        "source": "FDA_MAUDE", "coverage": "partial", "counts": counts,
        "verified_report_files": reports[0]["verified_files"],
        "report_manifest_sha256": hashlib.sha256((folder / "manifest.json").read_bytes()).hexdigest(),
        "scope": "Machine checks for one capped public query. Review the assessment, full evidence, "
                 "and private final response for agreement, and retain the files before cleanup.",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--output", type=Path, default=Path("agent-results"))
    args = parser.parse_args()
    try:
        from openai import OpenAI
        with OpenAI(max_retries=0, timeout=60.0) as client:
            receipt = finish(Path(__file__).resolve().parents[1], args.state, args.output, client)
        print(json.dumps(receipt, indent=2))
        return 0
    except AcceptanceError as exc:
        print(str(exc), file=sys.stderr)
    except Exception as exc:
        # SDK errors and arbitrary artifact/trace values may contain sensitive text.
        print("Acceptance check failed:", type(exc).__name__, file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
