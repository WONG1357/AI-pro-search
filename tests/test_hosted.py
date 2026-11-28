"""Behavioral tests of hosted execution; never make a live paid API call."""

from __future__ import annotations

import base64
import io
import json
import os
import zipfile
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from cloud_agent.hosted import (
    DEFAULT_PACKAGES,
    build_environment,
    cancel_task,
    collect_artifacts,
    delete_session,
    get_status,
    resume_task,
    start_task,
)


BUNDLE = {"cloud_agent/__init__.py": "", "cloud_agent/instructions.md": "Use verified evidence."}


def turn(status="completed", turn_id="turn_1", subagent_id=None):
    return NS(id=turn_id, status=status, subagent_id=subagent_id, session_id="sess_1")


def session(status="idle"):
    return NS(id="sess_1", status=status, environment=NS(type="openai_hosted", id="env_1"), required_actions=[])


def message(text="Evidence report", turn_id="turn_1", phase="final"):
    return NS(type="message", status="completed", role="assistant", phase=phase, turn_id=turn_id,
              content=[NS(type="output_text", text=text)])


class FakeStream:
    def __init__(self, events=(), result=None, error=None):
        self.events = events
        self.result = result or NS(turn=turn(), output_text="Evidence report")
        self.error = error
        self.entered = False

    def with_result_collection(self):
        return self

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, *args):
        return False

    def __iter__(self):
        yield from self.events
        if self.error:
            raise self.error

    def get_final_result(self):
        return self.result


def fake_client():
    client = NS()
    sessions = NS(
        create=Mock(), retrieve=Mock(return_value=session()),
        list=Mock(return_value=[session()]), delete=Mock(return_value=NS(deleted=True)),
        events=NS(stream=Mock(return_value=FakeStream()), create=Mock()),
        turns=NS(retrieve=Mock(return_value=turn()), list=Mock(return_value=[turn()]),
                 items=NS(list=Mock(return_value=[message()]))),
        artifacts=NS(list=Mock(return_value=[]), with_streaming_response=NS(content=Mock())),
    )
    client.beta = NS(agents=NS(sessions=sessions))
    sessions.create.return_value = FakeStream([
        NS(type="agent.session.created", session=session("in_progress")),
        NS(type="agent.session.turn.created", turn=turn("in_progress")),
        NS(type="agent.session.turn.completed", turn=turn()),
    ])
    return client


def started(tmp_path, client=None):
    client = client or fake_client()
    path = tmp_path / "run.json"
    result = start_task("Find trocar reports", BUNDLE, path, instructions="Use official sources.", client=client)
    return client, path, result


def test_bundle_is_reproducible_and_excludes_env_data_and_symlinks(tmp_path):
    (tmp_path / "cloud_agent").mkdir()
    (tmp_path / "pipeline").mkdir()
    (tmp_path / "data").mkdir()
    (tmp_path / "cloud_agent" / "run.py").write_text("print('safe')")
    (tmp_path / "pipeline" / "source.py").write_text("x = 1")
    (tmp_path / ".env").write_text("OPENAI_API_KEY=sentinel-secret")
    (tmp_path / "data" / "company.csv").write_text("private records")
    (tmp_path / "cloud_agent" / "secret.py").symlink_to(tmp_path / ".env")
    environment, digest = build_environment(tmp_path)
    repeated, repeated_digest = build_environment(tmp_path)
    assert environment == repeated and digest == repeated_digest
    archive = base64.b64decode(environment["files"][0]["data"])
    with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
        assert set(zipped.namelist()) == {"cloud_agent/run.py", "pipeline/source.py"}
    assert b"sentinel-secret" not in archive
    assert environment["type"] == "openai_hosted"
    assert environment["network"]["access"] == "restricted"
    assert "api.fda.gov" in environment["network"]["allowed_domains"]
    assert "env" not in environment


@pytest.mark.parametrize("path", ["../.env", "/workspace/.env", "cloud_agent/../.env",
                                         "cloud_agent/.env", "pipeline\\source.py", "company.csv"])
def test_explicit_bundle_rejects_non_code_and_traversal(path):
    with pytest.raises(ValueError):
        build_environment({path: "bad"})


def test_sandbox_installs_only_pinned_runtime_dependencies():
    pins = "\n".join([*DEFAULT_PACKAGES, "openai==3.57.0", "pytest==9.1.0"])
    environment, _ = build_environment({**BUNDLE, "requirements-cloud.txt": pins})
    assert environment["packages"]["python"] == list(DEFAULT_PACKAGES)
    with pytest.raises(ValueError, match="exact"):
        build_environment({**BUNDLE, "requirements-cloud.txt": "requests>=2"})
    with pytest.raises(ValueError, match="all six"):
        build_environment({**BUNDLE, "requirements-cloud.txt": "requests==2.34.2"})


def test_start_saves_real_ids_and_uses_public_hosted_contract(tmp_path):
    client, path, result = started(tmp_path)
    assert result["session_id"] == "sess_1" and result["turn_id"] == "turn_1"
    assert result["environment_id"] == "env_1" and result["status"] == "completed"
    assert result["output_text"] == "Evidence report"
    payload = client.beta.agents.sessions.create.call_args.kwargs
    assert payload["environment"]["type"] == "openai_hosted"
    assert payload["input"] == "Find trocar reports"
    assert payload["agent"]["tools"][0]["type"] == "web_search"
    assert payload["agent"]["tools"][0]["mode"] == "live"
    assert payload["metadata"]["run_id"] == result["run_id"]
    assert json.loads(path.read_text())["turn_id"] == "turn_1"
    assert os.stat(path).st_mode & 0o777 == 0o600


def test_state_file_never_overwrites_an_existing_run(tmp_path):
    client, path, result = started(tmp_path)
    with pytest.raises(FileExistsError):
        start_task("Duplicate", BUNDLE, path, instructions="Safe", client=client)
    assert client.beta.agents.sessions.create.call_count == 1
    assert json.loads(path.read_text())["run_id"] == result["run_id"]


def test_detached_start_needs_no_observer_process(tmp_path):
    client = fake_client()
    client.beta.agents.sessions.create.return_value = session("in_progress")
    result = start_task("Search", BUNDLE, tmp_path / "run.json", instructions="Safe", client=client,
                        agent_id="agent_existing", wait=False)
    assert result["session_id"] == "sess_1"
    assert result["observer_status"] == "detached"
    payload = client.beta.agents.sessions.create.call_args.kwargs
    assert payload["agent_id"] == "agent_existing" and payload["stream"] is False


def test_lost_stream_retains_ids_without_claiming_failure_or_retrying(tmp_path):
    client = fake_client()
    client.beta.agents.sessions.create.return_value = FakeStream([
        NS(type="agent.session.created", session=session("in_progress")),
        NS(type="agent.session.turn.created", turn=turn("in_progress")),
    ], error=ConnectionError("secret credential must never be logged"))
    client, path, result = started(tmp_path, client)
    assert result["status"] == "in_progress"
    assert result["observer_status"] == "disconnected"
    assert result["session_id"] == "sess_1" and result["turn_id"] == "turn_1"
    assert "secret credential" not in path.read_text()
    assert client.beta.agents.sessions.create.call_count == 1
    recovered = resume_task(path, client=client)
    assert recovered["status"] == "completed" and recovered["output_text"] == "Evidence report"
    assert client.beta.agents.sessions.create.call_count == 1
    client.beta.agents.sessions.events.create.assert_not_called()


def test_ambiguous_creation_is_reconciled_by_metadata_never_resubmitted(tmp_path):
    client = fake_client()
    client.beta.agents.sessions.create.side_effect = TimeoutError("network")
    _, path, result = started(tmp_path, client)
    assert result["status"] == "launch_unknown" and result["session_id"] is None
    recovered = get_status(path, client=client)
    assert recovered["status"] == "completed" and recovered["session_id"] == "sess_1"
    assert client.beta.agents.sessions.list.call_args.kwargs["metadata"] == {"run_id": result["run_id"]}
    assert client.beta.agents.sessions.create.call_count == 1


def test_unknown_creation_without_matching_session_stays_uncertain(tmp_path):
    client = fake_client()
    client.beta.agents.sessions.create.side_effect = TimeoutError()
    client.beta.agents.sessions.list.return_value = []
    _, path, _ = started(tmp_path, client)
    result = resume_task(path, client=client)
    assert result["status"] == "launch_unknown"
    assert client.beta.agents.sessions.create.call_count == 1


def test_resume_subscribes_before_reconciling_and_ignores_subagent_completion(tmp_path):
    client, path, _ = started(tmp_path)
    active_turn = turn("in_progress")
    client.beta.agents.sessions.turns.retrieve.side_effect = [active_turn, turn()]
    stream = FakeStream([
        NS(type="agent.session.idle", session=session()),
        NS(type="agent.session.turn.completed", turn=turn("completed", "turn_sub", "sub_1")),
        NS(type="agent.session.turn.completed", turn=turn()),
    ])
    client.beta.agents.sessions.events.stream.return_value = stream
    def retrieve(_):
        assert stream.entered
        return session("in_progress")
    client.beta.agents.sessions.retrieve.side_effect = retrieve
    result = resume_task(path, client=client)
    assert result["turn_id"] == "turn_1" and result["status"] == "completed"


@pytest.mark.parametrize("status", ["failed", "cancelled"])
def test_turn_failure_is_not_treated_as_idle_success(tmp_path, status):
    client, path, _ = started(tmp_path)
    client.beta.agents.sessions.turns.retrieve.return_value = turn(status)
    result = get_status(path, client=client)
    assert result["status"] == status


def test_cancellation_request_does_not_claim_cancellation_before_server_confirms(tmp_path):
    client, path, _ = started(tmp_path)
    client.beta.agents.sessions.turns.retrieve.return_value = turn("in_progress")
    result = cancel_task(path, client=client)
    assert result["status"] == "in_progress"
    assert result["cancellation_requested_at"]
    assert client.beta.agents.sessions.events.create.call_args.kwargs["events"] == [
        {"type": "agent.session.input.cancel"}
    ]


def artifact(path="/workspace/outputs/report.csv", turn_id="turn_1", content=b"verified\n"):
    return NS(id="artifact_1", turn_id=turn_id, session_id="sess_1", path=path, size_bytes=len(content))


class Download:
    def __init__(self, content):
        self.content = content

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def stream_to_file(self, path):
        Path(path).write_bytes(self.content)


def test_artifacts_match_exact_turn_and_preserve_report_content(tmp_path):
    client, path, _ = started(tmp_path)
    client.beta.agents.sessions.artifacts.list.return_value = [artifact(turn_id="old_turn"), artifact()]
    client.beta.agents.sessions.artifacts.with_streaming_response.content.return_value = Download(b"verified\n")
    files = collect_artifacts(path, tmp_path / "reports", client=client)
    assert len(files) == 1 and files[0].read_bytes() == b"verified\n"
    assert files[0].relative_to(tmp_path).as_posix() == "reports/sess_1/turn_1/report.csv"
    assert json.loads(path.read_text())["artifacts"][0]["sha256"]
    assert client.beta.agents.sessions.artifacts.with_streaming_response.content.call_count == 1
    # Retrying a download is safe, while changed content may never replace it.
    assert collect_artifacts(path, tmp_path / "reports", client=client) == files


@pytest.mark.parametrize("remote", ["/workspace/outputs/../../.env", "/workspace/input.csv", "relative.csv"])
def test_artifact_path_cannot_escape_download_directory(tmp_path, remote):
    client, path, _ = started(tmp_path)
    client.beta.agents.sessions.artifacts.list.return_value = [artifact(path=remote)]
    with pytest.raises(ValueError):
        collect_artifacts(path, tmp_path / "reports", client=client)
    client.beta.agents.sessions.artifacts.with_streaming_response.content.assert_not_called()


def test_incomplete_artifact_bytes_never_become_a_report(tmp_path):
    client, path, _ = started(tmp_path)
    client.beta.agents.sessions.artifacts.list.return_value = [artifact()]
    client.beta.agents.sessions.artifacts.with_streaming_response.content.return_value = Download(b"cut")
    with pytest.raises(RuntimeError, match="size"):
        collect_artifacts(path, tmp_path / "reports", client=client)
    assert not list((tmp_path / "reports").rglob("*.csv"))
    assert "artifacts_downloaded_at" not in json.loads(path.read_text())


def test_empty_completed_turn_is_not_a_complete_report(tmp_path):
    client, path, _ = started(tmp_path)
    with pytest.raises(FileNotFoundError, match="no published artifacts"):
        collect_artifacts(path, tmp_path / "reports", client=client)


def test_cleanup_requires_downloaded_artifacts_and_never_silently_cancels(tmp_path):
    client, path, _ = started(tmp_path)
    with pytest.raises(RuntimeError, match="Download"):
        delete_session(path, client=client)
    client.beta.agents.sessions.delete.assert_not_called()
    client.beta.agents.sessions.turns.retrieve.return_value = turn("in_progress")
    with pytest.raises(RuntimeError, match="Cancel"):
        delete_session(path, client=client, allow_without_download=True)
    client.beta.agents.sessions.events.create.assert_not_called()
    client.beta.agents.sessions.turns.retrieve.return_value = turn()
    result = delete_session(path, client=client, allow_without_download=True)
    assert result["status"] == "deleted"


def test_cleanup_refuses_to_delete_after_a_retained_download_changes(tmp_path):
    client, path, _ = started(tmp_path)
    sessions = client.beta.agents.sessions
    sessions.artifacts.list.return_value = [artifact()]
    sessions.artifacts.with_streaming_response.content.return_value = Download(b"verified\n")
    [downloaded] = collect_artifacts(path, tmp_path / "reports", client=client)
    downloaded.write_bytes(b"changed\n")
    with pytest.raises(RuntimeError, match="Retained artifact"):
        delete_session(path, client=client)
    sessions.delete.assert_not_called()


def test_acceptance_trace_rejects_an_additional_search_outside_the_expected_cwd():
    from scripts.verify_hosted_acceptance import AcceptanceError, check_trace

    task = (Path(__file__).resolve().parents[1] / "examples" / "hosted-acceptance.txt").read_text()
    folder = "run-example"
    digest = "0" * 64
    pins = set(DEFAULT_PACKAGES)
    outputs = [f"{digest}  /workspace/ai_pro_search.zip", "\n".join(sorted(pins))]
    commands = [line.replace("RUN_DIR", f"/workspace/outputs/{folder}")
                for line in task.splitlines() if line.startswith(("sha256sum ", "python "))]
    trace = [NS(turn_id="turn_1", type="command_execution", cwd="/workspace/app", command=command,
                status="completed", exit_code=0, output=outputs[i] if i < len(outputs) else "")
             for i, command in enumerate(commands)]
    check_trace(trace, task, "turn_1", folder, digest, pins)

    extra = NS(turn_id="turn_1", type="command_execution", cwd="/workspace",
               command="cd /workspace/app && python -m cloud_agent search --request /workspace/other.json")
    with pytest.raises(AcceptanceError, match="unexpected retrieval"):
        check_trace([*trace, extra], task, "turn_1", folder, digest, pins)


def test_report_recovery_excludes_other_turns_and_commentary(tmp_path):
    client, path, _ = started(tmp_path)
    client.beta.agents.sessions.turns.items.list.return_value = [
        message("old", turn_id="turn_old"), message("working", phase="commentary"), message("actual report"),
    ]
    assert get_status(path, client=client)["output_text"] == "actual report"


def test_session_failure_wins_over_a_stale_in_progress_turn(tmp_path):
    client, path, _ = started(tmp_path)
    client.beta.agents.sessions.retrieve.return_value = session("failed")
    client.beta.agents.sessions.turns.retrieve.return_value = turn("in_progress")
    assert get_status(path, client=client)["status"] == "failed"


def test_actual_sdk_serializes_the_documented_hosted_request(tmp_path):
    openai = pytest.importorskip("openai")
    httpx2 = pytest.importorskip("httpx2")
    requests = []

    def handle(request):
        requests.append(request)
        body = json.loads(request.content)
        assert request.url.path == "/v1/agents/sessions"
        assert request.headers["OpenAI-Beta"] == "agents=v1"
        assert body["environment"]["type"] == "openai_hosted"
        assert body["environment"]["files"][0]["type"] == "inline"
        assert body["environment"]["network"]["access"] == "restricted"
        return httpx2.Response(200, json={
            "id": "sess_1", "object": "agent.session", "status": "in_progress",
            "environment": {"id": "env_1", "type": "openai_hosted"},
            "required_actions": [], "metadata": body["metadata"],
        })

    # MockTransport cannot make network requests. trust_env=False isolates this
    # fixture from the host proxy; application networking keeps its defaults.
    with httpx2.Client(transport=httpx2.MockTransport(handle), trust_env=False) as http_client:
        with openai.OpenAI(api_key="test-fixture-no-real-key", http_client=http_client) as client:
            result = start_task("Search", BUNDLE, tmp_path / "run.json", instructions="Use official evidence",
                                client=client, wait=False)
    assert result["status"] == "in_progress" and result["session_id"] == "sess_1"
    assert len(requests) == 1


def test_actual_sdk_does_not_retry_a_503_session_creation(tmp_path):
    openai = pytest.importorskip("openai")
    httpx2 = pytest.importorskip("httpx2")
    requests = []

    def handle(request):
        requests.append(request)
        return httpx2.Response(503, json={"error": {"message": "transient", "type": "server_error"}})

    with httpx2.Client(transport=httpx2.MockTransport(handle), trust_env=False) as http_client:
        with openai.OpenAI(api_key="test-fixture-no-real-key", http_client=http_client, max_retries=3) as client:
            result = start_task("Search", BUNDLE, tmp_path / "run.json", instructions="Safe", client=client, wait=False)
    assert result["status"] == "launch_unknown"
    assert len(requests) == 1
