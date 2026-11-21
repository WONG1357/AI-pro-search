# OpenAI-hosted agent: implementation contract

Verified against public OpenAI documentation and the installed `openai==3.57.0`
Python SDK on 21 November 2026. The implementation lives in
`cloud_agent/hosted.py`. No live OpenAI request was made during these contract
tests: a newly provisioned key and project access are still required for live
acceptance.

## What runs in the cloud

This implementation uses the **Agents API** with
`environment.type = openai_hosted`. OpenAI provisions a Linux workspace, runs the
agent's managed harness, and retains session state. The local or cloud launcher
submits work and retrieves results. Closing that launcher's stream does not stop
the remote task. This is not a locally running Agents SDK loop or a
Responses-only inference wrapper.

The hosted working directory is `/workspace`. The launcher uploads a validated
code archive and extracts it to `/workspace/app`. The task instructions must tell
the agent to execute the reviewed surveillance tools from that directory and
write deliverables under `/workspace/outputs`.

Public references:

- [Agents API overview](https://developers.openai.com/api/docs/guides/agents-api/overview)
- [Quickstart and key permissions](https://developers.openai.com/api/docs/guides/agents-api/quickstart)
- [OpenAI-hosted sandbox configuration](https://developers.openai.com/api/docs/guides/agents-api/environments/openai-hosted)

## Authentication and access

The application key is used by the caller's `OpenAI` client. It must have
`api.agents.read`, `api.agents.write`, and `api.responses.write` permissions, and
the selected project must have access to the requested model and hosted Agents
API. A successfully created key does not establish that access.

The key is never put in the archive, agent instructions, task metadata, state
JSON, or sandbox environment. The hosted service rejects reserved environment
names including `OPENAI_API_KEY`. The launcher does not provision credentials.
Use the separate secure Platform key flow for the agent's dedicated key. A
non-expiring key is a credential policy choice; it does not make a sandbox
permanent or remove API billing requirements.

The SDK supplies `OpenAI-Beta: agents=v1`. Raw HTTP integrations must add that
header themselves. The current verified quickstart model is `gpt-6-astra`; the
launcher accepts an explicit model override and does not silently switch models
or execution providers after an access error.

## Public request contract

The initial launch uses:

```python
client.beta.agents.sessions.create(
    agent={
        "model": model,
        "instructions": instructions,
        "tools": [{
            "type": "web_search",
            "mode": "live",
            "allowed_domains": official_domains,
        }],
    },
    environment=hosted_environment,
    input=task,
    metadata={"application": "ai-pro-search", "run_id": run_id},
    stream=wait,
)
```

`stream=True` uses `with_result_collection()` before consuming events and then
`get_final_result()`. The result provides `session_id`, `turn_id`, `turn`, and
`output_text`. `stream=False` returns the created session immediately. The
launcher can then exit; polling and downloading can occur in a later process.

For a previously saved agent, the request also supplies top-level `agent_id`.
The inline configuration overrides those fields for this session only. The
launcher does not modify the saved agent. An agent can also be saved through
`client.beta.agents.create(...)`; this is an optional separate operation, not a
prerequisite for hosted execution.

The hosted harness supplies command and file capabilities. No invented
`shell` tool declaration is added. Built-in web search **does** require its
explicit tool entry. A prompt saying “search” cannot enable it by itself.

References:

- [Create-session reference](https://developers.openai.com/api/reference/resources/beta/subresources/agents/subresources/sessions/methods/create)
- [Saved and inline configuration](https://developers.openai.com/api/docs/guides/agents-api/configuration)
- [Web search configuration](https://developers.openai.com/api/docs/guides/agents-api/tools/web-search)
- [Python result implementation](https://github.com/openai/openai-python/blob/main/src/openai/lib/beta/agents/_result.py)

## Code, dependencies, and network boundaries

The archive contains only:

- `cloud_agent/**/*.py`;
- `pipeline/**/*.py`;
- `cloud_agent/instructions.md`;
- `requirements-cloud.txt`.

Repository exports, company files, `.env` files, `.git`, hidden paths, and symlink
targets are excluded. Explicit mapping inputs reject unsafe or unapproved paths
instead of silently accepting them. The deterministic archive digest is saved
with the run. Both archive size and uncompressed input size are bounded.

The six sandbox runtime dependencies are supplied using `environment.packages`:
`pandas`, `requests`, `beautifulsoup4`, `python-dateutil`, `openpyxl`, and `pypdf`.
`requirements-cloud.txt` must use exact version pins. Caller-only packages such
as `openai` and `pytest` are not installed in the sandbox by this implementation.
Packages and input files are prepared before the fixed setup command executes.
A failed setup must prevent the agent from starting.

The sandbox uses an explicit restricted network policy. Exact regulator hosts
used by the source connectors, supporting official document hosts, and the two
Python package distribution hosts are allowed. Subdomains and redirects need
their own entries. Search has its own official-domain filter. Adding a source or
a new redirect host requires a reviewed configuration update; a denied endpoint
must be reported as unavailable, never as zero matching records.

The public hosted-file limits are 50 files on session creation, 5 MiB per inline
file, and 10 MiB total inline content before base64 encoding. This application
uploads one ZIP and enforces its 5 MiB limit. The default medium container is
2 vCPU and 4 GB memory.

References:

- [Hosted setup and network settings](https://developers.openai.com/api/docs/guides/agents-api/environments/openai-hosted)
- [Input-file and artifact limits](https://developers.openai.com/api/docs/guides/agents-api/environments/files)

## Recoverable execution lifecycle

Every run first reserves a unique state file containing a run ID, input hash,
bundle hash, model, and submission status. Subsequent saves use an atomic file
replacement with restrictive permissions. An existing state path cannot be
overwritten by a new launch.

The observer records the session ID as soon as the creation event arrives and
records the first root turn ID. Subagent completion events do not complete the
root task. Only the intended root turn's terminal outcome determines success,
failure, or cancellation. An `idle` event is insufficient.

Session creation has no automatic retries, including when a caller supplies an
SDK client with retries enabled. A lost creation response is ambiguous: the
server may already have accepted it. Recovery searches the project for the
saved `run_id` metadata and attaches to the original session. It does not submit
the task a second time. Missing or multiple matching sessions remain explicitly
uncertain and require reconciliation.

`resume_task` opens the event stream before retrieving saved session and turn
state. Streams do not replay old events; persisted turn items restore completed
output. Recovery uses the original turn ID, not whichever turn happens to be
latest. Session, environment, and root-turn failures are surfaced. A disconnected
observer is tracked separately from the remote turn status.

Only safe error metadata is saved: exception class, numeric HTTP status, and a
restricted error code. Full HTTP bodies, submitted content, and stream event
payloads are not logged. Source-level coverage failures are part of the
surveillance report and must be assessed separately from transport failures.

References:

- [Session lifecycle and cancellation](https://developers.openai.com/api/docs/guides/agents-api/sessions)
- [Events, turn outcomes, and reconnection](https://developers.openai.com/api/docs/guides/agents-api/sessions/events)
- [Session inspection and required actions](https://developers.openai.com/api/docs/guides/agents-api/sessions/manage)
- [SDK turn retrieval implementation](https://github.com/openai/openai-python/blob/main/src/openai/resources/beta/agents/sessions/turns/turns.py)

## Artifacts and cleanup

Files under `/workspace/outputs` become immutable session artifacts when a turn
completes. The downloader lists artifacts and selects those matching **both**
the saved session and completed root turn. It rejects path traversal and paths
outside the output directory. Local files are grouped under
`<output directory>/<session ID>/<turn ID>/` to prevent cross-run overwrites.

Downloads go to temporary files, are checked against the API's byte count, and
receive a local SHA-256 digest. The completed bytes are installed without
overwriting an existing different file. A run with no published artifacts is
not accepted as a complete report. The caller must also check the required
report files and their evidence/coverage contents; “turn completed” alone is not
a surveillance-quality guarantee.

`cancel_task` submits `agent.session.input.cancel`. It does not announce a
cancelled result until the server confirms that outcome. `delete_session` is
explicit and, by default, requires downloaded artifacts and a terminal turn.
The launcher preserves sessions for follow-up and does not delete them after
every launch. HTTP 409 cleanup conflicts are surfaced for bounded later retry,
not handled with a spin loop or a silent cancellation.

Artifacts remain downloadable after sandbox expiration, but needed files must
be downloaded before deleting the session. The sandbox can expire after one
hour without activity or keep-alives; this is distinct from API-key expiry.

References:

- [Files, published artifacts, and retention](https://developers.openai.com/api/docs/guides/agents-api/environments/files)
- [Sandbox lifetime and cleanup](https://developers.openai.com/api/docs/guides/agents-api/environments/openai-hosted)

## Python integration surface

```python
start_task(task, bundle, state_path, *, instructions, client=None,
           model="gpt-6-astra", agent_id=None, wait=True) -> dict
resume_task(state_path, *, client=None, wait=True) -> dict
get_status(state_path, *, client=None) -> dict
collect_artifacts(state_path, output_dir, *, client=None) -> list[Path]
cancel_task(state_path, *, client=None) -> dict
delete_session(state_path, *, client=None, allow_without_download=False) -> dict
```

`bundle` is a repository `Path` or an explicit safe relative-path-to-content
mapping. The injected client supports deterministic tests. Without one, the
caller creates the official `OpenAI` client using its configured application
credential. The client honors the execution environment's network/proxy
configuration. No fallback execution provider exists.

## Verification boundary

`python -m pytest tests/test_hosted.py -q` currently passes **29 tests**, including
real installed-SDK serialization against an in-memory HTTP transport. Tests cover
code/secret separation, dependency pins, no duplicate creation on HTTP 503,
reconnection without input resubmission, root/subagent outcome separation,
failure and cancellation states, exact-turn artifact selection, traversal,
partial downloads, and explicit cleanup.

These checks establish local implementation behavior. They do not establish live
project/model access, real hosted provisioning, regulator endpoint availability,
live search quality, billing, or the validity/expiry of a newly created API key.
Acceptance must include a bounded real hosted run and inspection of its actual
report and coverage artifacts once the credential is provisioned.
