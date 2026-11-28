# AI Pro Search — OpenAI cloud surveillance agent

An agent for **medical-device adverse-event reports, recalls, and field safety notices** across FDA MAUDE, TGA DAEN, Health Canada MDI, Swissmedic, MHRA, and BfArM.

The replacement combines an OpenAI-hosted agent with a deterministic, audited Python retrieval toolkit. It accepts a natural-language request, plans device-specific searches, checks source coverage, examines original evidence, and produces CSV, Excel, JSON, and cited Markdown reports.

**Activation is pending.** The controller and retrieval toolkit are available for testing. The [acceptance record](docs/AUDIT_AND_ACCEPTANCE.md) documents the source checks and the pending live OpenAI-hosted run.

## What runs where

| Component | Runtime | Responsibility |
| --- | --- | --- |
| GPT agent and its Linux workspace | OpenAI Agents API, `openai_hosted` | Plan the search, execute the tested toolkit, inspect evidence, write the assessment |
| Retrieval and export toolkit | Inside that OpenAI-hosted workspace | Fetch official records, validate source responses, filter, deduplicate, classify, export, and verify |
| Thin controller | GitHub Actions or another cloud process | Start a session, observe/reconnect, retrieve artifacts, and clean up |
| Application API key | Controller secret store only | Authorize OpenAI requests; never uploaded to the agent workspace |

There is no requirement to leave a laptop running. Once a session has been accepted, closing the observer does not stop the agent. No external function responder or local tool server is required.

This uses the **Agents API**, not a local agent loop with only model inference in the cloud. The SDK contract is verified against OpenAI Python SDK 3.57.0. The documented default model is `gpt-6-astra`; use another supported model only if your project has the required access.

Primary references: [Agents API quickstart](https://developers.openai.com/api/docs/guides/agents-api/quickstart), [OpenAI-hosted sandboxes](https://developers.openai.com/api/docs/guides/agents-api/environments/openai-hosted), [files and artifacts](https://developers.openai.com/api/docs/guides/agents-api/environments/files).

## Main improvements

- Failures, rate limits, invalid payloads, page caps, source timeouts, and unverified coverage remain visible. They cannot become a successful “no incidents” answer.
- Each source has its own process and elapsed-time budget; one unavailable regulator does not erase successful results from others.
- Handled later-page errors retain valid earlier pages. A forcibly terminated worker cannot recover its in-memory pages; that source is explicitly failed/unknown.
- Source IDs are included in record identities. Missing IDs still count, and long narratives are not collapsed using their first 500 characters.
- Device terms drive retrieval. Outcome and component terms guide classification, avoiding broad unrelated-device searches.
- Negative findings such as “No patient injury” do not become positive findings. Rule labels do not establish clinical causation or validated risk.
- Event dates, receipt/report dates, and the date actually used for filtering remain separate. Invalid and partial dates are not filled using today's date.
- Every export has an isolated run directory, coverage ledger, raw evidence, query details, and SHA-256 checksums.
- Spreadsheet formula-looking content is stored as text. Complete raw strings are preserved in JSONL when Excel cells need truncation.
- Agent assessments must reference existing evidence IDs with exact source excerpts. This validates provenance; semantic support still requires review.
- When an official catalogue is blocked, the agent can discover official notice/PDF links through web search and preserve them with a bounded document fetcher. Supplemental evidence stays separate from database counts and remains unverified in coverage.
- Source bundles exclude API keys, environment files, Git history, exports, and symlinks.
- Session and intended root-turn IDs survive disconnection. Resuming does not submit the task a second time.

## Run entirely from a browser through GitHub Actions

After the changes are on the repository's default branch, the manual workflow appears under **Actions → Run OpenAI cloud surveillance agent**.

1. Complete the secure OpenAI API-key setup for the intended project. A non-expiring key requires the **Never** option to be permitted by that project's policy. The key needs Agents read/write and Responses write permissions.
2. Configure the controller's `OPENAI_API_KEY` repository secret. Do not put it in a task, committed file, or chat.
3. Select **Run workflow**, enter the device/date/region request, and select a model available to the project.
4. For downloadable repository artifacts, enable `export_reports`. Those files inherit the repository's access, so it is off by default. Otherwise, completed results remain in the OpenAI session.
5. The workflow verifies the actual report files after the model turn completes. Its recovery artifact stores session/turn IDs even if observation times out.

The repository is public. Workflow inputs are visible in that repository; use a private controller/repository for confidential requests. The workflow does not automatically publish reports or change the repository's audience.

Example request:

> Find trocar adverse-event reports and field safety notices from 1 January to 31 December 2024 across all six regulators. Include the exact device terms used, a source-by-source coverage table, original evidence links, category summaries, and CSV/Excel exports. Distinguish reports from safety notices and explain any gaps.

The workflow starts one task on its first attempt. If observation times out, use the saved recovery state to reconnect to the same session. A workflow rerun uses the retained initial recovery artifact and fails safely if that state is unavailable.

## Controller commands

These commands can run in a cloud terminal or service. Only the controller dependencies are needed here:

```bash
python -m pip install -r requirements-agent.txt
python -m cloud_agent start --task-file request.txt --state .agent-state/run.json --detach
python -m cloud_agent status --state .agent-state/run.json
python -m cloud_agent resume --state .agent-state/run.json
python -m cloud_agent download --state .agent-state/run.json --output agent-results
```

The controller expects `OPENAI_API_KEY` in its environment. It does not read or write an env file and does not create an API key by itself. Use the secure setup flow; do not commit a key.

- `start` without `--detach` observes the initial turn.
- `resume` reconnects to the same task and never resends its input.
- `download` selects artifacts from the exact completed root turn, checks their sizes and inventory, and verifies each surveillance report and assessment. Inspect its source coverage even when the command succeeds.
- `cancel` requests cancellation, then checks the remote state.
- `cleanup` requires verification of the retained report files before deleting a completed session. Keep the download on persistent storage and run cleanup from a controller that can still access it. Workflow recovery artifacts record session and turn IDs. If you recover from GitHub Actions, download and verify the files again before cleanup.

```bash
python -m cloud_agent cancel --state .agent-state/run.json
python -m cloud_agent cleanup --state .agent-state/run.json
```

A failed HTTP request or a disconnected stream is not proof that the remote task stopped. The saved state records the distinction.

The bounded public FDA task in [`examples/hosted-acceptance.txt`](examples/hosted-acceptance.txt) checks remote execution before cutover. Use a new state path for its first submission and reuse that path for status, resume, and download. The task deliberately caps retrieval at one page and two records, so a usable result should still disclose partial coverage. Save the verified download and controller state privately.

## Deterministic toolkit

The agent uses the same testable commands below inside `/workspace/app`:

```bash
python -m pip install -r requirements-cloud.txt
python -m cloud_agent search --request examples/trocar-search.json --output agent-results
```

The command reports the actual run directory. Use it for verification:

```bash
python -m cloud_agent verify --run-dir agent-results/ACTUAL_RUN_DIRECTORY
```

Each request has explicit sources, dates, device keywords, classification terms, and budgets. Defaults are ten pages, 1,000 records, a 20-second request timeout, and a 120-second elapsed-time budget per source. The hosted agent is instructed to make at most three retrieval runs unless a larger search is requested. This instruction is not a platform-enforced billing ceiling.

FDA searches include brand, generic name, model number, product code, and narrative fields. Other regulators provide keyword-based searches, not a universal exact product-code filter. A complete query does not establish worldwide completeness.

### Outputs

| File | Purpose |
| --- | --- |
| `request.json` | Exact validated request and limits |
| `overview.json` | Counts, all-source coverage, failures, warnings, and scope |
| `evidence.json` | Full returned record evidence, IDs, original links, timestamps, and hashes |
| `evidence_index.json` | Compact index for selecting evidence to inspect |
| `report.md` | Deterministic coverage and evidence report |
| `assessment.md/json` | Agent interpretation with checked references and verbatim support |
| `supplemental_evidence.json` and `supplemental_documents/` | Separately discovered official documents, extracted text, original bytes, and explicit unverified coverage |
| `tables/.../incident_report.xlsx` | Results, summaries, source status, warnings, and errors |
| `tables/.../*.csv` | Portable tables |
| `tables/.../*records.jsonl` | Complete raw/filtered values without Excel's cell limit |
| `manifest.json` | Report file inventory and integrity checks |

An official portal URL is explicitly distinguished from a direct incident/notice URL. Swissmedic notice PDFs can be used as direct evidence links.

`retrieved_count` describes source retrieval before local filtering; `returned_count` describes normalized rows returned by that connector; `exported_count` describes filtered records in the final output. These counts can differ legitimately. Raw exports contain the source rows returned by the connector, not every record that exists on the regulator's system.

Counts describe retrieved reports/notices. They are not unique patient counts, incidence rates, proof of device causality, or comparative safety rankings. Cross-regulator reports can overlap.

## Source coverage and current validation

| Source | Preserved capability | Coverage limit / latest check |
| --- | --- | --- |
| FDA MAUDE | Official openFDA retrieval, identity/narrative search, normalization and record links | Endpoint and a small full toolkit run succeeded; requested caps correctly returned partial coverage |
| TGA DAEN | Device selection, report/print retrieval, HTML/PDF parsing | A live December 2020 check retrieved the expected report and reconciled the portal's one-report total. The earlier Versaport/2024 query returned a source-confirmed zero. Fixtures cover PDF parsing and pagination |
| Health Canada MDI | Public DataTables retrieval, paging, native fields | Endpoint and bounded worker run succeeded; limited pages can contain records outside the requested period |
| Swissmedic | Official publication search and notice links | Bounded full toolkit run returned two records and correctly marked the cap as partial; OpenAI-hosted acceptance remains separate |
| MHRA | Official alert/weekly-notice parsing | The date filter uses the manufacturer notice date when the listing supplies it and records the weekly publication date separately. HTML listing coverage remains unverified |
| BfArM | Official search/notice retrieval and separate document fallback | The catalogue returned HTTP 403 during development and public web checks. Individual official notices were readable as supplemental evidence. The scanned PDF checked earlier requires OCR or visual review. A check from the OpenAI-hosted target remains pending |

Endpoint probes do not establish end-to-end connector correctness. Successful fixtures do not establish current source availability.

The supplementary fetcher currently supports text extraction from HTML and text-based PDFs. It reports scanned/image-only PDFs as requiring OCR or manual review; it does not turn missing text into a successful empty notice.

## Audit and acceptance evidence

- [Audit overview and remaining gates](docs/AUDIT_AND_ACCEPTANCE.md)
- [Core findings and regressions](docs/CORE_AUDIT.md)
- [Source findings and regressions](docs/SOURCE_AUDIT.md)
- [Bounded live endpoint probes](docs/SOURCE_ENDPOINT_CHECK.json)
- [Small live retrieval/export integration check](docs/INTEGRATION_CHECK.json)
- [Bounded full six-source integration check](docs/SIX_SOURCE_INTEGRATION_CHECK.json)
- [Default-budget TGA integration check](docs/TGA_INTEGRATION_CHECK.json)
- [Follow-up TGA zero-result confirmation](docs/TGA_ZERO_CONFIRMATION.json)
- [TGA positive-report and pagination evidence](docs/TGA_POSITIVE_ACCEPTANCE_CHECK.json)
- [Official-document fallback check](docs/SUPPLEMENT_CHECK.json)
- [MHRA notice-date and BfArM coverage checks](docs/MHRA_BFARM_ACCEPTANCE_CHECK.json)
- [Official-record search quality benchmark](docs/SEARCH_QUALITY_BENCHMARK.md)
- [Hosted acceptance continuation evidence](docs/HOSTED_ACCEPTANCE_CONTINUATION.json)
- [Final regression result](docs/REGRESSION_CHECK.json)
- [Verified hosted API contract](docs/HOSTED_API_CONTRACT.md)

The original commit under audit is `d67c548ae5aa7ebe9d0b233204c277dbf62cf413`. It had 81 passing tests, no GitHub issues, and no Actions runs. Passing the original tests did not imply the listed defects were absent.

Run the full suite without credentials:

```bash
python -m pip install -r requirements.txt -r requirements-agent.txt
python -m pytest -q
```

The tests include realistic source failures, malformed later pages, pagination overlap, date precision, counting, classification negation, exported formulas, evidence integrity, citation checks, and the OpenAI SDK's actual HTTP serialization against an in-memory transport.

## Existing dashboard

The corrected Streamlit dashboard remains available for structured searches:

```bash
python -m pip install -r requirements.txt
streamlit run app.py
```

It now displays source coverage and provides Excel, JSONL, and manifest downloads. The deprecated setup instructions referring to a nonexistent `.env.example` and unsupported UI controls have been removed.

## Explicit boundaries

This is incident surveillance. It is not a general regulation-version corpus, QMS amendment system, or a clinically validated risk assessment. The agent has no company database credentials or document-write integration.

Operational use is pending verification of project access, hosted execution, report and assessment integrity, recovery, and durable artifact retention. The [acceptance record](docs/AUDIT_AND_ACCEPTANCE.md) tracks the remaining source checks and cutover criteria.
