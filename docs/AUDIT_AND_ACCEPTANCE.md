# Audit, replacement design, and acceptance record

Originally reviewed **21 November 2026**, with acceptance checks updated **28 November 2026** (Hong Kong time). The original audited baseline in [WONG1357/AI-pro-search](https://github.com/WONG1357/AI-pro-search) was `d67c548ae5aa7ebe9d0b233204c277dbf62cf413`.

## Outcome and operational status

The replacement is implemented as an OpenAI-hosted medical-device surveillance agent with a deterministic Python retrieval and reporting toolkit. It retains the original purpose: searching adverse-event reports, recalls, and field safety notices across FDA MAUDE, TGA DAEN, Health Canada MDI, Swissmedic, MHRA, and BfArM.

The audit documents **55 finding rows: 21 core findings and 34 source findings**. Each row links the original code path, consequence, and correction; related findings sometimes share a common root cause. The fixes are implemented in the replacement toolkit and the existing dashboard. This is a bounded code audit and regression program, not a guarantee that all possible defects or future website changes have been eliminated.

**Cloud activation is pending.** The 28 November continuation found no configured key and skipped the credential-dependent work at the user's instruction. Dedicated key metadata, project and model access, a live OpenAI-hosted session, and recovery and retention of its outputs remain unverified. [Draft PR #1](https://github.com/WONG1357/AI-pro-search/pull/1) remains the cutover branch.

## What the audit found

| Area | Concrete original failure | Implemented behavior |
| --- | --- | --- |
| Counting and deduplication | Three records with overlapping or blank IDs could be summarized as one; long missing-ID narratives could collapse after their first 500 characters | Source-scoped identities, full-content stable hashes, duplicate counts, and preserved raw evidence |
| Search relevance | Generic outcomes/components widened device searches; local filtering discarded valid model/manufacturer matches | Device identity drives retrieval; component/outcome terms remain classification inputs; explicit source-query flags and word-aware matching |
| Classification | Negative injury/endotoxin text became positive, or one negative finding suppressed a separate fracture; labels invented design or causation | Local negation handling, separate positive findings, neutral screening labels, and evidence-bound interpretation |
| Dates | Fuzzy parsing filled missing date parts from today; receipt dates became occurrence dates; partial dates passed narrower windows | Validated dates with retained precision and explicit report/receipt/search-date basis; unknown occurrence remains unknown |
| Failure handling | HTTP errors, malformed payloads, or all-source failure could produce a successful empty answer | Per-source complete/partial/failed/unverified status and visible errors; failed retrieval cannot establish absence |
| Source pagination | Overlap across keywords stopped later queries; repeated/contradictory pages looked complete; later failures discarded earlier rows | Query-scoped pagination checks, coherent totals, preserved valid pages, bounded retries, explicit caps and timeouts |
| Evidence and export | Missing original payloads, omitted coverage, overwritten runs, executable spreadsheet formulas, silent truncation | Raw JSONL, original links, isolated runs, text-safe spreadsheets, source coverage, manifests and checksums |
| Cloud lifecycle | No hosted execution or recovery implementation existed | OpenAI-hosted sessions, exact root-turn tracking, reconnect without resubmission, verified artifact download, explicit cancellation and cleanup |

Detailed evidence is in [CORE_AUDIT.md](CORE_AUDIT.md) and [SOURCE_AUDIT.md](SOURCE_AUDIT.md). The baseline had **81 passing tests**, an empty GitHub issue collection, and no Actions runs when inspected. The findings above came from code review and targeted reproductions, not from a pre-existing issue list.

## Replacement architecture

The GPT agent and its Linux workspace run in OpenAI's managed Agents API environment. A thin controller, supplied as a GitHub Actions workflow or Python CLI, submits a task and retrieves its results. The application key stays in that controller's secret store and is never uploaded to the agent workspace.

The agent receives a bounded, validated code archive and official-domain web-search access. It plans device/date/source queries, executes the audited toolkit, reads the original evidence, and produces an assessment with existing evidence IDs and exact supporting excerpts. The validator checks provenance and artifact integrity; it does not establish semantic entailment, clinical causation, or measured search recall.

The default model is `gpt-6-astra`, following the verified current quickstart; project access must be checked during activation. The implementation uses `openai==3.57.0`. The precise request, environment, lifecycle, and artifact contract is documented in [HOSTED_API_CONTRACT.md](HOSTED_API_CONTRACT.md).

The repository is public. The manual cloud workflow leaves report uploads off by default, preserves recovery IDs, and avoids exposing the task or report text in recovery files. Workflow input text is still visible in that repository. Confidential searches require a private controller or repository. The manual workflow becomes selectable after the workflow is on the default branch.

## Verification performed

### Deterministic regression suite

The final suite result is recorded in [REGRESSION_CHECK.json](REGRESSION_CHECK.json). Tests cover the concrete defects above, strict request validation, per-source worker failure isolation, output integrity, evidence references, supplementary document boundaries, and cloud-session recovery. Existing expectations that encoded false-success or incorrect date semantics were corrected rather than preserved as requirements.

The hosted tests use both controlled client doubles and the installed OpenAI SDK's actual request serialization with an in-memory HTTP transport. They verify the Agents beta header and that creation is not automatically retried after an ambiguous error. They are **not live cloud execution tests**.

### Bounded live regulator checks

All network checks below ran from the development environment, not an OpenAI-hosted session. Small caps were intentional: these runs check integration and honesty of coverage reporting, not recall or complete medical-device surveillance.

| Check | Observed result | Evidence |
| --- | --- | --- |
| One request per official endpoint | FDA, Health Canada, Swissmedic, MHRA, and TGA autocomplete responded; BfArM catalogue returned 403 | [SOURCE_ENDPOINT_CHECK.json](SOURCE_ENDPOINT_CHECK.json) |
| FDA + Health Canada, trocar/2024, two-record and one-page caps | FDA exported two records; Health Canada retrieved two rows but none passed the requested dates; both remained partial; report integrity passed | [INTEGRATION_CHECK.json](INTEGRATION_CHECK.json) |
| All six sources, same bounded query | FDA exported two records, Swissmedic two notices, MHRA one notice; Health Canada had no date-matching rows within its capped page; TGA exceeded a deliberately short 35-second budget; BfArM failed with 403; five evidence records and an explicitly incomplete report passed file verification | [SIX_SOURCE_INTEGRATION_CHECK.json](SIX_SOURCE_INTEGRATION_CHECK.json) |
| TGA, narrower Versaport/2024 query, default 120-second source budget | Five device choices were selected; the source returned an explicit zero report count for that exact query. A separate bounded response inspection confirmed the devices, dates, count, and no-data message. This does not validate positive-result extraction or broader device coverage | [TGA_INTEGRATION_CHECK.json](TGA_INTEGRATION_CHECK.json) |
| Explicit official BfArM PDF | A public PDF downloaded successfully. The follow-up diagnosed eight image-only pages with no extractable text; the fetcher correctly requires OCR or visual review. This is infrastructure evidence, not a device-specific finding or catalogue-completeness claim | [SUPPLEMENT_CHECK.json](SUPPLEMENT_CHECK.json) |

The original TGA integration metadata preserves a contradictory empty-page warning discovered during review. The parser now distinguishes an explicitly verified zero-result page from an unexplained empty parse; the follow-up source regression covers that correction. The earlier metadata is retained as an honest record rather than silently rewritten to look like a later test.

### 28 November continuation

| Check | Result |
| --- | --- |
| MHRA dates | Official weekly notices contained individual manufacturer dates in an earlier year and month-only dates. The adapter retains their precision and separates them from weekly publication dates. [Source checks](MHRA_BFARM_ACCEPTANCE_CHECK.json) |
| BfArM access and scanned PDFs | Public web checks returned HTTP 403 for the catalogue. Individual official supplementary notices were readable, including one for ophthalmic trocar systems. The target hosted environment still needs an access check. Scanned supplements require manual visual review until OCR is verified there. [Source checks](MHRA_BFARM_ACCEPTANCE_CHECK.json) |
| TGA positive reference | A historical official printout covered six device selections and 15 reports. Positive-format and pagination regressions exercise the parser fixes. The current portal submission and production pagination require a live check. [Source checks](TGA_POSITIVE_ACCEPTANCE_CHECK.json) |
| Hosted report and lifecycle contracts | A controlled FDA replay exported two records with partial coverage. The simulated controller reconnected to the original root turn, verified the assessment and exports, and rechecked retained bytes before cleanup. The direct local FDA attempt produced a verified connection-failure report. Real hosted execution and retention remain pending. [Acceptance evidence](HOSTED_ACCEPTANCE_CONTINUATION.json) |
| Search quality | The reference set includes eight official FDA reports and four MHRA notices. All 12 keyword replay queries and three modeled edge cases passed. The separate exact-device diagnostic surfaced one concomitant-device mention as a broad keyword hit. Live retrieval precision and recall remain unmeasured. [Benchmark](SEARCH_QUALITY_BENCHMARK.md) |

## Remaining acceptance gates

1. **Credential and project.** Confirm the dedicated key's project and expiry metadata through the secure setup flow. Configure the controller secret and verify Agents read/write, Responses write, model access, and billing.
2. **Real hosted execution.** Submit the [bounded public FDA task](../examples/hosted-acceptance.txt) once, confirm `openai_hosted` provisioning, and inspect evidence of the toolkit command, official retrieval, partial coverage, and the report and assessment.
3. **Recovery and artifacts.** Reconnect to the same session, download the saved root turn's artifacts, verify the files against their manifests and assessment, and retain the files before cleanup. Record acceptance separately from local and fixture evidence.
4. **Source access.** Check BfArM from the target hosted environment. If access remains blocked, preserve that coverage gap. Verify a current positive portal search and pagination for TGA. MHRA and BfArM HTML listing coverage remains unverified.
5. **Cutover.** Verify the updated PR's CI, hosted acceptance, and deployment configuration before making the replacement the default. Continue to disclose incomplete source coverage in each report.

These gates are explicit operational limits, not unimplemented fixes hidden behind passing unit tests. The implemented collector reports caps, failures, publication-lag truncation, and unverified coverage. It can preserve separately discovered official documents without adding them to structured incident counts or declaring a failed catalogue complete.

## Known design limits

- A source process killed at its hard elapsed-time limit cannot recover pages still held only in that process's memory. Handled page failures retain prior data; hard timeouts remain failed/unknown. Per-page durable checkpoints are not implemented.
- The prompt's maximum of three retrieval runs is an instruction, not a platform-enforced billing ceiling. Record, page, request, and worker limits are enforced by code.
- Classifier labels remain screening suggestions. Exact supporting excerpts establish provenance but do not prove that a model's interpretation follows from them.
- Supplementary HTML and text-based PDFs are supported. Image-only PDFs require OCR or visual review, which is not implemented in the deterministic fetcher; missing text produces an explicit extraction failure.
- TGA excludes recent reports and uses the live portal's monthly cutoff. Effective query dates are disclosed separately from requested dates and occurrence dates.
- The toolkit has no company database credentials or document-write integration. This replacement preserves incident surveillance; it does not claim general regulation-version tracking or QMS amendment automation.

## Reproduction

```bash
python -m pip install -r requirements.txt -r requirements-agent.txt
python -m pytest -q
python -m cloud_agent search --request examples/trocar-search.json --output agent-results
```

Use the actual generated run directory with `python -m cloud_agent verify --run-dir ...`. The hosted start/resume/download commands and browser-operated workflow are documented in the [README](../README.md).
