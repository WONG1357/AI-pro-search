# QA/RA acceptance review

## Reviewer prompt

> Act as a medical-device Quality Assurance and Regulatory Affairs specialist at a manufacturer. You are screening public post-market surveillance (PMS) sources and preparing an evidence package for a colleague who must reproduce and review your work. Assume the colleague understands device vigilance and has never used this software. Start at the normal user entry point and complete realistic cases before judging usability.
>
> For each case, check whether you can identify the device and relevant model or product code, enter the intended dates and regulators, tell which inputs actually ran, and distinguish reported matches from narrative mentions. Check each date's meaning, evidence type, direct source link or source locator, and the full original excerpt. Reconcile retrieved and included counts, duplicate handling, limits, and requested versus effective dates. Treat confirmed no matching records, incomplete coverage, a failed source, and an unsearched source as different outcomes. Verify that the export retains the query, source status, original records, and manifest together. Open the export as a second reviewer would.
>
> Check whether the next action is obvious when inputs are invalid, a source fails, a run is interrupted, a new query changes the scope, or an export is missing. Look for avoidable re-entry, ambiguous labels, jargon, accessibility barriers, and details that are visible only in debug views. Include first-time setup, private access, public versus company-confidential inputs, evidence retention, and repeated searches. Use public or synthetic inputs for testing. Preserve records and logs already available when testing recovery.
>
> Compare every conclusion with the underlying source evidence, including negation and uncertainty. Count records and notices separately where supported. Keep causation, incidence, reportability, escalation, closure, and regulatory conclusions for qualified human review. Record the steps, expected and observed results, evidence, severity, and any check you could not perform. Treat a fixture, a live source, and a live hosted session as distinct kinds of evidence. Repair reproducible issues within scope and repeat the original case.

## What was exercised

| Reviewer task | Scenario and success criteria | Evidence |
| --- | --- | --- |
| Start a screening search | Run a time-bounded trocar query against selected regulators. Verify that the displayed terms, dates, and selected sources match the submitted run. | Streamlit interaction tests run the dashboard and real pipeline with fixture connectors. |
| Check mixed coverage | Retrieve an FDA record while a second selected connector fails. Surface incomplete coverage, identify the failed source, and omit unrequested regulators from result tabs. | The mixed-source dashboard replay verifies the status and record views. |
| Review an empty result | Compare a complete zero-match search and a failed search. Verify that the failure path offers no confirmed negative result after a rerun. | The dashboard replay checks both persistent messages. |
| Revise or retry a search | Edit a date, then submit an invalid query or simulate an interrupted refresh. Verify that the previous run and its evidence remain available, with the previous scope visible. | The dashboard replay checks the displayed earlier range and the original manifest. |
| Provide evidence to a colleague | Download one archive and reconcile every file against the manifest. Reproduce the scope and limits and inspect raw and reviewed records. | The exported package uses the original files, including both JSONL exports. |
| Check report and notice dates | Compare a publication date, a received date, and an occurrence date. Preserve the date used for filtering and any missing occurrence date. | Connector and pipeline regressions exercise the normalized fields. |
| Review a hosted interpretation | Compare an exact negative excerpt to a contradictory claim. Check that excerpts and source coverage are visible next to the interpretation. | Procedure and replay evaluate provenance and reviewer visibility. Semantic agreement requires human review. |

## Release boundaries

- **Highest remaining priority.** Exercise the full hosted flow from an ordinary reviewer account after credentials and private access are configured. The draft GitHub path is an engineering runner. Verify a user-facing launch, understandable progress, predictable passing and failed runs, result download, reconnecting, and retention. Use the approved target environment before making an operational release claim.
- **Clarification before execution.** Exercise an unclear missing-device request and an out-of-scope request in the hosted UI. The prompt asks for clarification or scope guidance. The existing one-shot workflow expects report artifacts, so the missing-report path needs its own acceptance check before use with nontechnical reviewers.
- **Interpretation review.** A legitimate excerpt can still contradict the statement it cites. Treat assessments as review drafts and open the quoted evidence before acting. Preserve field safety notices and adverse-event reports as separate evidence types. FDA explains the limits of incidence and causation claims from MAUDE reports. [FDA MAUDE limitations](https://www.fda.gov/medical-devices/mandatory-reporting-requirements-manufacturers-importers-and-device-user-facilities/about-manufacturer-and-user-facility-device-experience-maude-database)
- **Accessibility and handoff.** Streamlit interaction tests verify widget behavior and data flow. Layout at narrow widths, visible focus, screen-reader output, and download behavior in a real browser still need an observed trial. A second QA reviewer should complete an uninterrupted and a failed search before rollout.

## Evaluation tools

The replay uses the project's existing Python tests and Streamlit's test interface. Promptfoo's Echo provider can evaluate captured assessments later without calling a model. Use deterministic assertions for schema, evidence IDs, coverage, and required disclosures, with human review for semantic agreement. Apify MCP can support separate connector experiments when a specific source needs an Actor. The existing direct official-source adapters cover the current fixtures and the TGA live checks. [Promptfoo Echo provider](https://www.promptfoo.dev/docs/providers/echo/) · [Apify MCP documentation](https://docs.apify.com/mcp)
