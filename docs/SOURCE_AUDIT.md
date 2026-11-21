# Source connector audit and remediation

Reviewed: 21 November 2026 (Hong Kong). Original repository state: `d67c548ae5aa7ebe9d0b233204c277dbf62cf413` in [WONG1357/AI-pro-search](https://github.com/WONG1357/AI-pro-search/tree/d67c548ae5aa7ebe9d0b233204c277dbf62cf413).

This audit concerns the six existing medical-device incident/recall sources. It does not establish a general regulatory corpus, regulation-version search, or clinical validity. Original line references below refer to that immutable commit; current files have moved as fixes were applied. The GitHub issue collection was empty when inspected, so these are code findings, not a claim that a historical issue tracker contained them.

## Findings and implemented corrections

| ID | Original evidence | Defect and consequence | Correction in replacement toolkit |
| --- | --- | --- | --- |
| S01 | `sources_openfda.py:103–116`; `sources/openfda_maude.py:78–82` | FDA HTTP failures and timeouts were printed, then returned as successful empty or partial searches. A failed request could be mistaken for no incidents. | Bounded transient retries and typed failure/partial coverage; the source result exposes errors and preserves earlier successful pages. |
| S02 | `sources_openfda.py:103–104` | Every HTTP 404 was treated as an empty query, including an invalid endpoint or unexpected response. | Only the documented `NOT_FOUND` / “No matches” error shape counts as a verified empty query; other 404 responses fail. |
| S03 | `sources_openfda.py:129–130` | Retrieval silently stopped at the fixed query limit, without inspecting the API total. | Returned totals and per-query counts are retained; record/page caps explicitly produce partial coverage and warnings. A source-wide record budget also bounds retrieval. |
| S04 | `sources_openfda.py:82` | User keywords were interpolated into the search expression without escaping quotes or backslashes. | Escape expression literals before constructing an FDA query; test a quoted device name. |
| S05 | `sources_openfda.py:71–76` | A requested date interval outside a selected year was replaced with that whole year. This fetched out-of-range records and consumed budgets. | Intersect every year with the requested interval; skip disjoint years and reject reversed/invalid date inputs. |
| S06 | `sources_openfda.py:195–196` | Generic device names were written to `brand_names`. | Preserve separate brand, generic, model and product-code fields. |
| S07 | `sources_openfda.py:184–202` | FDA normalization discarded the original record and event/manufacturer details. Later interpretation could not inspect full native evidence. | Retain `raw_record`, source metadata, native event type, manufacturer, model and product code. |
| S08 | `sources_openfda.py:169–171` | A missing occurrence date was silently filled with the receipt date. | Leave an unknown event date empty; retain receipt/report dates separately. Core filtering states its date basis. |
| S09 | `sources_openfda.py:60` | The three query fields omitted the model-number and product-code fields, although these are useful device identifiers. | Add `device.model_number` and `device.device_report_product_code`, verified against the official FDA field reference. Expose distinct match categories and accurate progress totals. Other connectors still provide keyword-based searches. |
| S10 | `sources_openfda.py:113,121` | Metadata and nested row normalization had no shape/count contract. A malformed later page could discard earlier rows, or contradictory totals could look complete. | Validate nested metadata, row arrays, nonnegative consistent totals, page repeats and normalization inside the page failure boundary. Preserve prior rows. |
| S11 | `sources/health_canada_mdi.py:228–237` | Any JSON object without `data`, including an error response, became an empty success. | Reject error objects, missing arrays, malformed rows and invalid counts. |
| S12 | `sources/health_canada_mdi.py:128–176` | No repeated-page guard existed; a server ignoring offsets could loop indefinitely or repeat until a user cap. | Per-keyword content hashes, bounded page budgets, actual returned-row offsets and explicit partial coverage. |
| S13 | `sources/health_canada_mdi.py:144–147`; connector catch at 84–91 | A later HTTP error discarded all records already retrieved for the source. | Catch page failures, preserve previous valid records, and mark failed/partial coverage according to available data. |
| S14 | `sources/health_canada_mdi.py:152–156` | Rows without IDs were discarded; explicit JSON null became the literal `None`, collapsing multiple records. | Retain missing/null-ID rows with distinct stable content identities; test two null-ID records. |
| S15 | `sources/health_canada_mdi.py:247,276–280` | The receipt date was represented as an occurrence date. | Keep unknown `event_date` empty, retain `report_date`/`date_received`, filter by receipt date explicitly, and record this basis. |
| S16 | `sources/health_canada_mdi.py:149–175` | Changing or contradictory totals were accepted. | Reject counts below returned rows and counts changing during one paginated query; retain previous pages as partial. |
| S17 | `sources/swissmedic.py:188–196` | Missing `content` and missing pagination metadata defaulted to empty/last-page values. Error responses could appear complete. | Validate payload structure; missing pagination proof remains unverified; invalid types/counts fail. Explicit zero totals are not replaced by fallback values. |
| S18 | `sources/swissmedic.py:253–274` | Pagination stopped when a page added no globally new IDs. A second keyword with an overlapping first page lost its later unique records. | Deduplicate records globally while controlling pagination per keyword. An overlap with a previous keyword no longer stops the new query. |
| S19 | `sources/swissmedic.py:237–241` | Hashing the entire response allowed identical record pages with different pagination metadata to look different. Duplicate pages could satisfy total counts. | Hash parsed page contents per keyword, verify page numbers and consistent totals, and mark duplicate/changing result sets partial. |
| S20 | `sources/swissmedic.py:236`; connector catch at 98–105 | A later page failure erased previously retrieved publications. | Isolate page/record failures and retain valid prior publications with partial coverage. |
| S21 | `sources/mhra_fsca.py:204,215–228` | Repeated-page hashes were shared across keywords, while each notice was filtered by the current keyword. Identical finder pages caused valid second-term notices to be missed. | Scope page hashes to each keyword; cache notice HTML for reuse but re-parse/filter it for each requested term. |
| S22 | `sources/mhra_fsca.py:225–227,256–258` | Failed detail pages were warnings attached to success with no completeness status. | Preserve successful notices and mark the source partial/failed; bounded retries cover transient errors. |
| S23 | `sources/mhra_fsca.py:220–222` | Changed HTML or an access challenge with no matching selectors looked like zero results. | Reject recognized challenges and unrecognized empty listing structures. HTML coverage remains unverified even after successful parsing. |
| S24 | `sources/bfarm.py:117–120` | Search stopped after at least one record was found for every selected year, not after all terms/results were searched. | Remove that stop condition. Only explicit page/record/time budgets limit remaining terms, with partial status. |
| S25 | `sources/bfarm.py:467–482` | The presence of `trocar` replaced all supplied brand and other search terms with `Trocar`; some broad terms were silently dropped. | Preserve every supplied device query term with stable deduplication. Screening-only outcomes/components are excluded from retrieval by the core pipeline. |
| S26 | `sources/bfarm.py:101,145–154` | `max_pages` was accepted but unused, and only one period-result page was parsed. | Follow explicit same-host next-page links, detect repeats, enforce page/record budgets and flag truncation. HTML listing coverage is not declared exhaustive. |
| S27 | `sources/bfarm.py:64`; other connector mutable configs | BfArM ignored a requested timeout; several connectors mutated stored configuration during a request. | Copy per-call configuration and apply requested timeouts without changing the reusable connector instance. |
| S28 | `sources/tga_daen.py:247–267,964–983` | Device autocomplete ignored HTTP failures and turned invalid JSON into an empty result. The original test explicitly expected success after HTTP 500. | Check transport and JSON shape before parsing. Correct the original regression expectation and add HTTP/error/schema tests. |
| S29 | `sources/tga_daen.py:683–698,748–751` | An early return on zero parsed rows bypassed the later check that the source expected reports. | Expected-positive or unknown counts with zero parsed rows fail. A complete empty result requires the visible zero count, explicit source no-data state, and matching selected-device count/date echoes. Ignore script/template count placeholders. Legitimate empty first pages do not emit an incomplete-pagination warning; any remaining incomplete warning downgrades coverage. |
| S30 | `sources/tga_daen.py:764–831` | Repeated/failed pagination or a cap could lose records or end without a structured partial state. | Retain already parsed rows, report repeat/navigation/fetch failures, enforce caps before fetching another page and propagate partial coverage. |
| S31 | `sources/tga_daen.py:779–782`; PDF extraction at 385–394 | The PDF branch bypassed the page budget and returned before applying limits. | Apply a PDF page budget, report partial extraction and cap parsed report rows. The complete downloaded PDF is still loaded by the parser; the source process has a separate elapsed-time budget. |
| S32 | `sources/tga_daen.py:465–506,990–1030` | TGA normalization discarded original report/import rows and print-report provenance, and represented the DAEN report's `Date` as an actual occurrence date despite TGA's documented distinction. | Retain raw rows and source metadata. Map the standard Date/notification column to `report_date`/`date_received`; keep occurrence dates unknown unless explicitly supplied separately. Filter on report dates, preserve `effective_start`/`effective_end`, and disclose any publication-lag truncation. Do not invent permanent record links. |
| S33 | `sources/base.py:15–22,86–90` | A boolean success flag could not distinguish complete, partial, unavailable and unverified coverage; the placeholder connector returned success. | Add backward-compatible coverage/count fields, infer them from adapter metadata and make an unavailable placeholder an explicit failure. |
| S34 | Multiple source request sites; BfArM 205–223 | Most sources had no bounded transient retry; BfArM could sleep for an arbitrarily large Retry-After. | Centralize finite retries for read-only search calls. Respect short Retry-After values and stop explicitly when a cooldown exceeds the retry budget. Do not retry an access denial as if it were an empty query. |

`pipeline/source_support.py` implements shared HTTP and coverage behavior. `tests/test_source_regressions.py` exercises the failure boundaries above; existing parser fixtures remain in `tests/test_sources.py`.

### Additional integration corrections

- The core pipeline now sends only device identity keywords to source retrieval. Components and outcome terms remain classification inputs. This prevents `injury` or `leak` from broadening a device search and then bypassing its local relevance filter through `source_query_match`.
- The evidence layer checks a validated Swissmedic document link before a portal URL. Available official PDFs therefore remain usable as direct evidence links.
- Source retrieval counts, returned records and final exported matches remain separate in coverage reporting. Overlapping OR-query totals must not be summed into a unique report total.
- The five FDA query fields share bounded source budgets. A capped run is partial even when its successful first query returned many rows; the agent must narrow/refine a search to resolve an observed gap.

## Coverage semantics

| Value | Meaning |
| --- | --- |
| `complete` | The connector exhausted its declared query with coherent pagination/zero-result evidence. This is not a claim of all worldwide incidents, clinical causation, or absence of risk. |
| `partial` | Some records/query results were retrieved, but a cap, lag, failed page, changing result set, or other known gap remains. Zero retained matches in a partial run do not prove absence. |
| `failed` | The source could not produce a usable verified retrieval result. The report must distinguish this from zero matching records. |
| `unverified` | The request/parse succeeded, but completeness cannot be established, for example an HTML listing or missing total metadata. |

Native adverse-event reports and safety notices remain different evidence types. Records can overlap across regulators; source counts are not unique patients or incidence rates. A portal URL is a locator, not an individual case citation. Missing occurrence dates remain missing; a reporting/publication date must not be silently described as an occurrence date.

## Verification

The final source-focused run was:

```text
python -m pytest tests/test_sources.py tests/test_source_regressions.py -q
92 passed
```

This consists of 47 existing source tests and 45 new regression cases (including parametrized cases). Three existing expectations were corrected because they encoded misleading behavior: successful TGA HTTP failure, Health Canada receipt date presented as occurrence date, and the TGA report date presented as occurrence date. FDA progress expectations were updated from three to five documented search fields. TGA regressions cover separate occurrence/report dates, report-date filtering, unknown occurrence dates, the current portal's cutoff before/on/after its monthly boundary, effective-date metadata even for empty/capped results, and verified zero results versus missing/mismatched/template evidence. These are deterministic mocked tests. The complete application/hosted test run is documented by the integration owner separately.

### Bounded live checks

A single bounded request per official endpoint was made at `2026-11-21T11:27:41Z`. Exact results are retained in [SOURCE_ENDPOINT_CHECK.json](SOURCE_ENDPOINT_CHECK.json). These checks prove endpoint response/shape at that time; they do **not** prove full connector execution, end-to-end OpenAI hosting, parser recall, or exhaustive source coverage.

| Source | Observed response | Scope and interpretation |
| --- | --- | --- |
| [FDA openFDA](https://api.fda.gov/device/event.json) | HTTP 200, JSON `meta`/`results` | Generic-device `trocar` and receipt year 2024 reported 125 hits; probe requested and received one row. This is one field query, not the replacement's full dataset. |
| [Health Canada MDI](https://hpr-rps.hres.ca/mdi_dep/serverSideProcessing.php) | HTTP 200, valid DataTables JSON | `trocar` reported 351 keyword hits before local year/date filtering; probe requested and received one row. Response Content-Type was HTML despite valid JSON, so parsing relies on validated JSON shape rather than an overly strict MIME assumption. |
| [Swissmedic](https://fsca.swissmedic.ch/mep/api/publications/search) | HTTP 200, valid paginated JSON | `trocar`, calendar year 2024, reported and returned four publications. |
| [MHRA finder](https://www.gov.uk/drug-device-alerts) | HTTP 200, listing HTML | Listing endpoint responded; individual notice/attachment completeness was not established. |
| [BfArM expert search](https://www.bfarm.de/SiteGlobals/Forms/Suche/EN/Expertensuche_Formular.html) | **HTTP 403** | Access remains blocked from this test environment. Code fixes cannot grant source access. The replacement must report this source as failed and may use clearly labeled supplemental official web evidence; it must not claim exhaustive BfArM retrieval. |
| [TGA autocomplete](https://apps.tga.gov.au/PROD/DEVICES/daen-entry.aspx/GetSearchItems) | HTTP 200, JSON `d` list | Returned 153 device choices for `trocar`. Device choices are not adverse-event reports. The full consent/search/list/PDF workflow was not live-validated by this endpoint probe. |

A separate, narrowly bounded TGA follow-up checked the previously recorded `Versaport`/2024 empty result. Both the initial result and List of reports views echoed five selected devices and the requested dates, displayed a zero report total and the explicit no-data callout. The contradictory pagination warning was local code behavior and is corrected. [TGA_ZERO_CONFIRMATION.json](TGA_ZERO_CONFIRMATION.json) records the exact evidence and successful offline replay of the captured HTML after the fix. The historical [TGA_INTEGRATION_CHECK.json](TGA_INTEGRATION_CHECK.json) remains unchanged. This validates the source's empty state for that query, not positive-report parser recall or broader source completeness.

### FDA field verification

The official [searchable-field explorer](https://open.fda.gov/apis/device/event/searchable-fields/) links the [device-event YAML field reference](https://open.fda.gov/fields/deviceevent.yaml). The downloaded reference on 21 November 2026 contains `device.model_number` (device label/package model identifier) and `device.device_report_product_code` (FDA three-letter product classification identifier). These field names were verified from the official schema before adding them. The agent must disclose actual queried fields; an FDA-specific field does not establish equivalent structured-field support in other sources.

### TGA date and availability verification

The official [DAEN search guidance](https://www.tga.gov.au/safety/adverse-events/medical-device-adverse-events/how-search-medical-devices-database-adverse-event-notifications), checked on 21 November 2026, distinguishes a report date from the actual adverse-event date and describes a three-month reporting exclusion. The [public entry page](https://apps.tga.gov.au/PROD/DEVICES/daen-entry.aspx) also states that recent reports are excluded. The currently served [portal JavaScript](https://apps.tga.gov.au/PROD/DEVICES/js/daen-entry-all.min.js), function `initialiseDateSelection`, implements a stricter calendar limit: the most recent first Thursday, minus three months. For access on 21 November 2026, that limit is 5 August 2026. The connector retains this verified live search limit rather than assuming access-date-minus-three-months is accepted. It records the actual requested interval after capping; an effective end date is a search boundary, not proof that all eligible reports were retrieved. Standard report dates remain separate from explicitly supplied occurrence dates throughout normalization and filtering.

## Remaining source and operational limits

1. **BfArM access:** the observed HTTP 403 is unresolved externally. A different response in the eventual OpenAI-hosted environment requires its own verification. Do not label BfArM working based on the mocked pagination tests.
2. **Website changes:** TGA, MHRA and BfArM expose web workflows rather than a guaranteed stable API contract. Recognized format changes fail or downgrade coverage; test fixtures cannot guarantee future markup or every notice layout.
3. **TGA availability and dates:** the current published policy and live JavaScript cutoff were independently verified as described above. The source excludes recent reports, so a requested interval extending beyond its accepted end is partial even if every available report is parsed. The recorded `effective_end` does not establish report completeness. A future portal policy change will require rechecking the cutoff implementation; no clinical occurrence date is inferred from the standard DAEN report Date column.
4. **Budgets:** record/page caps are deliberate operational bounds. A source process killed at its overall elapsed-time limit cannot return rows not yet written by the worker. That outcome is failed/unknown, not a zero-result finding. Per-page durable checkpoints are not implemented.
5. **Semantic evaluation:** these fixes prevent specific omissions, false zeros and evidence loss. They do not establish a measured recall/precision target against a labeled multi-regulator reference set, nor the semantic correctness of an agent interpretation. Provenance validation and human review remain necessary for a consequential decision.
6. **Hosting/API access:** source tests and public endpoint probes do not establish availability of the user's OpenAI API project, new key, model or hosted Agents API. A real hosted session and artifact check are required for that separate claim.

No honest finite audit can prove that every future source failure or every possible defect has been eliminated. The implemented corrections are linked to concrete original code paths and regression cases, with unverified/external conditions stated explicitly.
