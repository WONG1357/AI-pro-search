# Search-quality benchmark

The reference set contains eight FDA MAUDE malfunction reports and four MHRA field safety notices. It covers trocar brand and model searches across manufacturers, source-specific dates, one receipt-year boundary, uncertain occurrence timing, a month-only notice date, negated findings, and overlapping records. The public record identifiers and links are in [SEARCH_QUALITY_CHECK.json](SEARCH_QUALITY_CHECK.json). The [reference data](../benchmarks/search_quality_reference.json) retain the relevant fields and short paraphrases for repeatable checks.

## Run the benchmark

From the repository root with the application dependencies installed:

```bash
python -m benchmarks.search_quality --output search-quality-result.json
```

The replay uses the FDA normalizer, the MHRA listing parser and normalizer, and the production pipeline's date filtering, keyword matching, deduplication, and classification. It reconstructs the minimum source-shaped input from publicly verified fields. It does not need a key, make network requests, or depend on current regulator rankings. The official listings and reports remain the evidence for the curated facts. The staged missing-ID, duplicate-query, and counterfactual-negation inputs are clearly marked in the reference data.

The command exits with status 1 if a required case misses an expected record, retains an unexpected one, repeats a record, or changes a checked field. The primary-device-only diagnostic is measured separately because the current request uses broad keyword matching across device metadata and narrative text.

## Replay results

| Evaluation | Cases passed | True positives | False positives | False negatives | Precision | Recall |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Official keyword replay | 12/12 | 17 | 0 | 0 | 100% | 100% |
| Modeled edge cases | 3/3 | 4 | 0 | 0 | 100% | 100% |
| Exact reported-device diagnostic | 0/1 | 1 | 1 | 0 | 50% | 100% |

These are aggregate search membership counts across query–record pairs in this small, intentionally selected reference set. The results provide neither a live retrieval metric nor a system-wide precision or recall estimate. Classification checks apply to the explicit cases in the result, including a reported fracture with no patient injury and a counterfactual where the fracture itself was negated.

The exact reported-device diagnostic returns MAUDE 20239840 because its narrative mentions VersaOne and the search accepts narrative matches. FDA lists VersaPort as the reported device for that record. The broad search can present it as a related candidate. A workflow limited to the reported VersaOne device requires an explicit identity distinction in the output.

FDA MAUDE reports 20185941 and 20273790 share a model and lot but describe different procedures. Both should remain in results for that model. The replay also verifies that repeat hits for one record collapse to one row while the modeled missing-identifier rows remain distinct when their content differs.

## Optional live checks

```bash
python -m benchmarks.search_quality --live --output search-quality-live.json
```

The live mode checks whether two known positives can be recovered from the actual FDA and MHRA connectors. The FDA probe searches for VersaPort on the FDA receipt day of report 19632393. The MHRA probe searches for Spaceplus Infusomat on the date of notice 30931286. Each probe requests at most two pages per query, 25 records, and a 10-second per-request timeout. The report records attempted source coverage and any warnings or errors. Other returned rows have no membership label and are counted separately. An observed anchor counts as a known positive. An absent anchor counts as a confirmed retrieval miss only if the adapter established complete coverage. Failed, partial, and unverified coverage leave absent anchors unverified and the command exits with status 1. These probes give no estimate of live precision or catalogue completeness.

The recorded 2026-11-28 baseline is an offline replay. A separate live attempt reached neither regulator's catalogue. Both connectors reported failed coverage and connection errors after three attempts. The execution shell could not resolve the regulator domains. Both known-positive probes remain unverified.
