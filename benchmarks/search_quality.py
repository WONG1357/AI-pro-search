"""Evaluate medical-device search against a bounded reference set."""

from __future__ import annotations

import argparse
from contextlib import redirect_stdout
from datetime import date
from html import escape
import json
from pathlib import Path
import sys

import pandas as pd

from pipeline.pipeline import (
    classify_records,
    clean_records,
    deduplicate_records,
    filter_records,
    run_pipeline,
)
from pipeline.sources.mhra_fsca import normalize_mhra_fsca_record, parse_mhra_notice_page
from pipeline.sources_openfda import _openfda_results_to_rows

REFERENCE = Path(__file__).with_name("search_quality_reference.json")
FDA_DETAIL = "https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfmaude/detail.cfm"
MHRA_NOTICES = "https://www.gov.uk/drug-device-alerts/"


def load_records(reference):
    """Normalize curated source fields without contacting the regulators."""
    records = {}
    anchors = {}
    for item in reference["fda"]:
        key = item["key"]
        source_row = {
            "mdr_report_key": key,
            "report_number": item["report_number"],
            "date_of_event": item["event_date"],
            "date_received": item["received_date"],
            "manufacturer_d_name": item["manufacturer"],
            "event_type": "Malfunction",
            "device": [{
                "brand_name": item["brand"],
                "generic_name": "Laparoscope, general and plastic surgery",
                "model_number": item["model"],
                "device_report_product_code": "GCJ",
            }],
            "mdr_text": [{"text": item["text"]}, {"text": item.get("additional_text", "")}],
        }
        records[key] = _openfda_results_to_rows([source_row])[0]
        anchors[key] = f"{FDA_DETAIL}?mdrfoi__id={key}&pc=GCJ"

    for item in reference["mhra"]:
        key = item["key"]
        page_url = MHRA_NOTICES + item["page"]
        # Reconstruct the relevant GOV.UK listing markup from the curated fields.
        # These are projected facts, with no claim to preserve the original HTML.
        html = (
            f"<h1>{escape(item['page_title'])}</h1>"
            f"<dl><dt>Issued:</dt><dd>{escape(item['weekly_issued_date'])}</dd></dl>"
            f"<div class='gem-c-govspeak'><h3>{escape(item['title'])}</h3>"
            f"<p>{escape(item['notice_date'])}</p><p>{escape(item['details'])}</p>"
            f"<p>MHRA reference: {escape(key)}</p></div>"
        )
        parsed = parse_mhra_notice_page(html, page_url)
        if len(parsed) != 1:
            raise ValueError(f"Expected one replayed notice for MHRA reference {key}")
        records[key] = normalize_mhra_fsca_record(parsed[0])
        anchors[key] = page_url

    return records, anchors


def replay_case(case, records):
    """Run production search, deduplication and classification over fixed inputs."""
    if "copies" in case:
        candidates = []
        for copy in case["copies"]:
            row = {**records[copy["from"]], "reference_key": copy["as"]}
            if copy.get("drop_ids"):
                row.update(event_id="", report_number="", event_link="")
            if "narrative" in copy:
                row["narrative_text"] = copy["narrative"]
                row["event_type"] = copy.get("event_type", "")
            candidates.append(row)
    else:
        candidates = [
            {**row, "reference_key": key}
            for key, row in records.items()
            if row["source"] == case["source"]
        ]

    start, end = case["start"], case["end"]
    years = range(date.fromisoformat(start).year, date.fromisoformat(end).year + 1)
    cleaned = clean_records(pd.DataFrame(candidates))
    filtered = deduplicate_records(filter_records(cleaned, case["keywords"], years, start, end))
    if not filtered.empty:
        profile = "Trocar" if case["source"] == "FDA_MAUDE" else "Custom"
        filtered["category"] = classify_records(filtered, profile).map(lambda result: result.category)
    return filtered.to_dict("records"), {row["reference_key"] for row in candidates}


def score_case(case, observed_rows, available):
    found = {row["reference_key"]: row for row in observed_rows}
    expected, actual = set(case["expect"]), set(found)
    if expected - available:
        raise ValueError(f"{case['name']} expects reference keys absent from its inputs: {sorted(expected - available)}")
    duplicate_rows = len(observed_rows) - len(actual)
    differences = []
    for key, fields in case.get("check", {}).items():
        if key in found:
            for field, value in fields.items():
                observed = found[key].get(field)
                if observed != value:
                    differences.append({"reference": key, "field": field, "expected": value, "observed": observed})
    return {
        "case": case["name"],
        "source": case["source"],
        "keywords": case["keywords"],
        "start": case["start"],
        "end": case["end"],
        "expected": sorted(expected),
        "observed": sorted(actual),
        "true_positives": sorted(actual & expected),
        "false_positives": sorted(actual - expected),
        "false_negatives": sorted(expected - actual),
        "true_negative_count": len(available - (expected | actual)),
        "duplicate_rows": duplicate_rows,
        "field_differences": differences,
        "passed": actual == expected and not duplicate_rows and not differences,
    }


def summarize(cases):
    tp, fp, fn = (
        sum(len(case[field]) for case in cases)
        for field in ("true_positives", "false_positives", "false_negatives")
    )
    return {
        "case_count": len(cases),
        "passed_case_count": sum(case["passed"] for case in cases),
        "true_positives": tp,
        "false_positives": fp,
        "false_negatives": fn,
        "true_negatives": sum(case["true_negative_count"] for case in cases),
        "precision": tp / (tp + fp) if tp + fp else None,
        "recall": tp / (tp + fn) if tp + fn else None,
        "duplicate_row_count": sum(case["duplicate_rows"] for case in cases),
        "field_difference_count": sum(len(case["field_differences"]) for case in cases),
    }


def run_live_probe(case, records, anchors):
    """Check known positives through the real adapter and preserve its coverage."""
    start, end = case["start"], case["end"]
    years = range(date.fromisoformat(start).year, date.fromisoformat(end).year + 1)
    with redirect_stdout(sys.stderr):
        result = run_pipeline(
            keywords=case["keywords"], years=years, export_results=False,
            selected_sources=[case["source"]], start_date=start, end_date=end,
            source_options={"_global": {"request_timeout": 10, "max_pages": 2, "max_records": 25}},
        )
    anchor_keys = set(case["expect"])
    found = []
    for row in result["filtered"].to_dict("records"):
        for key in anchor_keys:
            match = row["event_id"] == key if case["source"] == "FDA_MAUDE" else (
                row["raw_link"] == anchors[key] and row["product_name"] == records[key]["product_name"]
            )
            if match:
                found.append({**row, "reference_key": key})
    coverage = result["source_status"]["completeness"].iloc[0]
    scored = score_case(case, found, anchor_keys)
    # Missing anchors are evidence of a retrieval miss only when the connector
    # established complete coverage for the requested query.
    unverified_keys = scored["false_negatives"] if coverage != "complete" else []
    verification_status = "passed" if scored["passed"] else "failed"
    if unverified_keys:
        scored["false_negatives"] = []
        verification_status = "unverified"
    return {
        **scored,
        "verification_status": verification_status,
        "unverified_reference_keys": unverified_keys,
        "source_coverage": coverage,
        "coverage_errors": result["source_errors"].to_dict("records"),
        "coverage_warnings": result["source_warnings"].to_dict("records"),
        "unlabeled_rows": len(result["filtered"]) - len(found),
        "checks": "Known positives only. Other returned records have no gold membership label.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, default=REFERENCE)
    parser.add_argument("--output", type=Path, help="Write the JSON result to this path instead of stdout")
    parser.add_argument("--live", action="store_true", help="Also run the explicitly bounded live connector probes")
    args = parser.parse_args()
    reference = json.loads(args.reference.read_text(encoding="utf-8"))
    records, anchors = load_records(reference)
    suites = {"official_keyword_replay": [], "exact_reported_device_diagnostic": [], "modeled_edge_replay": []}
    for case in reference["replay_cases"] + reference["edge_case_inputs"]:
        if "copies" in case:
            suite = "modeled_edge_replay"
        elif case.get("intent") == "exact_reported_device":
            suite = "exact_reported_device_diagnostic"
        else:
            suite = "official_keyword_replay"
        suites[suite].append(score_case(case, *replay_case(case, records)))
    report = {
        "reference_version": reference["version"],
        "official_sources_checked_on": reference["official_sources_checked_on"],
        "official_records": anchors,
        "replay": {name: {"summary": summarize(cases), "cases": cases} for name, cases in suites.items()},
    }
    if args.live:
        report["live_known_positive_probes"] = [
            run_live_probe(case, records, anchors) for case in reference["replay_cases"] if case.get("live")
        ]
    payload = json.dumps(report, indent=2, allow_nan=False) + "\n"
    if args.output:
        args.output.write_text(payload, encoding="utf-8")
    else:
        print(payload, end="")
    required = suites["official_keyword_replay"] + suites["modeled_edge_replay"] + report.get("live_known_positive_probes", [])
    return 0 if all(case["passed"] for case in required) else 1


if __name__ == "__main__":
    raise SystemExit(main())
