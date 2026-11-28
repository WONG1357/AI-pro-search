"""Regressions reproduced against legacy commit d67c548 before replacement."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import hashlib
import json

import pandas as pd
import pytest
from openpyxl import load_workbook

from pipeline.classifier import classify_incident_details, classify_custom_incident_details
from pipeline.export import export_pipeline_results
from pipeline.pipeline import clean_records, deduplicate_records, filter_records, run_pipeline, summarize_by_source_category, summarize_by_year_category
from pipeline.progress import ProgressReporter
from pipeline.sources.base import SourceFetchResult, ensure_unified_columns
from pipeline.utils import contains_any_keyword, date_or_year_in_range, format_fda_date, parse_date_value, parse_year_from_date


def row(source="A", event_id="1", **values):
    return {"source": source, "event_id": event_id, "event_date": "2025-01-02", "product_name": "Trocar", "narrative_text": "Trocar cannula broke.", **values}


def test_summary_namespaces_ids_and_counts_rows_without_ids():
    frame = pd.DataFrame([
        row("A", "1", search_year=2025, category="Other"),
        row("B", "1", search_year=2025, category="Other"),
        row("A", "", search_year=2025, category="Other"),
    ])
    assert summarize_by_year_category(frame).incident_count.sum() == 3
    summary = summarize_by_source_category(frame).set_index("source")
    assert summary.loc["A", "incident_count"] == 2
    assert summary.loc["B", "incident_count"] == 1


def test_no_id_dedup_uses_full_text_stable_digest_and_source_identity():
    frame = clean_records(pd.DataFrame([
        row("A", "", narrative_text="x" * 600 + "fatal injury"),
        row("A", "", narrative_text="x" * 600 + "minor injury"),
        row("A", "", narrative_text="x" * 600 + "minor injury"),
        row("B", "", narrative_text="x" * 600 + "minor injury"),
    ]))
    result = deduplicate_records(frame)
    assert len(result) == 3
    assert result.record_uid.str.fullmatch(r"[a-f0-9]{64}").all()
    assert result.duplicate_count.sum() == 1
    assert result.record_uid.tolist() == deduplicate_records(frame).record_uid.tolist()


@pytest.mark.parametrize("text,expected", [
    ("The cannula broke. No leak was observed.", "Broken cannula"),
    ("The cannula broke but no patient injury occurred.", "Broken cannula"),
    ("No patient injury was reported.", "Other"),
    ("No endotoxin contamination was detected.", "Other"),
    ("The trocar caused vascular injury.", "Trocar-associated vascular injury"),
    ("Patient age unknown. Device malfunctioned.", "Unclassified mechanical issue"),
    ("Leak was not observed.", "Unrelated / insufficient detail"),
    ("The cannula did not break.", "Other"),
])
def test_classification_scopes_negation_and_does_not_invent_design_or_causation(text, expected):
    assert classify_incident_details(text).category == expected


def test_separate_negative_outcome_does_not_erase_positive_device_problem():
    frame = clean_records(pd.DataFrame([row(device_problem_text="Cannula broke", patient_problem_text="No patient injury", narrative_text="No leak observed")]))
    assert classify_incident_details(frame.iloc[0].classification_text).category == "Broken cannula"


def test_custom_negation_does_not_erase_different_component_failure():
    result = classify_custom_incident_details("No leak occurred. Pump housing cracked.", ["pump housing"], ["cracked"])
    assert result.category == "Pump Housing - Cracked issue"


@pytest.mark.parametrize("source,expected", [("12. März 2024", "2024-03-12"), ("5. Oktober 2024", "2024-10-05"), ("01/02/2025", "2025-02-01")])
def test_source_dates_never_fill_a_missing_component_with_today(source, expected):
    assert format_fda_date(source) == expected


@pytest.mark.parametrize("value", ["20241332", "20240000", "2024-02-30", "08/09", "not a date"])
def test_invalid_dates_do_not_get_rescued_as_a_year(value):
    assert parse_year_from_date(value) is None
    assert not date_or_year_in_range(value, "2024-01-01", "2024-12-31")


def test_partial_dates_require_whole_interval_inclusion():
    assert not date_or_year_in_range("March 2024", "2024-11-01", "2024-11-30")
    assert not date_or_year_in_range("2024", "2024-11-01", "2024-11-30")
    assert date_or_year_in_range("March 2024", "2024-03-01", "2024-03-31")
    assert date_or_year_in_range("2024", "2024-01-01", "2024-12-31")


def test_datetime_input_is_compared_as_date():
    assert date_or_year_in_range(datetime(2025, 1, 2, 10), "2025-01-01", "2025-12-31")


def test_report_date_does_not_invent_event_date():
    frame = clean_records(pd.DataFrame([row("FDA_MAUDE", "1", event_date="", date_received="2025-01-02")]))
    assert frame.iloc[0].event_date == ""
    assert frame.iloc[0].search_date == "2025-01-02"
    assert frame.iloc[0].search_date_basis == "report_date"


def test_tga_search_uses_report_date_even_when_occurrence_date_is_known():
    frame = clean_records(pd.DataFrame([
        row("TGA_DAEN", "1", event_date="2024-12-20", date_of_event="2024-12-20", report_date="2025-01-05"),
        row("TGA_DAEN", "2", event_date="2024-12-21", date_of_event="2024-12-21", report_date="", date_received="2025-01-06"),
    ]))
    assert frame.search_date.tolist() == ["2025-01-05", "2025-01-06"]
    assert frame.search_date_basis.tolist() == ["report_date", "report_date"]
    assert frame.event_date.tolist() == ["2024-12-20", "2024-12-21"]
    assert len(filter_records(frame, ["trocar"], [2025], start_date="2025-01-01", end_date="2025-12-31")) == 2


def test_tga_ui_exposes_report_date_without_labelling_it_as_occurrence(monkeypatch):
    import app
    frame = clean_records(pd.DataFrame([row("TGA_DAEN", "1", event_date="", report_date="2025-01-05")]))
    columns = app.source_table_columns("TGA_DAEN", frame)
    assert "report_date" in columns and "search_year" in columns
    assert "event_date" not in columns
    assert "2025-01-05" in app.format_record_label(0, frame.iloc[0])
    def capture(record, fields, **kwargs):
        assert "report_date" in fields
        assert app.FIELD_LABELS["report_date"] == "Report date"
        assert record.event_date == ""
    monkeypatch.setattr(app, "render_detail_fields", capture)
    app.render_tga_daen_detail(frame.iloc[0])


def test_ui_handles_unknown_occurrence_year_for_report_dated_records():
    import app
    frame = clean_records(pd.DataFrame([row("HEALTH_CANADA_MDI", "1", event_date="", date_received="2025-01-05")]))
    assert frame.iloc[0].search_date_basis == "receipt_date"
    assert frame["year"].isna().all()
    assert not app.column_has_values(frame["year"])
    assert app.display_year_column(frame) == "search_year"
    columns = app.source_table_columns("HEALTH_CANADA_MDI", frame)
    assert "date_received" in columns and "search_year" in columns
    assert "year" not in columns


def test_notice_dates_keep_filtering_semantics_without_fabricating_events():
    frame = clean_records(pd.DataFrame([
        row("SWISSMEDIC_FSCA", "A", event_date="2025-01-02"),  # legacy normalized row
        row("MHRA_FSCA", "B", event_date="", search_date="January 2025", search_date_basis="manufacturer_notice_date"),
        row("BFARM_RECALLS", "C", event_date="", search_date="2025-01-03", search_date_basis="listing_text_date"),
    ]))
    assert frame.event_date.tolist() == ["", "", ""]
    assert frame.year.isna().all()
    assert frame.search_date.tolist() == ["2025-01-02", "January 2025", "2025-01-03"]
    assert frame.search_date_basis.tolist() == ["publication_date", "manufacturer_notice_date", "listing_text_date"]
    assert set(filter_records(frame, ["trocar"], [2025], "2025-01-01", "2025-01-31").event_id) == {"A", "B", "C"}


def test_explicit_occurrence_does_not_change_notice_filter_date():
    frame = clean_records(pd.DataFrame([row("SWISSMEDIC_FSCA", event_date="", date_of_event="2024-12-31", report_date="2025-01-03")]))
    assert frame.iloc[0].event_date == "2024-12-31"
    assert frame.iloc[0].search_date == "2025-01-03"
    assert frame.iloc[0].search_date_basis == "publication_date"


def test_keyword_matching_includes_generic_names_and_manufacturers_without_substring_noise():
    frame = clean_records(pd.DataFrame([row(product_name="Model 123", narrative_text="", generic_name="Trocar"), row(event_id="2", product_name="Model 456", narrative_text="", manufacturer="Acme")]))
    assert len(filter_records(frame, ["trocar"], [2025])) == 1
    assert len(filter_records(frame, ["acme"], [2025])) == 1
    assert not contains_any_keyword("report", ["port"])
    assert not contains_any_keyword("anything", [""])
    assert contains_any_keyword("a LINa   Port device", ["Lina Port"])
    assert contains_any_keyword("穿刺器故障", ["穿刺器"])


def test_string_false_cannot_bypass_keyword_filter():
    frame = clean_records(pd.DataFrame([row(source_query_match="False")]))
    assert filter_records(frame, ["unrelated"], [2025]).empty
    frame["source_query_match"] = True
    assert len(filter_records(frame, ["unrelated"], [2025])) == 1


def test_date_filter_keeps_empty_record_schema():
    frame = clean_records(pd.DataFrame([row("FDA_MAUDE", date_received="2024-02-01", source_query_match=True)]))
    result = filter_records(frame, ["trocar"], [2024], "2024-04-01", "2024-05-31")
    assert result.empty
    assert {"source", "event_id", "classification_text", "keyword_hit"} <= set(result.columns)


def test_all_failed_partial_and_unverified_progress_are_not_completed():
    reporter = ProgressReporter(["A", "B"])
    reporter.fail_source("A", "Unavailable")
    reporter.fail_source("B", "Unavailable")
    assert reporter.get_snapshot().overall_status == "failed"
    reporter.complete_source("A", completeness="complete")
    assert reporter.get_snapshot().overall_status == "partial"
    reporter.complete_source("B", completeness="unverified")
    assert reporter.get_snapshot().overall_status == "unverified"
    json.dumps(reporter.to_dict())


def test_pipeline_keeps_partial_records_and_isolates_an_unexpected_source_error(monkeypatch):
    from pipeline import pipeline as module
    class Partial:
        def fetch(self, **kwargs):
            return SourceFetchResult("A", ensure_unified_columns(pd.DataFrame([row()])), False, "Second page unavailable", completeness="partial", total_available=2)
    class Broken:
        def __init__(self):
            raise RuntimeError("unexpected setup issue")
    monkeypatch.setattr(module, "SOURCE_REGISTRY", {"A": Partial, "B": Broken})
    result = run_pipeline(["trocar"], [2025], selected_sources=["A", "B"], export_results=False)
    assert len(result["filtered"]) == 1
    assert result["source_status"].set_index("source_name").loc["A", "completeness"] == "partial"
    assert result["source_status"].set_index("source_name").loc["B", "completeness"] == "failed"
    assert result["progress"].overall_status == "partial"


def test_pipeline_preserves_options_and_per_source_limits(monkeypatch, tmp_path):
    from pipeline import pipeline as module
    received = []
    class Good:
        def fetch(self, **kwargs):
            received.append(kwargs)
            return SourceFetchResult("A", ensure_unified_columns(pd.DataFrame()), True, completeness="complete")
    monkeypatch.setattr(module, "SOURCE_REGISTRY", {"A": Good})
    options = {"_global": {"request_timeout": 20, "max_pages": 10, "max_records": 500}, "A": {"request_timeout": 8, "max_records": 1000, "csv_path": "excluded.csv"}}
    original = deepcopy(options)
    result = run_pipeline(["trocar"], [2025], selected_sources=["A"], source_options=options, output_dir=tmp_path)
    assert options == original
    assert received[0]["request_timeout"] == 8
    assert received[0]["max_pages"] == 10
    assert received[0]["max_records"] == 1000
    assert result["progress"].overall_status == "completed"
    assert result["source_status"].iloc[0].completeness == "complete"
    manifest = json.loads(result["export_files"]["manifest"].read_text())
    assert manifest["query"]["source_limits"] == {"A": {"request_timeout": 8, "max_pages": 10, "max_records": 1000}}


def test_pipeline_uses_device_identity_for_retrieval_and_retains_classification_terms(monkeypatch):
    from pipeline import pipeline as module
    class Good:
        def fetch(self, **kwargs):
            assert kwargs["keywords"] == ["catheter"]
            assert kwargs["components"] is None
            assert kwargs["accident_terms"] is None
            frame = ensure_unified_columns(pd.DataFrame([row(product_name="Catheter", narrative_text="Catheter hub cracked.")]))
            return SourceFetchResult("A", frame, True, completeness="complete", retrieved_count=6)
    monkeypatch.setattr(module, "SOURCE_REGISTRY", {"A": Good})
    result = run_pipeline(["catheter"], [2025], selected_sources=["A"], components=["hub"],
                          accident_terms=["cracked"], classifier_profile="Custom", export_results=False)
    assert result["filtered"].iloc[0].category == "Hub - Cracked issue"
    assert result["source_status"].iloc[0].retrieved_count == 6
    assert result["source_status"].iloc[0].returned_count == 1


@pytest.mark.parametrize("kwargs", [{"keywords": []}, {"selected_sources": []}, {"start_date": "2025-05-01", "end_date": "2025-04-01"}, {"start_date": "20251332"}])
def test_pipeline_rejects_invalid_searches_before_network(kwargs):
    arguments = dict(keywords=["trocar"], years=[2025], export_results=False)
    arguments.update(kwargs)
    with pytest.raises(ValueError):
        run_pipeline(**arguments)


def test_exports_are_run_isolated_and_include_status_evidence_and_complete_raw_text(tmp_path):
    long = "x" * 40000 + "\x01final evidence"
    frame = pd.DataFrame({"event_id": ["1", "2"], "narrative_text": ["=1+1", long]})
    status = pd.DataFrame([{"source_name": "A", "completeness": "partial", "retrieved_count": 2}])
    results = {"all_raw": frame, "filtered": frame, "source_status": status, "source_errors": pd.DataFrame([{"source_name": "A", "error_message": "Page unavailable"}])}
    first = export_pipeline_results(results, tmp_path, file_prefix="trocar")
    second = export_pipeline_results(results, tmp_path, file_prefix="trocar")
    assert first["excel"].parent != second["excel"].parent
    workbook = load_workbook(first["excel"])
    assert {"source_status", "source_warnings", "source_errors"} <= set(workbook.sheetnames)
    assert workbook["all_raw"]["B2"].data_type == "s"
    assert "TRUNCATED" in workbook["all_raw"]["B3"].value
    assert json.loads(first["raw_jsonl"].read_text().splitlines()[1])["narrative_text"] == long
    manifest = json.loads(first["manifest"].read_text())
    assert manifest["source_status"][0]["completeness"] == "partial"
    for item in manifest["files"].values():
        assert hashlib.sha256((first["manifest"].parent / item["name"]).read_bytes()).hexdigest() == item["sha256"]
    assert "'=1+1" in first["all_raw"].read_text(encoding="utf-8-sig")


def test_export_prefix_cannot_escape_output_directory(tmp_path):
    with pytest.raises(ValueError):
        export_pipeline_results({}, tmp_path, "../../escape")
    assert not list(tmp_path.iterdir())


def test_fda_detail_selector_keeps_model_and_product_code_matches(monkeypatch):
    import app
    frame = pd.DataFrame({"event_id": ["1", "2", "3"], "fda_match_category": ["Model number matches", "Product code matches", "Narrative matches"]})
    def choose(label, options, **kwargs):
        assert "Model number matches" in options
        assert "Product code matches" in options
        return "Model number matches"
    monkeypatch.setattr(app.st, "selectbox", choose)
    assert app.filter_fda_detail_category(frame).event_id.tolist() == ["1"]
