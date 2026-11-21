"""Regression tests for evidence loss and false successful source searches."""
from __future__ import annotations

import json
from datetime import date
from unittest.mock import Mock

import pandas as pd
import pytest
import requests

from pipeline.source_support import request_with_retry, with_coverage
from pipeline.sources.base import SourceFetchResult
from pipeline.sources.openfda_maude import OpenFdaMaudeConnector
from pipeline.sources_openfda import fetch_fda_maude_openfda
from pipeline.sources.health_canada_mdi import (
    HealthCanadaMdiClientConfig, fetch_health_canada_mdi_direct, parse_health_canada_mdi_response,
)
from pipeline.sources.swissmedic import fetch_swissmedic_fsca_direct, parse_swissmedic_response
from pipeline.sources.mhra_fsca import fetch_mhra_fsca_direct
from pipeline.sources.bfarm import fetch_bfarm_recalls_direct, _bfarm_search_terms
from pipeline.sources import tga_daen as tga


def response(payload, status=200, content_type="application/json"):
    item = requests.Response()
    item.status_code = status
    item.url = "https://example.test/source"
    item.headers["Content-Type"] = content_type
    item._content = (json.dumps(payload) if isinstance(payload, (dict, list)) else str(payload)).encode()
    return item


class Session:
    def __init__(self, items):
        self.items = iter(items)
        self.calls = []

    def get(self, url, **kwargs):
        return self._call(url, kwargs)

    def post(self, url, **kwargs):
        return self._call(url, kwargs)

    def _call(self, url, kwargs):
        self.calls.append((url, kwargs))
        item = next(self.items)
        if isinstance(item, Exception):
            raise item
        item.url = url
        return item


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr("pipeline.source_support.time.sleep", lambda _: None)


def test_coverage_survives_legacy_result_contract():
    df = with_coverage(pd.DataFrame(), "failed", ["upstream HTTP 500"])
    result = SourceFetchResult("FDA_MAUDE", df, success=True)
    assert not result.success
    assert result.completeness == "failed"
    assert result.warnings == ["upstream HTTP 500"]


def test_transient_http_is_retried_but_cooldown_is_bounded():
    session = Session([response({}, 503), response({"data": []})])
    assert request_with_retry(session, "GET", "https://example.test").status_code == 200
    assert len(session.calls) == 2
    blocked = response({}, 429)
    blocked.headers["Retry-After"] = "300"
    with pytest.raises(RuntimeError, match="retry budget"):
        request_with_retry(Session([blocked]), "GET", "https://example.test")


def test_fda_http_failure_is_not_successful_zero(monkeypatch):
    monkeypatch.setattr("pipeline.sources_openfda.requests.get", lambda *a, **kw: response({}, 500))
    result = OpenFdaMaudeConnector().fetch(["trocar"], [2024])
    assert not result.success
    assert result.completeness == "failed"
    assert result.records.empty
    assert len(result.warnings) == 5


def test_fda_preserves_partial_rows_and_original_evidence(monkeypatch):
    raw = {"mdr_report_key": "A", "event_type": "Injury", "date_received": "20240201",
           "manufacturer_d_name": "Example", "device": [{"brand_name": "Brand", "generic_name": "Trocar"}]}
    def get(*args, **kwargs):
        if kwargs["params"]["search"].startswith("device.brand_name"):
            return response({"results": [raw], "meta": {"results": {"total": 1}}})
        return response({}, 503)
    monkeypatch.setattr("pipeline.sources_openfda.requests.get", get)
    result = OpenFdaMaudeConnector().fetch(["trocar"], [2024])
    assert result.completeness == "partial"
    assert len(result.records) == 1
    row = result.records.iloc[0]
    assert row["brand_names"] == "Brand"
    assert row["generic_name"] == "Trocar"
    assert row["event_type"] == "Injury"
    assert json.loads(row["raw_record"]) == raw


def test_fda_caps_are_explicit_and_dates_intersect_years(monkeypatch):
    calls = []
    def get(*args, **kwargs):
        calls.append(kwargs["params"])
        return response({"results": [{"mdr_report_key": "A", "device": []}], "meta": {"results": {"total": 8}}})
    monkeypatch.setattr("pipeline.sources_openfda.requests.get", get)
    df = fetch_fda_maude_openfda(['A"B'], [2023, 2024, 2025], start_date="2024-02-01",
                               end_date="2024-03-01", max_skip_per_query=1, limit_per_query=1)
    assert df.attrs["completeness"] == "partial"
    assert len(calls) == 5
    assert all("date_received:[20240201 TO 20240301]" in q["search"] for q in calls)
    assert '\\"' in calls[0]["search"]
    assert any("capped" in text for text in df.attrs["warnings"])


def test_fda_only_documented_no_matches_is_verified_empty(monkeypatch):
    monkeypatch.setattr("pipeline.sources_openfda.requests.get", lambda *a, **kw: response({"error": {"code": "NOT_FOUND", "message": "No matches found!"}}, 404))
    result = OpenFdaMaudeConnector().fetch(["rare-device"], [2024])
    assert result.success
    assert result.completeness == "complete"
    assert result.records.empty


@pytest.mark.parametrize("payload", [{"error": "maintenance"}, {}, {"data": "bad"}])
def test_canada_rejects_error_or_changed_schema(payload):
    with pytest.raises(RuntimeError):
        parse_health_canada_mdi_response(response(payload))


def test_canada_preserves_prior_pages_after_failure():
    session = Session([response({"recordsFiltered": 2, "data": [{"incident": {"incident_id": "A", "receipt_date": "2024-05-01"}}]}),
                       requests.Timeout(), requests.Timeout(), requests.Timeout()])
    df, warnings = fetch_health_canada_mdi_direct(["trocar"], [2024], session=session,
                                                 client_config=HealthCanadaMdiClientConfig(page_size=1))
    assert len(df) == 1
    assert df.attrs["completeness"] == "partial"
    assert any("failed" in warning for warning in warnings)


def test_canada_repeated_page_terminates_and_missing_id_is_retained():
    payload = {"recordsFiltered": 4, "data": [{"incident": {"receipt_date": "2024-05-01", "trade_name": ["Trocar"]}}]}
    session = Session([response(payload), response(payload)])
    df, warnings = fetch_health_canada_mdi_direct(["trocar"], [2024], session=session,
                                                 client_config=HealthCanadaMdiClientConfig(page_size=1))
    assert len(df) == 1
    assert len(session.calls) == 2
    assert df.attrs["completeness"] == "partial"
    assert any("no incident ID" in warning for warning in warnings)


def swiss_page(ids, page, total, last):
    return response({"content": [{"swissmedicRef": i, "publikationsDatum": "2024-04-01", "devices": [], "documents": []} for i in ids],
                     "number": page, "totalElements": total, "totalPages": 1 if total == 1 else 2, "last": last})


def test_swiss_overlapping_first_page_does_not_skip_later_unique_records():
    session = Session([swiss_page(["A"], 0, 1, True), swiss_page(["A"], 0, 2, False), swiss_page(["B"], 1, 2, True)])
    df, _ = fetch_swissmedic_fsca_direct(["trocar", "Brand"], [2024], session=session)
    assert set(df.event_id) == {"A", "B"}
    assert len(session.calls) == 3
    assert df.attrs["completeness"] == "complete"


@pytest.mark.parametrize("payload", [{"error": "service unavailable"}, {}, {"content": [] , "last": "false"}])
def test_swiss_rejects_false_empty_payloads(payload):
    with pytest.raises(RuntimeError):
        parse_swissmedic_response(payload)


def test_swiss_keeps_first_page_when_later_page_fails():
    session = Session([swiss_page(["A"], 0, 2, False), response({}, 503), response({}, 503), response({}, 503)])
    df, _ = fetch_swissmedic_fsca_direct(["trocar"], [2024], session=session)
    assert len(df) == 1
    assert df.attrs["completeness"] == "partial"


def test_mhra_identical_finder_pages_are_reprocessed_for_each_keyword():
    listing = '<li class="gem-c-document-list__item"><a href="/drug-device-alerts/week">Weekly notices</a>Issued: 1 May 2024</li>'
    detail = '<h1>Week</h1><div class="gem-c-govspeak"><h2>Brand: Trocar</h2><p>Recall</p><h2>Brand: Kii</h2><p>Recall</p></div>'
    session = Session([response(listing, content_type="text/html"), response(detail, content_type="text/html"),
                       response(listing, content_type="text/html")])
    df, _ = fetch_mhra_fsca_direct(["trocar", "Kii"], [2024], session=session)
    assert len(df) == 2
    assert set(df.product_name) == {"Trocar", "Kii"}
    assert len(session.calls) == 3  # Cached detail reused, but filtered per term.
    assert df.attrs["completeness"] == "unverified"


def bfarm_card(ref, title="Trocar"):
    return f'<section id="results"><li class="l-teaser-list__item"><a href="/SharedDocs/Kundeninfos/EN/{ref}.pdf">{title}</a><span class="c-icon-teaser__date">Date: 2024-05-01</span><span class="c-icon-teaser__topic">Topics: Medical devices</span><span class="c-icon-teaser__category">Type: Customer information</span><p>Reference {ref}/24</p></li></section>'


def test_bfarm_retains_all_terms_and_follows_next_page():
    assert _bfarm_search_terms(["trocar", "VersaOne", "injury"]) == ["Trocar", "VersaOne", "injury"]
    facet = '<a href="?dateOfIssue_dt=penultimateyear">2024 (2)</a>'
    page1 = bfarm_card("12345") + '<a rel="next" href="?page=2">Next</a>'
    page2 = bfarm_card("12346")
    page3 = bfarm_card("12347", "VersaOne")
    session = Session([response(facet, content_type="text/html"), response(page1, content_type="text/html"),
                       response(page2, content_type="text/html"), response(facet, content_type="text/html"),
                       response(page3, content_type="text/html")])
    df, _ = fetch_bfarm_recalls_direct(["trocar", "VersaOne"], [2024], session=session)
    assert len(df) == 3
    assert session.calls[3][1]["params"]["templateQueryString"] == "VersaOne"
    assert df.attrs["completeness"] == "unverified"


def test_bfarm_page_cap_is_partial():
    facet = '<a href="?dateOfIssue_dt=penultimateyear">2024 (2)</a>'
    page = bfarm_card("12345") + '<a rel="next" href="?page=2">Next</a>'
    session = Session([response(facet, content_type="text/html"), response(page, content_type="text/html")])
    df, _ = fetch_bfarm_recalls_direct(["trocar"], [2024], session=session, max_pages=1)
    assert len(df) == 1
    assert len(session.calls) == 2
    assert df.attrs["completeness"] == "partial"


@pytest.mark.parametrize("payload,status", [("Service unavailable", 500), ("<html>challenge</html>", 200), ({"error": "bad"}, 200)])
def test_tga_device_failure_never_becomes_zero_matches(payload, status):
    with pytest.raises(RuntimeError):
        tga.parse_tga_device_response(response(payload, status))


def test_tga_expected_reports_but_no_rows_is_failure(monkeypatch):
    monkeypatch.setattr(tga, "_initialize_tga_session", lambda *a, **kw: None)
    monkeypatch.setattr(tga, "search_tga_devices", lambda *a, **kw: {"records": [{"HashKey": "0#1"}], "selected_device_count": 1})
    monkeypatch.setattr(tga, "submit_tga_daen_search", lambda *a, **kw: response("<html></html>", content_type="text/html"))
    monkeypatch.setattr(tga, "parse_tga_search_result_counts", lambda *a: {"expected_report_count": 4, "selected_devices_count": 1})
    monkeypatch.setattr(tga, "fetch_tga_print_report", lambda *a, **kw: (b"<html></html>", "text/html", "https://example.test/report"))
    monkeypatch.setattr(tga, "_fetch_tga_report_rows", lambda *a, **kw: ([], ""))
    with pytest.raises(RuntimeError, match="not a verified empty"):
        tga.fetch_tga_daen_direct(["trocar"], [2024], start_date="2024-01-01", end_date="2024-12-31")


@pytest.mark.parametrize("no_data_callout,echoed_end,verified", [(True, "31/12/2024", True), (False, "31/12/2024", False), (True, "31/12/2023", False)])
def test_tga_zero_requires_source_empty_state_and_requested_query_echo(monkeypatch, no_data_callout, echoed_end, verified):
    html = f'<p>5 medical devices selected between 01/01/2024 - {echoed_end}.</p><div id="report-totals">Number of reports: <strong>0</strong></div>'
    if no_data_callout:
        html += '<p class="callout keywords">No data was returned for your search parameters.</p>'
    monkeypatch.setattr(tga, "_initialize_tga_session", lambda *a, **kw: None)
    monkeypatch.setattr(tga, "search_tga_devices", lambda *a, **kw: {"records": [{"HashKey": f"0#{i}"} for i in range(5)], "selected_device_count": 5})
    monkeypatch.setattr(tga, "submit_tga_daen_search", lambda *a, **kw: response(html, content_type="text/html"))
    monkeypatch.setattr(tga, "fetch_tga_print_report", lambda *a, **kw: (html.encode(), "text/html", "https://example.test/report"))
    if not verified:
        with pytest.raises(RuntimeError, match="not a verified empty result"):
            tga.fetch_tga_daen_direct(["Versaport"], [2024], start_date="2024-01-01", end_date="2024-12-31")
        return
    result = tga.fetch_tga_daen_direct(["Versaport"], [2024], start_date="2024-01-01", end_date="2024-12-31")
    assert result["records"].empty
    assert result["records"].attrs["completeness"] == "complete"
    assert result["records"].attrs["total_available"] == 0
    assert result["metadata"]["zero_reports_confirmed"] is True
    assert not any("incomplete" in warning.lower() for warning in result["warnings"])


def test_tga_script_or_template_zero_is_not_result_evidence():
    placeholder = '5 medical devices selected between 01/01/2024 - 31/12/2024. Number of reports: 0. No data was returned for your search parameters.'
    parsed = tga.parse_tga_search_result_counts(f'<html><script>var message="{placeholder}";</script><template>{placeholder}</template><p>Loading...</p></html>')
    assert parsed["expected_report_count"] is None
    assert parsed["zero_reports_confirmed"] is False


def test_tga_keeps_rows_after_pagination_failure(monkeypatch):
    monkeypatch.setattr(tga, "parse_tga_report_summary_html", lambda *a: [{"report_number": "A", "date": "2024-01-01"}])
    monkeypatch.setattr(tga, "_has_tga_paging_control", lambda *a: True)
    monkeypatch.setattr(tga, "_extract_tga_page_status", lambda *a: (1, 3))
    monkeypatch.setattr(tga, "_advance_tga_report_page", Mock(side_effect=RuntimeError("HTTP 503")))
    warnings = []
    rows, _ = tga._fetch_tga_report_rows(Session([]), b"<html>page 1</html>", "text/html", "https://example.test", tga.TgaDaenClientConfig(), warnings=warnings)
    assert len(rows) == 1
    assert any("incomplete" in warning for warning in warnings)


def test_tga_explicit_occurrence_is_preserved_but_search_filters_report_date():
    raw = tga._normalize_report_row({"Report #": "A", "Date": "05/03/2024", "Date of Event": "20/12/2023", "Trade Name": "Trocar"})
    normalized = tga.normalize_tga_daen_report(raw, {})
    assert normalized["event_date"] == "2023-12-20"
    assert normalized["report_date"] == normalized["date_received"] == "2024-03-05"
    assert normalized["year"] == 2024
    assert json.loads(normalized["source_specific"])["date_basis"] == "report_date"
    df = pd.DataFrame([normalized])
    assert len(tga._filter_by_date_range(df, "2024-03-01", "2024-03-31")) == 1
    assert tga._filter_by_date_range(df, "2023-01-01", "2023-12-31").empty


def test_tga_notification_date_does_not_fabricate_an_occurrence_date():
    row = tga.normalize_tga_daen_record({"report_id": "A", "notification_date": "20240305", "product": "Trocar"})
    assert row["event_date"] == row["date_of_event"] == ""
    assert row["report_date"] == row["date_received"] == "2024-03-05"
    assert row["year"] == row["received_year"] == 2024
    assert json.loads(row["source_specific"])["occurrence_date_basis"] == "unknown"


@pytest.mark.parametrize("today,expected", [(date(2026, 11, 1), "2026-07-01"), (date(2026, 11, 5), "2026-08-05"), (date(2026, 11, 21), "2026-08-05")])
def test_tga_cutoff_matches_current_portal_rule(today, expected):
    assert tga._daen_latest_allowed_date(today) == expected


def test_tga_lag_capped_source_preserves_effective_date_and_partial_status(monkeypatch):
    monkeypatch.setattr(tga, "_daen_latest_allowed_date", lambda: "2026-08-05")
    direct = Mock(return_value={"records": with_coverage(pd.DataFrame(), "complete", []), "warnings": []})
    monkeypatch.setattr(tga, "fetch_tga_daen_direct", direct)
    result = tga.TgaDaenConnector().fetch(["trocar"], [2026], start_date="2026-01-01", end_date="2026-11-21")
    assert direct.call_args.kwargs["end_date"] == "2026-08-05"
    assert result.records.attrs["effective_end"] == "2026-08-05"
    assert result.records.attrs["date_basis"] == "report_date"
    assert result.completeness == "partial"
    assert any("2026-08-05" in warning and "2026-11-21" in warning for warning in result.warnings)


def test_tga_empty_device_search_keeps_effective_date_metadata(monkeypatch):
    monkeypatch.setattr(tga, "_initialize_tga_session", lambda *a, **kw: None)
    monkeypatch.setattr(tga, "search_tga_devices", lambda *a, **kw: {"records": [], "selected_device_count": 0})
    result = tga.fetch_tga_daen_direct(["trocar"], [2024], start_date="2024-01-01", end_date="2024-12-31")
    assert result["metadata"]["effective_end"] == result["records"].attrs["effective_end"] == "2024-12-31"
    assert result["metadata"]["date_basis"] == "report_date"
    assert result["records"].attrs["completeness"] == "unverified"


def test_fda_source_record_budget_limits_all_queries(monkeypatch):
    calls = []
    def get(*a, **kwargs):
        calls.append(kwargs["params"])
        return response({"results": [{"mdr_report_key": "A", "device": []}], "meta": {"results": {"total": 5}}})
    monkeypatch.setattr("pipeline.sources_openfda.requests.get", get)
    result = OpenFdaMaudeConnector().fetch(["trocar", "brand"], [2024], max_records=1)
    assert len(calls) == 1
    assert len(result.records) == 1
    assert result.completeness == "partial"


def test_fda_receipt_date_does_not_fabricate_an_event_date(monkeypatch):
    raw = {"mdr_report_key": "A", "date_received": "20240401", "device": []}
    monkeypatch.setattr("pipeline.sources_openfda.requests.get", lambda *a, **kw: response({"results": [raw], "meta": {"results": {"total": 1}}}))
    result = OpenFdaMaudeConnector().fetch(["trocar"], [2024])
    assert result.records.iloc[0]["event_date"] == ""
    assert result.records.iloc[0]["report_date"] == "2024-04-01"


def test_fda_model_and_product_code_are_separate_search_fields(monkeypatch):
    fields = []
    def get(*a, **kwargs):
        fields.append(kwargs["params"]["search"].split(":", 1)[0])
        return response({"error": {"code": "NOT_FOUND", "message": "No matches found!"}}, 404)
    monkeypatch.setattr("pipeline.sources_openfda.requests.get", get)
    OpenFdaMaudeConnector().fetch(["OMS-T10SB"], [2024])
    assert "device.model_number" in fields
    assert "device.device_report_product_code" in fields


def test_fda_malformed_later_page_preserves_prior_rows(monkeypatch):
    def get(*a, **kwargs):
        params = kwargs["params"]
        if not params["search"].startswith("device.brand_name"):
            return response({"error": {"code": "NOT_FOUND", "message": "No matches found!"}}, 404)
        if params["skip"] == 0:
            return response({"results": [{"mdr_report_key": "A", "device": []}], "meta": {"results": {"total": 2}}})
        return response({"results": [{"mdr_report_key": "B"}], "meta": None})
    monkeypatch.setattr("pipeline.sources_openfda.requests.get", get)
    result = OpenFdaMaudeConnector().fetch(["trocar"], [2024], limit_per_query=1)
    assert len(result.records) == 1
    assert result.completeness == "partial"


@pytest.mark.parametrize("source", ["fda", "canada", "swiss"])
def test_inconsistent_totals_cannot_produce_complete(source, monkeypatch):
    if source == "fda":
        monkeypatch.setattr("pipeline.sources_openfda.requests.get", lambda *a, **kw: response({"results": [{"mdr_report_key": "A"}], "meta": {"results": {"total": 0}}}))
        result = OpenFdaMaudeConnector().fetch(["trocar"], [2024])
        assert result.completeness == "failed"
    elif source == "canada":
        df, _ = fetch_health_canada_mdi_direct(["trocar"], [2024], session=Session([response({"recordsFiltered": 0, "data": [{"incident": {"incident_id": "A"}}]})]))
        assert df.attrs["completeness"] == "failed"
    else:
        with pytest.raises(RuntimeError):
            parse_swissmedic_response({"content": [{"swissmedicRef": "A"}], "totalElements": 0, "totalPages": 1, "number": 0, "last": True})


def test_swiss_repeated_records_with_different_page_metadata_are_not_complete():
    session = Session([swiss_page(["A"], 0, 2, False), swiss_page(["A"], 1, 2, True)])
    df, warnings = fetch_swissmedic_fsca_direct(["trocar"], [2024], session=session)
    assert len(df) == 1
    assert df.attrs["completeness"] == "partial"
    assert any("Repeated" in warning for warning in warnings)


def test_canada_null_ids_remain_distinct_records():
    payload = {"recordsFiltered": 2, "data": [{"incident": {"incident_id": None, "receipt_date": "2024-04-01", "trade_name": [term]}} for term in ["Brand A", "Brand B"]]}
    df, _ = fetch_health_canada_mdi_direct(["trocar"], [2024], session=Session([response(payload)]))
    assert len(df) == 2
    assert len(set(df.event_id)) == 2
    assert all(value.startswith("content:") for value in df.event_id)


def test_tga_pdf_respects_page_budget(monkeypatch):
    class Page:
        def extract_text(self):
            return "report text"
    monkeypatch.setattr(tga, "PdfReader", lambda _: type("Reader", (), {"pages": [Page(), Page(), Page()]})())
    warnings = []
    text = tga.extract_text_from_tga_pdf(b"%PDF-test", max_pages=1, warnings=warnings)
    assert text == "report text"
    assert any("incomplete" in warning for warning in warnings)
