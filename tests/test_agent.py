"""Regression checks for the replacement agent's data boundaries and outputs."""

import hashlib
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pandas as pd
import pytest

from cloud_agent.collector import collect, _fetch_subprocess, redact
from cloud_agent.evidence import add_assessment, add_supplements, build_evidence, official_url, verify_report, write_report
from cloud_agent.schema import SearchRequest
from cloud_agent.integrity import verify_downloads


def request(**changes):
    return SearchRequest.from_dict({"keywords": ["trocar"], "start_date": "2024-01-01", "end_date": "2024-12-31", "sources": ["FDA_MAUDE", "TGA_DAEN"], **changes})


def record(source="FDA_MAUDE", identifier="1", **changes):
    return {"source": source, "event_id": identifier, "report_number": identifier,
            "event_date": "2024-05-12", "report_date": "2024-06-12", "search_date": "2024-05-12",
            "search_year": 2024, "year": 2024, "product_name": "Trocar", "manufacturer": "Fixture manufacturer",
            "combined_text": "trocar component fractured during use", "category": "Component fracture",
            "narrative_text": "The trocar component fractured during use. No patient injury was reported.",
            "raw_record": {"id": identifier, "text": "The trocar component fractured during use."},
            "event_link": "https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfmaude/detail.cfm?mdrfoi__id=1",
            **changes}


def source_result(source, **changes):
    rows = [record(source)]
    return {"all_raw": rows, "filtered": rows, "source_errors": [], "source_warnings": [],
            "source_status": [{"source_name": source, "success": True, "completeness": "complete", "retrieved_count": 1}], **changes}


@pytest.mark.parametrize("changes", [
    {"keywords": []}, {"keywords": ["death", "injury"]}, {"keywords": ["trocar", "injury"]}, {"keywords": "trocar"},
    {"sources": []}, {"sources": ["FAKE_REGULATOR"]}, {"max_pages": True}, {"max_records": -1},
    {"request_timeout": 0}, {"source_timeout": 2}, {"start_date": "2024-02-31"},
    {"start_date": "2025-01-01", "end_date": "2024-01-01"}, {"end_date": "2099-01-01"},
    {"base_url": "http://localhost/secrets"}, {"classifier_profile": "unknown"},
])
def test_invalid_search_requests_are_rejected(changes):
    with pytest.raises(ValueError):
        request(**changes)


def test_request_preserves_device_terms_and_separates_outcomes():
    query = request(keywords=[" trocar ", "trocar", "Versaport"], accident_terms=["injury"])
    assert query.keywords == ("trocar", "Versaport")
    assert query.accident_terms == ("injury",)
    assert query.classifier_profile == "Trocar"
    assert query.years == [2024]


def test_collector_does_not_erase_success_when_one_source_raises():
    def fetch(query, source):
        if source == "TGA_DAEN":
            raise RuntimeError("HTTP 500")
        return source_result(source)
    result = collect(request(), fetcher=fetch)
    assert len(result["filtered"]) == 1
    status = result["source_status"].set_index("source_name")
    assert status.loc["TGA_DAEN", "completeness"] == "failed"
    assert status.loc["FDA_MAUDE", "completeness"] == "complete"
    assert len(result["source_errors"]) == 1


def test_collector_keeps_cross_regulator_ids_separate():
    result = collect(request(), fetcher=lambda query, source: source_result(source))
    assert len(result["filtered"]) == 2
    assert result["summary_year_category"]["incident_count"].sum() == 2


def test_collector_marks_export_caps_partial():
    def fetch(query, source):
        rows = [record(source, str(i)) for i in range(4)]
        return source_result(source, all_raw=rows, filtered=rows)
    result = collect(request(max_records=2), fetcher=fetch)
    assert len(result["filtered"]) == 4
    assert set(result["source_status"]["completeness"]) == {"partial"}
    assert len(result["source_warnings"]) >= 2


def test_timeout_becomes_unknown_coverage_not_empty_success(monkeypatch):
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired("worker", 120)
    monkeypatch.setattr(subprocess, "run", timeout)
    result = _fetch_subprocess(request(), "FDA_MAUDE")
    assert result["source_status"][0]["success"] is False
    assert "not zero" in result["source_errors"][0]["error_message"]


@pytest.mark.parametrize("url", [
    "javascript:alert(1)", "https://www.fda.gov.evil.test/report", "http://127.0.0.1/",
    "https://secret@www.fda.gov/report", "https://www.fda.gov:8080/report", "https://www.fda.gov/\nreport",
])
def test_evidence_url_must_be_on_correct_official_host(url):
    assert official_url(url, "FDA_MAUDE") is None


def test_portal_is_not_promoted_to_record_citation():
    frame = pd.DataFrame([record(event_link="https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfmaude/search.cfm")])
    row = build_evidence(frame, "2026-11-21T00:00:00+00:00")[0]
    assert row["direct_record_url"] is None
    assert row["source_portal"].startswith("https://")
    assert len(row["raw_sha256"]) == 64


@pytest.mark.parametrize("source, listing, attachment", [
    ("MHRA_FSCA", "https://www.gov.uk/drug-device-alerts/field-safety-notices-fsns-from-1-to-5-january-2024",
     "https://assets.publishing.service.gov.uk/media/abc/device-notice.pdf"),
    ("BFARM_RECALLS", "https://www.bfarm.de/SiteGlobals/Forms/Suche/EN/Expertensuche_Formular.html?query=trocar",
     "https://www.bfarm.de/SharedDocs/Kundeninfos/EN/01/2024/device-notice.pdf"),
])
def test_listing_retained_as_locator_while_specific_document_is_citable(source, listing, attachment):
    frame = pd.DataFrame([record(source, event_link=listing, raw_link=listing)])
    evidence = build_evidence(frame, "2026-11-21T00:00:00+00:00")[0]
    assert evidence["direct_record_url"] is None
    assert evidence["source_locator_url"] == listing
    frame["event_link"] = attachment
    evidence = build_evidence(frame, "2026-11-21T00:00:00+00:00")[0]
    assert evidence["direct_record_url"] == attachment
    assert evidence["source_locator_url"] == listing


def make_report(tmp_path):
    query = request(sources=["FDA_MAUDE"])
    result = collect(query, fetcher=lambda q, source: source_result(source))
    return write_report(query, result, tmp_path)


def test_report_is_unique_verified_and_contains_coverage(tmp_path):
    first, second = make_report(tmp_path), make_report(tmp_path)
    assert first != second
    assert verify_report(first)["evidence_records"] == 1
    assert json.loads((first / "overview.json").read_text())["status"] == "complete_for_requested_queries"
    assert list(first.rglob("*.xlsx"))
    assert "not unique patients" in (first / "report.md").read_text()


def test_tampering_with_raw_evidence_fails_integrity_check(tmp_path):
    folder = make_report(tmp_path)
    (folder / "evidence.json").write_text("[]")
    with pytest.raises(ValueError, match="integrity"):
        verify_report(folder)


def test_adding_untracked_artifact_fails_integrity_check(tmp_path):
    folder = make_report(tmp_path)
    (folder / "invented.csv").write_text("made up")
    with pytest.raises(ValueError, match="Artifact set"):
        verify_report(folder)


def test_assessment_rejects_fake_citations_and_fabricated_quotes(tmp_path):
    folder = make_report(tmp_path)
    evidence = json.loads((folder / "evidence.json").read_text())[0]
    for identifier, quote in [("FAKE", "The trocar component fractured during use."), (evidence["evidence_id"], "The report proves clinical causation.")]:
        with pytest.raises(ValueError):
            add_assessment(folder, {"claims": [{"statement": "A claim", "support": [{"evidence_id": identifier, "quote": quote}]}]})


def test_assessment_requires_real_excerpts_then_revalidates_artifacts(tmp_path):
    folder = make_report(tmp_path)
    evidence = json.loads((folder / "evidence.json").read_text())[0]
    quote = "The trocar component fractured during use. No patient injury was reported."
    path = add_assessment(folder, {"claims": [{"statement": "The report describes a component fracture.", "support": [{"evidence_id": evidence["evidence_id"], "quote": quote}]}], "limitations": ["Reporting is not proof of causation."], "next_steps": []})
    markdown = path.read_text()
    assert "Human review required" in markdown
    assert "Complete within the selected queries" in markdown
    assert "2024-01-01 through 2024-12-31" in markdown
    assert "| FDA MAUDE | complete | 1 |" in markdown
    assert f"Excerpt from `{evidence['evidence_id']}`\n  ~~~text\n  {quote}\n  ~~~" in markdown
    assert json.loads((folder / "manifest.json").read_text())["assessment_format_version"] == 2
    assert verify_report(folder, require_assessment=True)["verified_files"] > 5


def test_legacy_assessment_without_format_marker_still_verifies(tmp_path):
    from cloud_agent.evidence import _manifest

    folder = make_report(tmp_path)
    evidence = json.loads((folder / "evidence.json").read_text())[0]
    overview = json.loads((folder / "overview.json").read_text())
    statement = "The report describes a component fracture."
    assessment = {"claims": [{"statement": statement, "support": [
        {"evidence_id": evidence["evidence_id"], "quote": "The trocar component fractured during use."},
    ]}]}
    (folder / "assessment.json").write_text(json.dumps(assessment))
    (folder / "assessment.md").write_text(
        "# Agent assessment\n\nReferences and excerpts below were checked against this run. "
        "This check establishes provenance, not that the interpretation is correct.\n\n"
        f"- {statement} (`{evidence['evidence_id']}`)\n",
    )
    _manifest(folder, overview["run_id"], status=overview["status"])
    assert verify_report(folder, require_assessment=True)["evidence_records"] == 1


def test_all_failed_report_cannot_say_no_incidents(tmp_path):
    def failed(query, source):
        raise RuntimeError("unavailable")
    query = request()
    folder = write_report(query, collect(query, fetcher=failed), tmp_path)
    assert json.loads((folder / "overview.json").read_text())["status"] == "failed"
    assert "cannot answer whether matching reports exist" in (folder / "report.md").read_text()


def test_full_bearer_value_is_redacted():
    assert "secret_value" not in redact("Authorization: Bearer secret_value")
    assert "secret_value" not in redact("authorization=Basic secret_value")


def test_exported_count_is_reconciled_after_deduplication():
    def fetch(query, source):
        rows = [record(source), record(source)]
        return source_result(source, all_raw=rows, filtered=rows)
    result = collect(request(sources=["FDA_MAUDE"]), fetcher=fetch)
    assert len(result["filtered"]) == 1
    assert result["source_status"].iloc[0]["exported_count"] == 1


def test_original_duplicate_count_is_not_erased():
    def fetch(query, source):
        rows = [record(source, record_uid="source-unique-id", duplicate_count=4)]
        return source_result(source, all_raw=rows, filtered=rows)
    result = collect(request(sources=["FDA_MAUDE"]), fetcher=fetch)
    assert result["filtered"].iloc[0]["duplicate_count"] == 4


def test_swissmedic_official_pdf_is_a_direct_citation():
    row = record("SWISSMEDIC_FSCA", event_link="https://fsca.swissmedic.ch/mep/#/?q=trocar", device_link="https://fsca.swissmedic.ch/mep/api/publications/123/attachment.pdf")
    evidence = build_evidence(pd.DataFrame([row]), "2026-11-21T00:00:00+00:00")[0]
    assert evidence["direct_record_url"].endswith("attachment.pdf")


def test_retained_partial_evidence_is_not_labelled_total_failure(tmp_path):
    def partial(query, source):
        result = source_result(source)
        result["source_status"][0].update(success=False, completeness="partial")
        result["source_errors"] = [{"source_name": source, "error_message": "Later page unavailable"}]
        return result
    query = request(sources=["FDA_MAUDE"])
    folder = write_report(query, collect(query, fetcher=partial), tmp_path)
    assert json.loads((folder / "overview.json").read_text())["status"] == "incomplete"
    assert verify_report(folder)["evidence_records"] == 1


def test_cloud_download_gate_requires_actual_assessment(tmp_path):
    folder = make_report(tmp_path)
    with pytest.raises(ValueError, match="Required report files"):
        verify_downloads(list(folder.rglob("*")))
    add_assessment(folder, {"claims": [], "limitations": ["Fixture run only."], "next_steps": []})
    assert verify_downloads([p for p in folder.rglob("*") if p.is_file()])[0]["evidence_records"] == 1


def test_an_old_local_file_cannot_fill_a_gap_in_the_exact_turn_download(tmp_path):
    folder = make_report(tmp_path)
    add_assessment(folder, {"limitations": ["Fixture run only."]})
    files = [p for p in folder.rglob("*") if p.is_file() and p.name != "assessment.md"]
    with pytest.raises(ValueError, match="exact-turn download"):
        verify_downloads(files)


def test_rehashing_a_coverage_csv_cannot_hide_inconsistent_counts(tmp_path):
    folder = make_report(tmp_path)
    add_assessment(folder, {"limitations": ["Fixture run only."]})
    coverage_csv = next(folder.rglob("source_status.csv"))
    contents = pd.read_csv(coverage_csv)
    contents.loc[0, "exported_count"] = 7
    contents.to_csv(coverage_csv, index=False, encoding="utf-8-sig")
    digest = hashlib.sha256(coverage_csv.read_bytes()).hexdigest()

    nested_path = coverage_csv.parent / "manifest.json"
    nested = json.loads(nested_path.read_text())
    nested["files"]["source_status"]["sha256"] = digest
    nested_path.write_text(json.dumps(nested))
    outer_path = folder / "manifest.json"
    outer = json.loads(outer_path.read_text())
    for changed in (coverage_csv, nested_path):
        outer["files"][changed.relative_to(folder).as_posix()] = {
            "sha256": hashlib.sha256(changed.read_bytes()).hexdigest(), "bytes": changed.stat().st_size,
        }
    outer_path.write_text(json.dumps(outer))

    with pytest.raises(ValueError, match="CSV rows differ"):
        verify_report(folder, require_assessment=True)


def test_capped_public_acceptance_report_with_an_official_record_replay(tmp_path, monkeypatch):
    from cloud_agent.worker import run_source
    from pipeline import sources_openfda
    from scripts.verify_hosted_acceptance import check_public_report

    # Selected fields were transcribed from the FDA's public detail pages.
    # The adapter payload and total below are a controlled replay, not a live
    # openFDA page or an assertion about which reports it currently returns.
    brand = "ENDOPATH XCEL BLADELESS TROCAR"
    records = [
        {"mdr_report_key": "20155780", "report_number": "3005075853-2024-06605",
         "date_received": "20240906", "date_of_event": "20240810", "event_type": "Malfunction",
         "device": [{"brand_name": brand, "model_number": "B5LT"}],
         "mdr_text": [{"text": "failure of leakage of co2"}]},
        {"mdr_report_key": "20200955", "report_number": "3005075853-2024-06829",
         "date_received": "20240912", "date_of_event": "20240820", "event_type": "Malfunction",
         "device": [{"brand_name": brand, "model_number": "B12LT"}],
         "mdr_text": [{"text": "the device was broken"}]},
    ]
    calls = []

    def fetch_page(client, method, url, **kwargs):
        calls.append((method, url, kwargs["params"]))
        return SimpleNamespace(status_code=200, json=lambda: {"meta": {"results": {"total": 3}}, "results": records})

    monkeypatch.setattr(sources_openfda, "OPENFDA_API_KEY", "")
    monkeypatch.setattr(sources_openfda, "request_with_retry", fetch_page)
    task = (Path(__file__).resolve().parents[1] / "examples" / "hosted-acceptance.txt").read_text()
    task_request = json.loads(next(line for line in task.splitlines() if line.startswith('{"keywords"')))
    query = SearchRequest.from_dict(task_request)
    folder = write_report(query, collect(query, fetcher=run_source), tmp_path)
    evidence = json.loads((folder / "evidence.json").read_text())
    assert len(calls) == 1
    assert calls[0] == ("GET", "https://api.fda.gov/device/event.json", {
        "search": 'device.brand_name:"trocar" AND date_received:[20240101 TO 20241231]', "limit": 2, "skip": 0,
    })
    assert [(row["native_id"], row["search_date"]) for row in evidence] == [
        ("20155780", "2024-09-06"), ("20200955", "2024-09-12"),
    ]
    assert all(f"mdrfoi__id={row['native_id']}" in row["direct_record_url"] for row in evidence)
    add_assessment(folder, {
        "claims": [{"statement": "The B5LT report describes CO2 leakage.",
                    "support": [{"evidence_id": evidence[0]["evidence_id"], "quote": "leakage of co2"}]}],
        "limitations": ["Controlled replay of selected fields from two public FDA records; the retrieval cap leaves coverage partial."],
    })
    assert verify_report(folder, require_assessment=True)["evidence_records"] == 2
    assert check_public_report(folder, task) == {"retrieved": 2, "returned": 2, "exported": 2}


def test_nested_export_manifest_matches_outer_coverage(tmp_path):
    folder = make_report(tmp_path)
    nested = list((folder / "tables").rglob("manifest.json"))
    assert len(nested) == 1
    assert json.loads(nested[0].read_text())["overall_status"] == json.loads((folder / "overview.json").read_text())["status"]


def test_download_cli_exits_unsuccessfully_when_all_sources_failed(tmp_path, monkeypatch):
    from cloud_agent import hosted
    from cloud_agent.__main__ import main
    def failed(query, source):
        raise RuntimeError("source unavailable")
    query = request()
    folder = write_report(query, collect(query, fetcher=failed), tmp_path)
    path = add_assessment(folder, {"claims": [], "limitations": ["Both sources were unavailable."], "next_steps": []})
    markdown = path.read_text()
    assert "The search could not determine whether matching reports exist" in markdown
    assert "| FDA MAUDE | failed | 0 |" in markdown
    assert "| TGA DAEN | failed | 0 |" in markdown
    assert "No evidence-backed claims were provided in this assessment" in markdown
    monkeypatch.setattr(hosted, "collect_artifacts", lambda *a, **kw: [p for p in folder.rglob("*") if p.is_file()])
    assert main(["download", "--state", str(tmp_path / "state.json"), "--output", str(tmp_path)]) == 2


def test_report_without_required_excel_cannot_pass(tmp_path):
    folder = make_report(tmp_path)
    manifest_path = folder / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    for relative in list(manifest["files"]):
        if relative.endswith("incident_report.xlsx"):
            (folder / relative).unlink()
            del manifest["files"][relative]
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="Required CSV, Excel"):
        verify_report(folder)


def test_multiple_downloaded_runs_each_need_an_assessment(tmp_path):
    one, two = make_report(tmp_path), make_report(tmp_path)
    add_assessment(one, {"claims": [], "limitations": ["First diagnostic run."], "next_steps": []})
    files = [p for p in tmp_path.rglob("*") if p.is_file()]
    with pytest.raises(ValueError, match="Required report files"):
        verify_downloads(files)
    add_assessment(two, {"claims": [], "limitations": ["Final diagnostic run."], "next_steps": []})
    assert len(verify_downloads([p for p in tmp_path.rglob("*") if p.is_file()])) == 2


def test_official_document_fallback_stays_separate_and_supports_citations(tmp_path, monkeypatch):
    import hashlib
    from cloud_agent import supplement
    folder = make_report(tmp_path)
    assessment_path = add_assessment(folder, {"limitations": ["Awaiting supplemental review."]})
    raw = b"Official notice: component fracture."
    def fetch(source, url):
        return {"evidence_id": "WEB-FDA_MAUDE-abc123", "source": source, "source_name": "FDA", "coverage": "unverified", "direct_record_url": url, "raw_sha256": hashlib.sha256(raw).hexdigest(), "narrative": raw.decode(), "patient_outcome": "", "device_problem": "", "raw_record": {"text": raw.decode()}, "extraction_truncated": False}, raw, ".html"
    monkeypatch.setattr(supplement, "fetch_official_document", fetch)
    result = add_supplements(folder, [{"source": "FDA_MAUDE", "url": "https://www.fda.gov/notice"}])
    assert result["supplemental_documents"] == 1
    overview = json.loads((folder / "overview.json").read_text())
    assert overview["included_records"] == 1  # Original database count unchanged.
    assert overview["supplemental_coverage"] == "unverified"
    assert "**Supplemental documents** — 1 (coverage unverified)" in assessment_path.read_text()
    assert verify_report(folder, require_assessment=True)["evidence_records"] == 1
    add_assessment(folder, {"claims": [{"statement": "A separate official notice describes component fracture.", "support": [{"evidence_id": "WEB-FDA_MAUDE-abc123", "quote": "Official notice: component fracture."}]}], "limitations": ["The supplemental search is not exhaustive."], "next_steps": []})
    assert verify_report(folder)["evidence_records"] == 1


def test_supplemental_failure_is_recorded_without_erasing_primary_report(tmp_path, monkeypatch):
    from cloud_agent import supplement
    folder = make_report(tmp_path)
    def unavailable(*args, **kwargs):
        raise ValueError("Official document unavailable")
    monkeypatch.setattr(supplement, "fetch_official_document", unavailable)
    result = add_supplements(folder, [{"source": "FDA_MAUDE", "url": "https://www.fda.gov/notice"}])
    assert result["failed_attempts"] == 1
    assert verify_report(folder)["evidence_records"] == 1


def test_supplement_requires_source_in_the_original_request(tmp_path):
    folder = make_report(tmp_path)
    with pytest.raises(ValueError, match="requested source"):
        add_supplements(folder, [{"source": "BFARM_RECALLS", "url": "https://www.bfarm.de/notice.pdf"}])
