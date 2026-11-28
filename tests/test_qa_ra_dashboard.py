"""Exercise the dashboard as a surveillance reviewer using controlled source replies."""

from datetime import date
import hashlib
from io import BytesIO
import json
from zipfile import ZipFile

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

import app
from pipeline.sources import SourceFetchResult


def dashboard(monkeypatch, tmp_path):
    monkeypatch.setattr(app, "EXPORTS_DIR", tmp_path)
    page = AppTest.from_string("import app; app.main()", default_timeout=15).run()
    page.date_input(key="search_date_range").set_value((date(2024, 1, 1), date(2024, 3, 1))).run()
    return page


def source_reply(monkeypatch, source, records=(), *, failed=False):
    class ControlledSource:
        def fetch(self, **kwargs):
            return SourceFetchResult(
                source, pd.DataFrame(records), not failed,
                error_message="Source response timed out." if failed else None,
                completeness="failed" if failed else "complete",
            )

    monkeypatch.setitem(app.SOURCE_REGISTRY, source, ControlledSource)


def click_search(page):
    next(button for button in page.button if button.label == "Run Search").click().run()
    assert not page.exception


def test_dashboard_preserves_review_scope_and_downloads_through_draft_edits(monkeypatch, tmp_path):
    source_reply(monkeypatch, "FDA_MAUDE", [{
        "source": "FDA_MAUDE",
        "event_id": "FDA-1",
        "report_number": "FDA-1",
        "event_date": "2024-02-01",
        "report_date": "2024-02-02",
        "product_name": "Trocar",
        "manufacturer": "Fixture manufacturer",
        "narrative_text": "The trocar tip broke. No patient injury was reported.",
        "fda_match_category": "Brand name matches",
    }])
    source_reply(monkeypatch, "TGA_DAEN", failed=True)
    page = dashboard(monkeypatch, tmp_path)
    page.checkbox(key="source_TGA_DAEN").check().run()
    click_search(page)

    assert "Selected sources: FDA MAUDE, TGA DAEN" in [element.value for element in page.text]
    assert "Requested dates: 2024-01-01 to 2024-03-01" in [element.value for element in page.text]
    assert any("could not be fully checked" in element.value for element in page.warning)
    tabs = [tab.label for tab in page.tabs]
    assert "TGA DAEN" in tabs
    assert "Health Canada MDI" not in tabs
    assert any("This source has failed coverage" in element.value for element in page.warning)
    assert any("evidence" in element.proto.label.lower() and "zip" in element.proto.label.lower()
               for element in page.get("download_button"))
    export_files = page.session_state["results"]["export_files"]
    previous_manifest = export_files["manifest"]
    with ZipFile(BytesIO(app.evidence_archive(tuple(export_files.values())))) as archive:
        manifest = json.loads(archive.read("manifest.json"))
        expected_files = {item["name"]: item["sha256"] for item in manifest["files"].values()}
        assert sorted(archive.namelist()) == sorted({"manifest.json", *expected_files})
        assert {"raw_records.jsonl", "filtered_records.jsonl"} <= expected_files.keys()
        for filename, checksum in expected_files.items():
            assert hashlib.sha256(archive.read(filename)).hexdigest() == checksum

    page.date_input(key="search_date_range").set_value((date(2024, 4, 1), date(2024, 5, 1))).run()
    assert "Requested dates: 2024-01-01 to 2024-03-01" in [element.value for element in page.text]
    assert any("Search inputs have changed" in element.value for element in page.warning)

    next(field for field in page.text_input if field.label == "Keywords").set_value("").run()
    click_search(page)
    assert any("Enter at least one device keyword" in element.value for element in page.error)
    assert page.session_state["results"]["export_files"]["manifest"] == previous_manifest
    assert previous_manifest.is_file()

    def failed_search(**kwargs):
        raise RuntimeError("Internal fixture failure")

    next(field for field in page.text_input if field.label == "Keywords").set_value("Trocar").run()
    monkeypatch.setattr(app, "run_pipeline", failed_search)
    click_search(page)
    assert any("previous search and its downloads" in element.value for element in page.error)
    assert page.session_state["results"]["export_files"]["manifest"] == previous_manifest


@pytest.mark.parametrize("failed", [False, True], ids=["completed-no-match", "failed-coverage"])
def test_dashboard_distinguishes_completed_no_match_from_failed_coverage(monkeypatch, tmp_path, failed):
    source_reply(monkeypatch, "FDA_MAUDE", failed=failed)
    page = dashboard(monkeypatch, tmp_path)
    click_search(page)
    page.run()

    messages = [element.value for element in page.info]
    warnings = [element.value for element in page.warning]
    if failed:
        assert any("No matches could be confirmed" in message for message in warnings)
        assert not any("No records matched" in message for message in messages)
    else:
        assert any("No records matched the selected sources" in message for message in messages)
        assert not any("No matches could be confirmed" in message for message in warnings)
    assert len(page.session_state["results"]["filtered"]) == 0


def test_dashboard_retains_raw_evidence_when_local_dates_exclude_every_record(monkeypatch, tmp_path):
    source_reply(monkeypatch, "FDA_MAUDE", [{
        "source": "FDA_MAUDE",
        "event_id": "FDA-OLD",
        "report_number": "FDA-OLD",
        "report_date": "2024-04-02",
        "event_date": "2024-02-01",
        "product_name": "Trocar",
        "narrative_text": "The trocar did not open.",
    }])
    page = dashboard(monkeypatch, tmp_path)
    click_search(page)
    page.run()

    assert any("No records matched the selected sources" in element.value for element in page.info)
    results = page.session_state["results"]
    assert len(results["all_raw"]) == 1
    assert results["filtered"].empty
    assert "Exports & Raw Data" in [tab.label for tab in page.tabs]
