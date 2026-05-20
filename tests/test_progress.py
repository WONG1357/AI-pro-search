from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from pipeline.progress import ProgressReporter
from pipeline.pipeline import run_pipeline


def test_progress_reporter_source_lifecycle() -> None:
    reporter = ProgressReporter(["FDA_MAUDE"])
    reporter.start_source("FDA_MAUDE", total_steps=10, message="starting")
    reporter.update_source("FDA_MAUDE", current_step=5, records_fetched=100, stage="fetching")
    snapshot = reporter.get_snapshot()

    source = snapshot.sources["FDA_MAUDE"]
    assert source.status == "running"
    assert source.current_step == 5
    assert source.records_fetched == 100
    assert source.percent_complete == 50.0


def test_eta_calculation_with_known_total_steps() -> None:
    reporter = ProgressReporter(["FDA_MAUDE"])
    reporter.start_source("FDA_MAUDE", total_steps=4)
    reporter.update_source("FDA_MAUDE", current_step=2)
    source = reporter.get_snapshot().sources["FDA_MAUDE"]
    assert source.eta_seconds is not None


def test_overall_progress_calculation() -> None:
    reporter = ProgressReporter(["A", "B"])
    reporter.start_source("A", total_steps=10)
    reporter.update_source("A", current_step=5)
    reporter.complete_source("B")
    snapshot = reporter.get_snapshot()
    assert snapshot.overall_percent_complete is not None
    assert snapshot.overall_percent_complete > 0


def test_source_failure_progress() -> None:
    reporter = ProgressReporter(["A"])
    reporter.start_source("A", total_steps=2)
    reporter.fail_source("A", "boom")
    source = reporter.get_snapshot().sources["A"]
    assert source.status == "failed"
    assert source.percent_complete == 100.0


def test_run_pipeline_passes_progress_reporter(monkeypatch) -> None:
    from pipeline import pipeline as pipeline_module

    class FakeConnector:
        source_name = "FAKE"

        def fetch(self, **kwargs):
            assert kwargs["progress_reporter"] is not None
            kwargs["progress_reporter"].update_source("FAKE", current_step=1, total_steps=1, records_fetched=1, stage="done")
            return type(
                "R",
                (),
                    {
                        "source_name": "FAKE",
                        "warnings": [],
                        "success": True,
                        "error_message": None,
                    "records": pd.DataFrame(
                        [
                            {
                                "source": "FAKE",
                                "event_id": "1",
                                "report_number": "",
                                "date_of_event": "2025-01-01",
                                "date_received": "",
                                "event_date": "2025-01-01",
                                "year": 2025,
                                "received_year": 2025,
                                "product_name": "Trocar",
                                "brand_names": "",
                                "narrative_text": "Trocar issue",
                                "raw_link": "",
                                "event_link": "",
                            }
                        ]
                    ),
                },
            )()

    monkeypatch.setitem(pipeline_module.SOURCE_REGISTRY, "FAKE", FakeConnector)
    result = pipeline_module.run_pipeline(
        keywords=["trocar"],
        years=[2025],
        selected_sources=["FAKE"],
        export_results=False,
        source_options={},
    )
    assert result["progress"].overall_status == "completed"
    assert "FAKE" in result["progress"].sources
