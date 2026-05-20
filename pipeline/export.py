"""CSV and Excel export helpers for pipeline outputs."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def export_pipeline_results(
    results: dict[str, pd.DataFrame],
    output_dir: str | Path,
    file_prefix: str = "",
) -> dict[str, Path]:
    """Write pipeline result tables to CSV files and a multi-sheet Excel workbook."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    prefix = f"{file_prefix}_" if file_prefix else ""

    files = {
        "all_raw": output_path / f"{prefix}all_raw_records.csv",
        "filtered": output_path / f"{prefix}filtered_classified_records.csv",
        "summary_year_category": output_path / f"{prefix}summary_year_category.csv",
        "summary_source_category": output_path / f"{prefix}summary_source_category.csv",
        "excel": output_path / f"{prefix}incident_report.xlsx",
    }

    all_raw = results.get("all_raw", pd.DataFrame())
    filtered = results.get("filtered", pd.DataFrame())
    summary_year_category = results.get("summary_year_category", pd.DataFrame())
    summary_source_category = results.get("summary_source_category", pd.DataFrame())

    all_raw.to_csv(files["all_raw"], index=False)
    filtered.to_csv(files["filtered"], index=False)
    summary_year_category.to_csv(files["summary_year_category"], index=False)
    summary_source_category.to_csv(files["summary_source_category"], index=False)

    with pd.ExcelWriter(files["excel"], engine="openpyxl") as writer:
        all_raw.to_excel(writer, sheet_name="all_raw", index=False)
        filtered.to_excel(writer, sheet_name="filtered_classified", index=False)
        summary_year_category.to_excel(writer, sheet_name="summary_year_category", index=False)
        summary_source_category.to_excel(writer, sheet_name="summary_source_category", index=False)

    return files
