"""Isolated, auditable CSV, JSONL and Excel reports for surveillance runs."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any
from uuid import uuid4

import pandas as pd


TABLES = {
    "all_raw": ("all_raw_records.csv", "all_raw"),
    "filtered": ("filtered_classified_records.csv", "filtered_classified"),
    "summary_year_category": ("summary_year_category.csv", "summary_year_category"),
    "summary_source_category": ("summary_source_category.csv", "summary_source_category"),
    "source_status": ("source_status.csv", "source_status"),
    "source_warnings": ("source_warnings.csv", "source_warnings"),
    "source_errors": ("source_errors.csv", "source_errors"),
}
_ILLEGAL_XML = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def spreadsheet_safe_value(value: Any, *, excel: bool = False) -> Any:
    """Treat retrieved text as text, including strings beginning with formulas.

    Full source values remain in JSONL; workbook-only limits are explicit in
    the manifest, so long narratives never masquerade as complete evidence.
    """
    if isinstance(value, (dict, list)):
        value = json.dumps(value, ensure_ascii=False, default=str)
    if not isinstance(value, str):
        return value
    if value.lstrip().startswith(("=", "+", "-", "@")) or value.startswith(("\t", "\r", "\n")):
        value = "'" + value
    if excel:
        value = _ILLEGAL_XML.sub("", value)
        if len(value) > 32767:
            value = value[:32710] + " [TRUNCATED: full value in raw_records.jsonl]"
    return value


def spreadsheet_safe_frame(frame: pd.DataFrame, *, excel: bool = False) -> pd.DataFrame:
    safe = frame.copy()
    for column in safe.columns:
        safe[column] = safe[column].map(lambda value: spreadsheet_safe_value(value, excel=excel))
    safe.columns = [spreadsheet_safe_value(str(column), excel=excel) for column in safe.columns]
    return safe


def export_pipeline_results(
    results: dict[str, Any],
    output_dir: str | Path,
    file_prefix: str = "",
) -> dict[str, Path]:
    """Write a unique run directory, including source failures and coverage.

    Returning paths is the commit point. Incomplete writes are not advertised
    as a finished report. Existing exports are never overwritten.
    """
    if file_prefix and not re.fullmatch(r"[A-Za-z0-9_.-]{1,120}", file_prefix):
        raise ValueError("Export prefix must be a filename-safe identifier")
    if file_prefix in {".", ".."}:
        raise ValueError("Export prefix cannot be a path component")
    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex
    output_path = Path(output_dir) / run_id
    output_path.mkdir(parents=True, exist_ok=False)
    prefix = f"{file_prefix}_" if file_prefix else ""
    files: dict[str, Path] = {}

    frames = {name: results.get(name, pd.DataFrame()) for name in TABLES}
    for name, (filename, _) in TABLES.items():
        frame = frames[name]
        if not isinstance(frame, pd.DataFrame):
            raise TypeError(f"{name} must be a DataFrame")
        files[name] = output_path / f"{prefix}{filename}"
        spreadsheet_safe_frame(frame).to_csv(files[name], index=False, encoding="utf-8-sig")

    files["raw_jsonl"] = output_path / "raw_records.jsonl"
    frames["all_raw"].to_json(files["raw_jsonl"], orient="records", lines=True, force_ascii=False, date_format="iso")
    files["filtered_jsonl"] = output_path / "filtered_records.jsonl"
    frames["filtered"].to_json(files["filtered_jsonl"], orient="records", lines=True, force_ascii=False, date_format="iso")
    files["excel"] = output_path / f"{prefix}incident_report.xlsx"
    with pd.ExcelWriter(files["excel"], engine="openpyxl") as writer:
        for name, (_, sheet_name) in TABLES.items():
            spreadsheet_safe_frame(frames[name], excel=True).to_excel(writer, sheet_name=sheet_name, index=False)

    progress = results.get("progress")
    metadata = {
        "schema_version": 1, "run_id": run_id, "created_at": now.isoformat(),
        "overall_status": results.get("overall_status", getattr(progress, "overall_status", "unverified")),
        "query": results.get("query", {}),
        "source_status": json.loads(frames["source_status"].to_json(orient="records")),
        "source_warnings": json.loads(frames["source_warnings"].to_json(orient="records")),
        "source_errors": json.loads(frames["source_errors"].to_json(orient="records")),
        "row_counts": {name: len(frame) for name, frame in frames.items()},
        "spreadsheet_text_policy": "Formula-looking strings are escaped. Excel control characters are removed and cells over 32767 characters are marked truncated. JSONL retains full source text.",
        "scope": "Counts are retrieved records within the declared query, not incidence rates or proof that no other events occurred.",
        "files": {name: {"name": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for name, path in files.items()},
    }
    files["manifest"] = output_path / "manifest.json"
    files["manifest"].write_text(json.dumps(metadata, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return files
