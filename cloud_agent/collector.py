"""Bounded parallel retrieval and a source-by-source coverage ledger."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from typing import Callable

import pandas as pd

from cloud_agent.schema import SearchRequest
from pipeline.pipeline import deduplicate_records, summarize_by_source_category, summarize_by_year_category


def redact(message: object) -> str:
    text = str(message)
    text = re.sub(r"sk-[A-Za-z0-9_-]+", "[REDACTED]", text)
    text = re.sub(r"(?i)(authorization\s*[:=]\s*)(?:bearer\s+|basic\s+)?[^\s,;]+", r"\1[REDACTED]", text)
    text = re.sub(r"(?i)(api_key|token|authorization)([=:]\s*)[^\s&]+", r"\1\2[REDACTED]", text)
    return text[:1000]


def _failed(source: str, message: str, elapsed: float = 0.0) -> dict:
    return {
        "all_raw": [], "filtered": [],
        "source_errors": [{"source_name": source, "error_message": message}],
        "source_warnings": [],
        "source_status": [{"source_name": source, "success": False, "completeness": "failed", "retrieved_count": 0, "error_message": message, "elapsed_seconds": round(elapsed, 3)}],
    }


def _fetch_subprocess(request: SearchRequest, source: str) -> dict:
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="surveillance-source-") as temporary:
        folder = Path(temporary)
        request_path, result_path = folder / "request.json", folder / "result.json"
        request_path.write_text(json.dumps(request.as_dict()), encoding="utf-8")
        # No shell. All worker arguments are controlled file paths or validated source IDs.
        command = [sys.executable, "-m", "cloud_agent.worker", "--request", str(request_path), "--source", source, "--result", str(result_path)]
        try:
            completed = subprocess.run(
                command, cwd=Path(__file__).resolve().parents[1],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                timeout=request.source_timeout, check=False, text=True,
            )
        except subprocess.TimeoutExpired:
            return _failed(source, f"Source exceeded the {request.source_timeout}s elapsed-time budget; coverage is unknown, not zero.", time.monotonic() - started)
        if completed.returncode or not result_path.is_file():
            return _failed(source, "Source worker failed: " + redact(completed.stderr[-1000:] or f"exit {completed.returncode}"), time.monotonic() - started)
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
            if not isinstance(result, dict) or not isinstance(result.get("all_raw"), list):
                raise ValueError("Unexpected worker result schema")
        except (ValueError, OSError) as exc:
            return _failed(source, "Invalid worker result: " + redact(exc), time.monotonic() - started)
        for status in result.get("source_status", []):
            status["elapsed_seconds"] = round(time.monotonic() - started, 3)
        return result


def collect(request: SearchRequest, *, fetcher: Callable[[SearchRequest, str], dict] | None = None) -> dict[str, pd.DataFrame]:
    """Each source has independent resource limits; one failure cannot erase others."""
    fetcher = fetcher or _fetch_subprocess
    payloads: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=min(3, len(request.sources))) as executor:
        futures = {executor.submit(fetcher, request, source): source for source in request.sources}
        for future in as_completed(futures):
            source = futures[future]
            try:
                payloads[source] = future.result()
            except Exception as exc:
                payloads[source] = _failed(source, "Source failed: " + redact(exc))

    frames: dict[str, list[dict]] = {key: [] for key in ("all_raw", "filtered", "source_errors", "source_warnings", "source_status")}
    for source in request.sources:
        payload = payloads[source]
        status_list = payload.get("source_status") or [{
            "source_name": source, "success": not bool(payload.get("source_errors")),
            "completeness": "failed" if payload.get("source_errors") else "unverified",
            "retrieved_count": len(payload.get("all_raw", [])),
        }]
        # A source can never silently exceed the caller's export budget.
        for key in ("all_raw", "filtered"):
            rows = payload.get(key, [])
            if len(rows) > request.max_records:
                rows = rows[:request.max_records]
                for status in status_list:
                    status["completeness"] = "partial"
                frames["source_warnings"].append({"source_name": source, "warning_message": f"{key} capped at {request.max_records} records; results are partial."})
            frames[key].extend(rows)
        for status in status_list:
            status["source_name"] = source
            status.setdefault("completeness", "unverified")
            status["exported_count"] = min(len(payload.get("filtered", [])), request.max_records)
            status["requested_start"] = request.start_date
            status["requested_end"] = request.end_date
            frames["source_status"].append(status)
        for key in ("source_errors", "source_warnings"):
            frames[key].extend(payload.get(key, []))
    result = {key: pd.DataFrame(rows) for key, rows in frames.items()}
    filtered = result["filtered"]
    if not filtered.empty:
        # Workers already retain duplicate counts from raw retrieval. Avoid
        # resetting that evidence by deduplicating an already unique table.
        already_unique = ("record_uid" in filtered and filtered["record_uid"].notna().all()
                          and filtered["record_uid"].astype(str).ne("").all()
                          and not filtered["record_uid"].duplicated().any())
        if not already_unique:
            filtered = deduplicate_records(filtered)
        result["filtered"] = filtered
    for index, status in result["source_status"].iterrows():
        result["source_status"].loc[index, "exported_count"] = int(
            (filtered["source"] == status["source_name"]).sum()
        ) if not filtered.empty else 0
    result["summary_year_category"] = summarize_by_year_category(filtered) if not filtered.empty else pd.DataFrame()
    result["summary_source_category"] = summarize_by_source_category(filtered) if not filtered.empty else pd.DataFrame()
    return result
