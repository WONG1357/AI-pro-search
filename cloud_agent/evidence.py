"""Evidence-bound reports, portable run manifests, and citation validation."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlsplit
from uuid import uuid4

import pandas as pd

from cloud_agent.schema import SearchRequest, SOURCE_DOMAINS, SOURCE_INFO
from cloud_agent.assessment import ASSESSMENT_FORMAT_VERSION, _md, _text, render_assessment
from cloud_agent.integrity import verify_report
from pipeline.export import export_pipeline_results


def json_rows(frame: pd.DataFrame | None) -> list[dict]:
    if frame is None or frame.empty:
        return []
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def official_url(value: object, source: str) -> str | None:
    url = _text(value).strip()
    try:
        parsed = urlsplit(url)
        if parsed.scheme not in ("https", "http") or parsed.username or parsed.password:
            return None
        if parsed.hostname not in SOURCE_DOMAINS.get(source, set()):
            return None
        if parsed.port not in (None, 80, 443) or any(c in url for c in ("\n", "\r", "\t")):
            return None
    except ValueError:
        return None
    return url


def direct_record_url(record: dict, source: str) -> str | None:
    """Return an official record-level link, excluding source search and listing pages."""
    for field in ("event_link", "device_link", "raw_link"):
        candidate = official_url(record.get(field), source)
        if not candidate:
            continue
        path = urlsplit(candidate).path.lower().rstrip("/")
        if path.endswith(("mdi_results.php", "daen-entry.aspx", "search.cfm", "/drug-device-alerts",
                          "/mep", "/expertensuche_formular.html")):
            continue
        if source == "MHRA_FSCA" and path.startswith("/drug-device-alerts/field-safety-notices"):
            continue
        return candidate
    return None


def build_evidence(records: pd.DataFrame, retrieved_at: str) -> list[dict]:
    evidence = []
    for record in json_rows(records):
        source = _text(record.get("source"))
        if source not in SOURCE_INFO:
            raise ValueError(f"Unknown evidence source: {source}")
        raw = record.get("raw_record") or record
        raw_text = _text(raw)
        digest = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
        native_id = _text(record.get("event_id") or record.get("report_number"))
        identity = native_id or digest
        identifier = "E-" + source + "-" + hashlib.sha256(identity.encode()).hexdigest()[:16]
        evidence.append({
            "evidence_id": identifier,
            "source": source,
            "source_name": SOURCE_INFO[source][0],
            "record_kind": SOURCE_INFO[source][2],
            "native_id": native_id,
            "product_name": _text(record.get("product_name")),
            "manufacturer": _text(record.get("manufacturer")),
            "event_date": _text(record.get("event_date") or record.get("date_of_event")),
            "report_date": _text(record.get("report_date") or record.get("date_received")),
            "search_date": _text(record.get("search_date")),
            "search_date_basis": _text(record.get("search_date_basis")),
            "direct_record_url": direct_record_url(record, source),
            "source_locator_url": official_url(record.get("raw_link"), source) or SOURCE_INFO[source][1],
            "source_portal": SOURCE_INFO[source][1],
            "retrieved_at": retrieved_at,
            "raw_sha256": digest,
            "category": _text(record.get("category")),
            "category_reason": _text(record.get("category_reason")),
            "classification_method": "rule_based_screening_requires_review",
            "narrative": _text(record.get("narrative_text")),
            "patient_outcome": _text(record.get("patient_outcome")),
            "device_problem": _text(record.get("device_problem_text")),
            "raw_record": raw,
        })
    ids = [record["evidence_id"] for record in evidence]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate evidence identity survived deduplication; report was not published")
    return evidence


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def _manifest(folder: Path, run_id: str, *, status: str, assessment_format_version: int | None = None) -> dict:
    files = {}
    for path in sorted(folder.rglob("*")):
        if not path.is_file() or path == folder / "manifest.json":
            continue
        if path.is_symlink():
            raise ValueError("Report artifacts must not be symlinks")
        relative = path.relative_to(folder).as_posix()
        files[relative] = {"sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "bytes": path.stat().st_size}
    manifest = {"schema_version": 1, "run_id": run_id, "status": status, "files": files}
    if assessment_format_version is not None:
        manifest["assessment_format_version"] = assessment_format_version
    _write_json(folder / "manifest.json", manifest)
    return manifest


def write_report(request: SearchRequest, results: dict[str, pd.DataFrame], output_root: Path) -> Path:
    retrieved_at = datetime.now(timezone.utc).isoformat()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid4().hex[:12]
    folder = output_root / ("run-" + run_id)
    folder.mkdir(parents=True, exist_ok=False)
    evidence = build_evidence(results.get("filtered", pd.DataFrame()), retrieved_at)
    statuses = json_rows(results.get("source_status"))
    if {s.get("source_name") for s in statuses} != set(request.sources):
        raise ValueError("Coverage ledger must contain every selected source")
    errors = json_rows(results.get("source_errors"))
    warnings = json_rows(results.get("source_warnings"))
    complete = all(s.get("completeness") == "complete" and s.get("success") is True for s in statuses) and not errors
    failed = not evidence and all(s.get("completeness") == "failed" or s.get("success") is False for s in statuses)
    status = "failed" if failed else "complete_for_requested_queries" if complete else "incomplete"
    kinds = {kind: sum(item["record_kind"] == kind for item in evidence) for kind in ("adverse_event_report", "safety_notice")}
    overview = {
        "schema_version": 1, "run_id": run_id, "status": status,
        "request": request.as_dict(), "retrieved_at": retrieved_at,
        "raw_records": len(results.get("all_raw", [])), "included_records": len(evidence),
        "counts_by_record_kind": kinds, "source_status": statuses,
        "source_errors": errors, "source_warnings": warnings,
        "records_without_direct_link": sum(not r["direct_record_url"] for r in evidence),
        "count_semantics": "Counts are retrieved reports/notices, not unique patients, proven causal events, incidence rates, or a device safety ranking. Cross-regulator reports may overlap.",
        "coverage_semantics": "Complete means the specified connector query was exhausted within its documented scope; it does not establish all worldwide incidents or absence of risk.",
    }
    _write_json(folder / "request.json", request.as_dict())
    _write_json(folder / "overview.json", overview)
    _write_json(folder / "evidence.json", evidence)
    _write_json(folder / "evidence_index.json", [{k: v for k, v in row.items() if k not in ("raw_record", "narrative")} | {"narrative_preview": row["narrative"][:500]} for row in evidence])
    exported = export_pipeline_results({**results, "query": request.as_dict(), "overall_status": status}, folder / "tables")
    lines = [
        "# Medical-device surveillance report", "", f"**Run:** `{run_id}`  ",
        f"**Status:** {status}  ", f"**Retrieved:** {retrieved_at}  ",
        f"**Requested dates:** {request.start_date} through {request.end_date} (inclusive)", "",
        "## Search scope", "", _md(request.question), "",
        "Device terms: " + ", ".join(_md(t) for t in request.keywords) + ".", "",
        "## Coverage", "",
        "| Source | Coverage | Exported records | Detail |", "| --- | --- | ---: | --- |",
    ]
    for s in statuses:
        source = s["source_name"]
        count = sum(r["source"] == source for r in evidence)
        detail = s.get("error_message") or s.get("message") or "See source warnings and original records."
        lines.append(f"| {SOURCE_INFO[source][0]} | {_md(s.get('completeness', 'unverified'))} | {count} | {_md(detail)} |")
    lines += ["", "## What the counts mean", "", overview["count_semantics"], "", overview["coverage_semantics"], ""]
    lines += [f"Retrieved **{kinds['adverse_event_report']} adverse-event reports** and **{kinds['safety_notice']} safety notices** after filtering and within-source deduplication.", ""]
    if failed:
        lines += ["**All selected sources failed. This run cannot answer whether matching reports exist.**", ""]
    elif not complete:
        lines += ["**Coverage is incomplete or unverified. An empty source is not evidence of no incidents.**", ""]
    if warnings or errors:
        lines += ["## Source messages", ""]
        for message in errors + warnings:
            lines.append(f"- **{_md(message.get('source_name'))}:** {_md(message.get('error_message') or message.get('warning_message'))}")
        lines.append("")
    lines += [
        "## Evidence index", "", "The full source payloads and evidence hashes are retained in `evidence.json` and the raw exports. A portal link is a locator and is not a direct incident citation.", "",
        "| Evidence ID | Type | Source date | Product | Source link |", "| --- | --- | --- | --- | --- |",
    ]
    for row in evidence:
        direct = row["direct_record_url"]
        label, link = ("Record", direct) if direct else ("Portal / locator", row["source_locator_url"])
        safe_link = link.replace("(", "%28").replace(")", "%29").replace(" ", "%20")
        lines.append(f"| `{row['evidence_id']}` | {row['record_kind']} | {_md(row['search_date'])} | {_md(row['product_name'])} | [{label}]({safe_link}) |")
    lines += ["", "## Interpretation status", "", "Rule-based categories are screening labels for review. This deterministic report has not inferred causation, clinical risk, or regulatory compliance. Any agent interpretation is supplied separately with validated evidence references.", ""]
    lines += ["## Files", ""]
    for name, path in exported.items():
        lines.append(f"- {name}: `{path.relative_to(folder).as_posix()}`")
    (folder / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    _manifest(folder, run_id, status=status)
    verify_report(folder)
    return folder


def add_assessment(folder: Path, assessment: dict) -> Path:
    """Validate factual references and literal excerpts; semantic entailment still needs review."""
    verify_report(folder)
    evidence = {r["evidence_id"]: r for r in json.loads((folder / "evidence.json").read_text(encoding="utf-8"))}
    supplemental = folder / "supplemental_evidence.json"
    if supplemental.is_file():
        evidence.update({r["evidence_id"]: r for r in json.loads(supplemental.read_text(encoding="utf-8"))})
    overview = json.loads((folder / "overview.json").read_text(encoding="utf-8"))
    destination = folder / "assessment.md"
    destination.write_text(render_assessment(assessment, evidence, overview=overview), encoding="utf-8")
    _write_json(folder / "assessment.json", assessment)
    _manifest(folder, overview["run_id"], status=overview["status"],
              assessment_format_version=ASSESSMENT_FORMAT_VERSION)
    verify_report(folder)
    return destination


def add_supplements(folder: Path, entries: list[dict]) -> dict:
    """Preserve independently discovered official documents without fabricating rows."""
    from cloud_agent.supplement import fetch_official_document
    from cloud_agent.collector import redact
    verify_report(folder)
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    assessment_format = manifest.get("assessment_format_version")
    if not isinstance(entries, list) or not entries or len(entries) > 5:
        raise ValueError("Supply one to five explicit official document links")
    request = json.loads((folder / "request.json").read_text(encoding="utf-8"))
    for entry in entries:
        if (not isinstance(entry, dict) or set(entry) - {"source", "url", "discovery_note"}
                or entry.get("source") not in request["sources"]
                or not isinstance(entry.get("url"), str)
                or not isinstance(entry.get("discovery_note", ""), str)
                or len(entry.get("discovery_note", "")) > 1000):
            raise ValueError("Each document must name a requested source, URL, and optional discovery note")
    evidence_path = folder / "supplemental_evidence.json"
    errors_path = folder / "supplemental_errors.json"
    evidence = json.loads(evidence_path.read_text()) if evidence_path.exists() else []
    errors = json.loads(errors_path.read_text()) if errors_path.exists() else []
    if len(evidence) + len(errors) + len(entries) > 5:
        raise ValueError("At most five supplemental document attempts are allowed per run")
    for entry in entries:
        try:
            record, raw, extension = fetch_official_document(entry["source"], entry["url"])
            if extension not in {".pdf", ".html", ".txt"} or not re.fullmatch(r"[A-Za-z0-9_-]+", record["evidence_id"]):
                raise ValueError("Unsafe supplemental artifact identifier")
            if any(row["evidence_id"] == record["evidence_id"] for row in evidence):
                continue
            raw_folder = folder / "supplemental_documents"
            if raw_folder.is_symlink():
                raise ValueError("Supplemental artifact directory cannot be a symlink")
            raw_folder.mkdir(exist_ok=True)
            raw_path = raw_folder / (record["evidence_id"] + extension)
            with raw_path.open("xb") as handle:
                handle.write(raw)
            record["local_document"] = raw_path.relative_to(folder).as_posix()
            record["discovery_note"] = entry.get("discovery_note", "")
            evidence.append(record)
        except Exception as exc:
            errors.append({"source": entry["source"], "url": entry["url"], "error": redact(exc)})
    _write_json(evidence_path, evidence)
    _write_json(errors_path, errors)
    overview = json.loads((folder / "overview.json").read_text(encoding="utf-8"))
    overview.setdefault("structured_retrieval_status", overview["status"])
    overview["supplemental_documents"] = len(evidence)
    overview["supplemental_coverage"] = "unverified"
    overview["supplemental_errors"] = errors
    if evidence and overview["status"] == "failed":
        overview["status"] = "incomplete"
    _write_json(folder / "overview.json", overview)
    lines = ["# Supplemental official documents", "", "These documents were discovered separately from the structured searches. Their coverage is unverified. They are not added to incident counts, and a successful download does not repair a failed catalogue query.", ""]
    for record in evidence:
        link = record["direct_record_url"].replace("(", "%28").replace(")", "%29")
        lines += [f"- `{record['evidence_id']}` — [{_md(record['source_name'])}]({link}); extraction truncated: {bool(record.get('extraction_truncated'))}; original: `{record['local_document']}`."]
    if errors:
        lines += ["", "## Failed document attempts", ""]
        lines += [f"- {_md(error['source'])}: {_md(error['error'])}" for error in errors]
    (folder / "supplemental.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    if assessment_format == ASSESSMENT_FORMAT_VERSION:
        assessment = json.loads((folder / "assessment.json").read_text(encoding="utf-8"))
        primary = json.loads((folder / "evidence.json").read_text(encoding="utf-8"))
        assessment_rows = {row["evidence_id"]: row for row in primary + evidence}
        markdown = render_assessment(assessment, assessment_rows, overview=overview)
        (folder / "assessment.md").write_text(markdown, encoding="utf-8")
    _manifest(folder, overview["run_id"], status=overview["status"],
              assessment_format_version=assessment_format)
    verify_report(folder)
    return {"supplemental_documents": len(evidence), "failed_attempts": len(errors), "status": overview["status"], "coverage": "unverified"}
