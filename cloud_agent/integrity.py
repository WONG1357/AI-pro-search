"""Artifact verification using only the standard library, including in cloud controllers."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


def verify_report(folder: Path, *, require_assessment: bool = False) -> dict:
    folder = folder.resolve(strict=True)
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    required = {"request.json", "overview.json", "evidence.json", "evidence_index.json", "report.md"}
    if require_assessment:
        required |= {"assessment.json", "assessment.md"}
    if not required.issubset(manifest.get("files", {})):
        raise ValueError("Required report files are missing")
    expected_exports = {
        "all_raw_records.csv", "filtered_classified_records.csv", "summary_year_category.csv",
        "summary_source_category.csv", "source_status.csv", "source_warnings.csv",
        "source_errors.csv", "raw_records.jsonl", "filtered_records.jsonl", "incident_report.xlsx",
    }
    exported = {Path(name).name for name in manifest["files"] if name.startswith("tables/")}
    if not expected_exports.issubset(exported):
        raise ValueError("Required CSV, Excel, or raw evidence exports are missing")
    for relative, info in manifest["files"].items():
        path = folder / relative
        if path.is_symlink() or not path.resolve().is_relative_to(folder):
            raise ValueError("Unsafe artifact path")
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != info["sha256"]:
            raise ValueError(f"Artifact integrity check failed: {relative}")
    actual = {p.relative_to(folder).as_posix() for p in folder.rglob("*") if p.is_file() and p != folder / "manifest.json"}
    if actual != set(manifest["files"]):
        raise ValueError("Artifact set changed since manifest creation")
    overview = json.loads((folder / "overview.json").read_text(encoding="utf-8"))
    request = json.loads((folder / "request.json").read_text(encoding="utf-8"))
    evidence = json.loads((folder / "evidence.json").read_text(encoding="utf-8"))
    if overview["included_records"] != len(evidence):
        raise ValueError("Report count does not match its evidence")
    if len({r["evidence_id"] for r in evidence}) != len(evidence):
        raise ValueError("Evidence identifiers are not unique")
    supplemental_count = overview.get("supplemental_documents", 0)
    if supplemental_count:
        supplemental = json.loads((folder / "supplemental_evidence.json").read_text(encoding="utf-8"))
        if len(supplemental) != supplemental_count:
            raise ValueError("Supplemental document count does not match its evidence")
        identifiers = [r["evidence_id"] for r in evidence + supplemental]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("Supplemental evidence identifiers are not unique")
        if any(row.get("coverage") != "unverified" for row in supplemental):
            raise ValueError("Supplemental discovery must not claim verified database coverage")
        for row in supplemental:
            document = folder / row["local_document"]
            if not document.resolve().is_relative_to(folder) or hashlib.sha256(document.read_bytes()).hexdigest() != row["raw_sha256"]:
                raise ValueError("Supplemental original document does not match its evidence hash")
    if {row["source_name"] for row in overview["source_status"]} != set(request["sources"]):
        raise ValueError("Coverage ledger does not match the requested sources")
    if overview["run_id"] != manifest["run_id"] or overview["status"] != manifest["status"]:
        raise ValueError("Manifest does not match report identity or coverage")
    return {"run_id": manifest["run_id"], "status": overview["status"], "verified_files": len(manifest["files"]), "evidence_records": len(evidence)}


def verify_downloads(files: list[Path], *, require_assessment: bool = True) -> list[dict]:
    roots = sorted({path.parent for path in files if path.name == "overview.json"})
    if not roots:
        raise ValueError("No surveillance report was produced by the completed turn")
    return [verify_report(root, require_assessment=require_assessment) for root in roots]
