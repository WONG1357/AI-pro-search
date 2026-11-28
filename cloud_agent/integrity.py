"""Artifact verification using only the standard library, including in cloud controllers."""

from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
import re

from cloud_agent.assessment import render_assessment
from cloud_agent.table_integrity import verify_tables


def sha256_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


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
        if not path.is_file() or path.stat().st_size != info["bytes"] or sha256_file(path) != info["sha256"]:
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
    supplemental = []
    if supplemental_count or (folder / "supplemental_evidence.json").is_file():
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
            if (not document.is_file() or not document.resolve().is_relative_to(folder)
                    or sha256_file(document) != row["raw_sha256"]):
                raise ValueError("Supplemental original document does not match its evidence hash")
    if overview["request"] != request:
        raise ValueError("Overview does not match the search request")
    if Counter(row["source_name"] for row in overview["source_status"]) != Counter(request["sources"]):
        raise ValueError("Coverage ledger does not match the requested sources")
    if overview["run_id"] != manifest["run_id"] or overview["status"] != manifest["status"]:
        raise ValueError("Manifest does not match report identity or coverage")
    if require_assessment:
        rows = {r["evidence_id"]: r for r in evidence + supplemental}
        assessment = json.loads((folder / "assessment.json").read_text(encoding="utf-8"))
        rendered = render_assessment(assessment, rows)
        if (folder / "assessment.md").read_text(encoding="utf-8") != rendered:
            raise ValueError("Assessment Markdown differs from the cited assessment")
        kinds = Counter(row["record_kind"] for row in evidence)
        if +Counter(overview["counts_by_record_kind"]) != kinds:
            raise ValueError("Overview record types differ from evidence")
        index = json.loads((folder / "evidence_index.json").read_text(encoding="utf-8"))
        expected_index = [
            {k: v for k, v in row.items() if k not in {"raw_record", "narrative"}}
            | {"narrative_preview": row["narrative"][:500]}
            for row in evidence
        ]
        if index != expected_index:
            raise ValueError("Evidence index differs from full evidence")
        verify_tables(folder, overview, evidence, request, manifest["files"])
    return {"run_id": manifest["run_id"], "status": overview["status"], "verified_files": len(manifest["files"]), "evidence_records": len(evidence)}


def verify_downloads(files: list[Path], *, require_assessment: bool = True) -> list[dict]:
    downloaded = {p.resolve() for p in files if p.is_file()}
    markers = {"overview.json", "evidence.json", "evidence_index.json", "request.json",
               "report.md", "assessment.json", "assessment.md", "supplemental_evidence.json"}
    roots = set()
    for path in downloaded:
        run = next((parent for parent in path.parents
                    if re.fullmatch(r"run-\d{8}T\d{6}Z-[0-9a-f]{12}", parent.name)), None)
        if run is not None:
            roots.add(run)
        elif path.name in markers:
            roots.add(path.parent)
    if not roots:
        raise ValueError("No surveillance report was produced by the completed turn")
    verified = []
    for root in sorted(roots):
        # A file left over from a previous download cannot fill a gap in this turn.
        report = verify_report(root, require_assessment=require_assessment)
        required = {p.resolve() for p in root.rglob("*") if p.is_file()}
        if not required.issubset(downloaded):
            raise ValueError("The exact-turn download is missing required report artifacts")
        verified.append(report)
    return verified
