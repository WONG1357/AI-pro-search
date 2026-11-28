"""Reconcile the toolkit's structured exports in a standard-library controller."""

import csv
import hashlib
import json
import posixpath
import re
import sys
from collections import Counter
from decimal import Decimal
from itertools import zip_longest
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile

from cloud_agent.assessment import _text


_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_REL = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
_IDENTIFIERS = ("source", "record_uid", "event_id", "report_number")
_FILTERED = (*_IDENTIFIERS, "category", "search_date", "search_date_basis")
_STATUS = ("source_name", "success", "completeness", "retrieved_count", "returned_count", "exported_count")
_NUMBERS = {"retrieved_count", "returned_count", "exported_count", "year", "incident_count"}


def _sheet_rows(book, part, strings):
    # The exporter uses inline strings. Also accept Excel's shared-string representation.
    columns = None
    with book.open(part) as stream:
        for _, row in ET.iterparse(stream, events=("end",)):
            if row.tag != _NS + "row":
                continue
            values = {}
            for cell in row.iterfind(_NS + "c"):
                if cell.find(_NS + "f") is not None:
                    raise ValueError("Workbook contains an unexpected formula")
                column = cell.attrib["r"].rstrip("0123456789")
                kind = cell.get("t")
                value = cell.findtext(_NS + "v", "")
                if kind == "inlineStr":
                    value = "".join(node.text or "" for node in cell.iter(_NS + "t"))
                elif kind == "s":
                    value = strings[int(value)]
                elif kind == "b":
                    value = {"1": "True", "0": "False"}[value]
                values[column] = value
            row.clear()
            if columns is None:
                columns = values
            else:
                yield {name: values.get(col, "") for col, name in columns.items()}


def _sheet_parts(book):
    workbook = ET.fromstring(book.read("xl/workbook.xml"))
    rels = ET.fromstring(book.read("xl/_rels/workbook.xml.rels"))
    parts = {rel.attrib["Id"]: posixpath.normpath(posixpath.join("xl", rel.attrib["Target"])).lstrip("/")
             for rel in rels if rel.get("TargetMode") != "External"}
    return {s.attrib["name"]: parts[s.attrib[_REL]] for s in workbook.iter(_NS + "sheet")}


def _jsonl(path):
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def _projection(row, fields, *, source=False, excel=False):
    result = []
    for field in fields:
        value = row.get(field)
        if field in _NUMBERS and value not in (None, ""):
            value = Decimal(str(value))
        else:
            value = "" if value is None else str(value)
            if source and (value.lstrip().startswith(("=", "+", "-", "@")) or value.startswith(("\t", "\r", "\n"))):
                value = "'" + value
            if source and excel:
                value = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", value)
                if len(value) > 32767:
                    value = value[:32710] + " [TRUNCATED: full value in raw_records.jsonl]"
        result.append(value)
    return tuple(result)


def verify_tables(folder: Path, overview: dict, evidence: list[dict], request: dict, outer_files: dict) -> None:
    manifests = list((folder / "tables").glob("*/manifest.json"))
    if len(manifests) != 1:
        raise ValueError("Expected one structured export manifest")
    table_dir = manifests[0].parent
    export = json.loads(manifests[0].read_text(encoding="utf-8"))
    if (export["query"] != request or export["source_status"] != overview["source_status"]
            or export["source_warnings"] != overview["source_warnings"]
            or export["source_errors"] != overview["source_errors"]
            or export["overall_status"] != overview.get("structured_retrieval_status", overview["status"])):
        raise ValueError("Structured export scope or coverage differs from overview")
    table_files = {path.name for path in table_dir.iterdir() if path.is_file() and path != manifests[0]}
    nested_files = export["files"].values()
    if Counter(file["name"] for file in nested_files) != Counter(table_files):
        raise ValueError("Structured export manifest has a different file inventory")
    for file in nested_files:
        relative = (table_dir / file["name"]).relative_to(folder).as_posix()
        if file["sha256"] != outer_files[relative]["sha256"]:
            raise ValueError("Structured export hash differs from the report manifest")

    by_source = Counter(item["source"] for item in evidence)
    for row in overview["source_status"]:
        if row["exported_count"] != by_source[row["source_name"]]:
            raise ValueError("Source coverage exported count differs from evidence")

    filtered = _jsonl(table_dir / "filtered_records.jsonl")
    year_category, source_category = Counter(), Counter()
    for item, record in zip_longest(evidence, filtered):
        if item is None or record is None:
            raise ValueError("Filtered JSONL count differs from evidence")
        raw = record.get("raw_record") or record
        native_id = _text(record.get("event_id") or record.get("report_number"))
        digest = hashlib.sha256(_text(raw).encode()).hexdigest()
        identity = hashlib.sha256((native_id or digest).encode()).hexdigest()[:16]
        if (item["source"] != record["source"] or item["native_id"] != native_id or
                item["evidence_id"] != f"E-{record['source']}-{identity}" or
                item["category"] != _text(record.get("category")) or
                item["search_date"] != _text(record.get("search_date")) or
                item["search_date_basis"] != _text(record.get("search_date_basis")) or
                item["narrative"] != _text(record.get("narrative_text")) or
                item["raw_sha256"] != digest or item["raw_record"] != raw):
            raise ValueError("Filtered JSONL identity or evidence payload differs from evidence")
        year_category[(record.get("search_year", record.get("year")), record.get("category"))] += 1
        source_category[(record["source"], record.get("category"))] += 1

    def summary(values, key):
        return [{key: group, "category": category, "incident_count": count}
                for (group, category), count in values.items()]

    tables = [
        ("all_raw", "all_raw", "all_raw_records.csv", _IDENTIFIERS, _jsonl(table_dir / "raw_records.jsonl"), overview["raw_records"]),
        ("filtered", "filtered_classified", "filtered_classified_records.csv", _FILTERED, _jsonl(table_dir / "filtered_records.jsonl"), len(evidence)),
        ("source_status", "source_status", "source_status.csv", _STATUS, overview["source_status"], len(overview["source_status"])),
        ("source_warnings", "source_warnings", "source_warnings.csv", ("source_name", "warning_message"), overview["source_warnings"], len(overview["source_warnings"])),
        ("source_errors", "source_errors", "source_errors.csv", ("source_name", "error_message"), overview["source_errors"], len(overview["source_errors"])),
        ("summary_year_category", "summary_year_category", "summary_year_category.csv", ("year", "category", "incident_count"), summary(year_category, "year"), len(year_category)),
        ("summary_source_category", "summary_source_category", "summary_source_category.csv", ("source", "category", "incident_count"), summary(source_category, "source"), len(source_category)),
    ]
    # A full source narrative can exceed csv.reader's default 128 KiB field limit.
    old_limit = csv.field_size_limit(sys.maxsize)
    try:
        with ZipFile(table_dir / "incident_report.xlsx") as book:
            parts = _sheet_parts(book)
            strings = []
            if "xl/sharedStrings.xml" in book.namelist():
                strings = ["".join(n.text or "" for n in item.iter(_NS + "t"))
                           for item in ET.fromstring(book.read("xl/sharedStrings.xml"))]
            for key, sheet, name, fields, expected, count in tables:
                expected_csv, expected_excel = Counter(), Counter()
                for row in expected:
                    expected_csv[_projection(row, fields, source=True)] += 1
                    expected_excel[_projection(row, fields, source=True, excel=True)] += 1
                if sum(expected_csv.values()) != count or export["row_counts"][key] != count:
                    raise ValueError(f"Row count differs from overview or evidence: {sheet}")
                with (table_dir / name).open(encoding="utf-8-sig", newline="") as stream:
                    if Counter(_projection(row, fields) for row in csv.DictReader(stream)) != expected_csv:
                        raise ValueError(f"CSV rows differ from report: {name}")
                actual = _sheet_rows(book, parts[sheet], strings)
                if Counter(_projection(row, fields) for row in actual) != expected_excel:
                    raise ValueError(f"Workbook rows differ from report: {sheet}")
    finally:
        csv.field_size_limit(old_limit)
