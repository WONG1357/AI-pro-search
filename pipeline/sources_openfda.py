"""openFDA MAUDE device event source adapter."""

from __future__ import annotations

import hashlib
import json
import time
from datetime import date
from typing import Callable, Iterable

import pandas as pd
import requests

from pipeline.config import OPENFDA_API_KEY, OPENFDA_DEVICE_EVENT_ENDPOINT, UNIFIED_COLUMNS
from pipeline.utils import format_fda_date, parse_year_from_date
from pipeline.source_support import request_with_retry, with_coverage


# Verified against https://open.fda.gov/fields/deviceevent.yaml (2026-11-21).
FDA_SEARCH_FIELDS = (
    "device.brand_name", "device.generic_name", "device.model_number",
    "device.device_report_product_code", "mdr_text.text",
)


def extract_maude_narrative(item: dict) -> str:
    """Extract narrative text from an openFDA MAUDE device event record."""
    texts: list[str] = []

    mdr_text = item.get("mdr_text", [])
    if isinstance(mdr_text, list):
        for text_block in mdr_text:
            if isinstance(text_block, dict) and text_block.get("text"):
                texts.append(str(text_block["text"]))

    for key in ["event_description", "manufacturer_narrative", "patient_sequence_number"]:
        value = item.get(key)
        if value:
            texts.append(str(value))

    return " ".join(texts).strip()


def build_fda_maude_event_link(mdr_report_key: object | None, report_number: object | None = None) -> str:
    """Build a direct FDA MAUDE detail link, falling back to an API search URL."""
    if mdr_report_key:
        return (
            "https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfmaude/"
            f"detail.cfm?mdrfoi__id={mdr_report_key}"
        )

    if report_number:
        return f'{OPENFDA_DEVICE_EVENT_ENDPOINT}?search=report_number:"{report_number}"&limit=1'

    return ""


def fetch_fda_maude_openfda(
    keywords: Iterable[str],
    years: Iterable[int],
    start_date: object | None = None,
    end_date: object | None = None,
    limit_per_query: int = 100,
    max_skip_per_query: int = 1000,
    request_timeout: int = 60,
    progress_callback: Callable[[int, int, int, str], None] | None = None,
    max_records: int | None = None,
) -> pd.DataFrame:
    """Fetch MAUDE records with explicit pagination and failure accounting.

    The legacy DataFrame return is retained; coverage and per-query counts are
    attached as attrs and propagated by OpenFdaMaudeConnector.
    """
    if not 1 <= int(limit_per_query) <= 1000 or int(max_skip_per_query) < 1:
        raise ValueError("FDA page size must be 1..1000 and query record budget must be positive")
    if max_records is not None and int(max_records) < 1:
        raise ValueError("FDA source record budget must be positive")
    all_rows: list[dict[str, object]] = []
    search_fields = FDA_SEARCH_FIELDS
    warnings: list[str] = []
    query_counts: list[dict[str, object]] = []
    failures = 0
    truncated = False
    unknown_totals = False
    source_budget_exhausted = False
    keywords = list(dict.fromkeys(str(k).strip() for k in keywords if str(k).strip()))
    years = list(dict.fromkeys(int(y) for y in years))
    if not keywords or not years:
        raise ValueError("FDA search requires at least one keyword and one year")
    explicit_start = date.fromisoformat(str(start_date)[:10]).strftime("%Y%m%d") if start_date else None
    explicit_end = date.fromisoformat(str(end_date)[:10]).strftime("%Y%m%d") if end_date else None
    if explicit_start and explicit_end and explicit_start > explicit_end:
        raise ValueError("FDA start date is after end date")
    total_queries = max(1, len(keywords) * len(years) * len(search_fields))
    query_index = 0

    for keyword in keywords:
        if source_budget_exhausted:
            break
        for year in years:
            if source_budget_exhausted:
                break
            start = max(f"{year}0101", explicit_start or f"{year}0101")
            end = min(f"{year}1231", explicit_end or f"{year}1231")
            if start > end:
                continue

            for field in search_fields:
                if source_budget_exhausted:
                    break
                query_index += 1
                escaped = keyword.replace("\\", "\\\\").replace('"', '\\"')
                search_query = f'{field}:"{escaped}" AND date_received:[{start} TO {end}]'
                skip = 0
                available: int | None = None
                seen_pages: set[str] = set()
                if progress_callback:
                    progress_callback(query_index - 1, total_queries, len(all_rows), f"Query {query_index}/{total_queries}: {keyword} in {field}")

                while True:
                    params: dict[str, object] = {
                        "search": search_query,
                        "limit": min(limit_per_query, max_skip_per_query - skip, max_records - len(all_rows) if max_records else limit_per_query),
                        "skip": skip,
                    }
                    if OPENFDA_API_KEY:
                        params["api_key"] = OPENFDA_API_KEY

                    try:
                        response = request_with_retry(
                            requests, "GET",
                            OPENFDA_DEVICE_EVENT_ENDPOINT,
                            params=params,
                            timeout=request_timeout,
                            allowed_statuses=(404,),
                        )
                        if response.status_code == 404:
                            error = response.json().get("error", {})
                            if error.get("code") != "NOT_FOUND" or "no matches" not in str(error.get("message", "")).lower():
                                raise RuntimeError("FDA returned unexpected HTTP 404, not a verified empty query")
                            if skip:
                                raise RuntimeError("FDA returned no-matches after earlier pages; result set changed")
                            available = 0
                            break
                        payload = response.json()
                        if not isinstance(payload, dict) or not isinstance(payload.get("results"), list):
                            raise ValueError("FDA response missing results array")
                        results = payload["results"]
                        if any(not isinstance(item, dict) for item in results):
                            raise ValueError("FDA result entry is not an object")
                        meta = payload.get("meta", {})
                        if not isinstance(meta, dict) or not isinstance(meta.get("results", {}), dict):
                            raise ValueError("FDA pagination metadata has an unexpected shape")
                        total = meta.get("results", {}).get("total")
                        if total is not None:
                            value = int(total)
                            if value < 0 or value < skip + len(results):
                                raise ValueError("FDA reported total contradicts its returned rows")
                            if available is not None and value != available:
                                raise ValueError("FDA reported total changed during pagination")
                            available = value
                        else:
                            unknown_totals = True
                        page_key = hashlib.sha256(json.dumps(results, sort_keys=True).encode()).hexdigest()
                        if results and page_key in seen_pages:
                            raise RuntimeError("FDA repeated a page before pagination finished")
                        seen_pages.add(page_key)
                        normalized_rows = _openfda_results_to_rows(results, field)
                    except (requests.RequestException, RuntimeError, ValueError, TypeError, AttributeError) as exc:
                        failures += 1
                        warnings.append(f"FDA query {query_index} incomplete: {type(exc).__name__}: {exc}")
                        break

                    if not results:
                        if available is not None and skip < available:
                            failures += 1
                            warnings.append(f"FDA query {query_index} ended before its reported total")
                        break

                    all_rows.extend(normalized_rows)

                    fetched = len(results)
                    skip += fetched
                    if max_records and len(all_rows) >= max_records:
                        source_budget_exhausted = True
                        truncated = True
                        warnings.append(f"FDA source retrieval capped at max_records={max_records} query hits before deduplication")
                        break
                    if progress_callback:
                        progress_callback(query_index - 1, total_queries, len(all_rows), f"Fetched {len(all_rows)} rows; query {query_index}/{total_queries}")

                    if available is not None and skip >= available:
                        break
                    if skip >= max_skip_per_query:
                        truncated = True
                        warnings.append(f"FDA query {query_index} capped at {skip}; total={available if available is not None else 'unknown'}")
                        break
                    if fetched < int(params["limit"]):
                        if available is not None and skip < available:
                            failures += 1
                            warnings.append(f"FDA query {query_index} returned a short page before its reported total")
                        break

                    time.sleep(0.25)
                    if progress_callback:
                        progress_callback(query_index - 1, total_queries, len(all_rows), f"Waiting before next page; query {query_index}/{total_queries}")

                if progress_callback:
                    progress_callback(query_index, total_queries, len(all_rows), f"Completed query {query_index}/{total_queries}")
                query_counts.append({"field": field, "keyword": keyword, "year": year, "retrieved": skip, "total_available": available})

    df = deduplicate_fda_maude_rows(pd.DataFrame(all_rows)) if all_rows else pd.DataFrame(columns=UNIFIED_COLUMNS)
    for column in UNIFIED_COLUMNS:
        if column not in df.columns:
            df[column] = None
    df = df[UNIFIED_COLUMNS]
    completeness = "failed" if failures and df.empty else "partial" if failures or truncated else "unverified" if unknown_totals else "complete"
    with_coverage(df, completeness, warnings)
    # Totals for overlapping OR queries cannot be summed into a unique total.
    df.attrs["query_counts"] = query_counts
    return df


def _openfda_range_date(value: object | None, year: int, month: int, day: int) -> str:
    if value:
        text = str(value).strip()
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            return text[:10].replace("-", "")
    return f"{year}{month:02d}{day:02d}"


def _openfda_results_to_rows(results: list[dict], search_field: str = "") -> list[dict[str, object]]:
    """Convert raw openFDA result records to the project unified schema."""
    rows: list[dict[str, object]] = []

    for item in results:
        mdr_report_key = item.get("mdr_report_key")
        report_number = item.get("report_number")
        event_id = mdr_report_key or report_number or item.get("event_key")

        date_of_event = format_fda_date(item.get("date_of_event"))
        date_received = format_fda_date(item.get("date_received"))
        event_date = date_of_event

        brand_names: list[str] = []
        generic_names: list[str] = []
        devices = item.get("device", [])
        if not isinstance(devices, list):
            devices = []
        if isinstance(devices, list):
            for device in devices:
                if isinstance(device, dict):
                    if device.get("brand_name"):
                        brand_names.append(str(device["brand_name"]))
                    if device.get("generic_name"):
                        generic_names.append(str(device["generic_name"]))

        rows.append(
            {
                "source": "FDA_MAUDE",
                "source_type": "adverse_event",
                "country": "United States",
                "event_id": str(event_id) if event_id is not None else None,
                "report_number": str(report_number) if report_number is not None else "",
                "date_of_event": date_of_event,
                "date_received": date_received,
                "report_date": date_received,
                "event_date": event_date,
                "year": parse_year_from_date(event_date),
                "received_year": parse_year_from_date(date_received),
                "product_name": "; ".join(brand_names)[:1000],
                "brand_names": "; ".join(brand_names)[:1000],
                "generic_name": "; ".join(generic_names)[:1000],
                "manufacturer": str(item.get("manufacturer_d_name") or item.get("manufacturer_g1_name") or ""),
                "device_model": "; ".join(str(d.get("model_number", "")) for d in devices if isinstance(d, dict) and d.get("model_number")),
                "product_code": "; ".join(str(d.get("device_report_product_code", "")) for d in devices if isinstance(d, dict) and d.get("device_report_product_code")),
                "event_type": str(item.get("event_type") or ""),
                "device_problem_text": "; ".join(str(v) for v in item.get("device_problem_codes", []) or []),
                "patient_outcome": "; ".join(str(v) for v in item.get("patient_outcome", []) or []),
                "fda_match_field": search_field,
                "fda_match_category": fda_match_category(search_field),
                "narrative_text": extract_maude_narrative(item),
                "raw_link": OPENFDA_DEVICE_EVENT_ENDPOINT,
                "event_link": build_fda_maude_event_link(mdr_report_key, report_number),
                "raw_record": json.dumps(item, ensure_ascii=False, default=str),
                "source_specific": json.dumps({"date_basis": "date_received", "search_field": search_field}, ensure_ascii=False),
            }
        )

    return rows


def deduplicate_fda_maude_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse repeated openFDA query hits for the same MAUDE event."""
    if df.empty:
        return df.copy()

    deduped = df.copy()
    deduped["_maude_dedup_key"] = deduped.apply(_fda_maude_dedup_key, axis=1)
    deduped["_maude_match_priority"] = deduped["fda_match_category"].apply(fda_match_priority)
    deduped = deduped.sort_values("_maude_match_priority", kind="stable")
    return deduped.drop_duplicates(subset=["_maude_dedup_key"], keep="first").drop(
        columns=["_maude_dedup_key", "_maude_match_priority"]
    )


def fda_match_category(search_field: str) -> str:
    """Return the user-facing FDA MAUDE match category for an openFDA search field."""
    mapping = {
        "device.brand_name": "Brand name matches",
        "device.generic_name": "Generic name matches",
        "device.model_number": "Model number matches",
        "device.device_report_product_code": "Product code matches",
        "mdr_text.text": "Narrative matches",
    }
    return mapping.get(str(search_field).strip(), "Other matches")


def fda_match_priority(category: object) -> int:
    """Prioritize device-field matches over narrative matches for duplicate events."""
    priorities = {
        "Model number matches": 0,
        "Product code matches": 1,
        "Brand name matches": 2,
        "Generic name matches": 3,
        "Narrative matches": 4,
    }
    return priorities.get(str(category), 99)


def _fda_maude_dedup_key(row: pd.Series) -> str:
    for column in ["event_id", "report_number", "event_link"]:
        value = str(row.get(column) or "").strip()
        if value:
            return f"{column}:{value}"

    content_key = "|".join(
        str(row.get(column) or "").strip().lower()
        for column in ["event_date", "product_name", "brand_names", "narrative_text"]
    )
    digest = hashlib.sha256(content_key.encode("utf-8")).hexdigest()
    return f"content:{digest}"
