"""openFDA MAUDE device event source adapter."""

from __future__ import annotations

import hashlib
import time
from typing import Callable, Iterable

import pandas as pd
import requests

from pipeline.config import OPENFDA_API_KEY, OPENFDA_DEVICE_EVENT_ENDPOINT, UNIFIED_COLUMNS
from pipeline.utils import format_fda_date, parse_year_from_date


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
) -> pd.DataFrame:
    """Fetch FDA MAUDE device adverse events from the openFDA device/event API."""
    all_rows: list[dict[str, object]] = []
    search_fields = ["device.brand_name", "device.generic_name", "mdr_text.text"]

    print("Fetching FDA MAUDE records from openFDA...")

    keywords = list(keywords)
    years = list(years)
    total_queries = max(1, len(keywords) * len(years) * len(search_fields))
    query_index = 0

    for keyword in keywords:
        for year in years:
            start = _openfda_range_date(start_date, year, 1, 1)
            end = _openfda_range_date(end_date, year, 12, 31)
            if start[:4] != str(year):
                start = f"{year}0101"
            if end[:4] != str(year):
                end = f"{year}1231"
            if start > end:
                continue

            for field in search_fields:
                query_index += 1
                search_query = f'{field}:"{keyword}" AND date_received:[{start} TO {end}]'
                skip = 0
                print(f"[QUERY] keyword={keyword}, year={year}, field={field}")
                if progress_callback:
                    progress_callback(query_index - 1, total_queries, len(all_rows), f"Query {query_index}/{total_queries}: {keyword} in {field}")

                while True:
                    params: dict[str, object] = {
                        "search": search_query,
                        "limit": limit_per_query,
                        "skip": skip,
                    }
                    if OPENFDA_API_KEY:
                        params["api_key"] = OPENFDA_API_KEY

                    try:
                        response = requests.get(
                            OPENFDA_DEVICE_EVENT_ENDPOINT,
                            params=params,
                            timeout=request_timeout,
                        )
                        if response.status_code == 404:
                            break
                        if response.status_code >= 500:
                            print(
                                "[WARN] openFDA server error "
                                f"{response.status_code}: {response.text[:250]}"
                            )
                            break

                        response.raise_for_status()
                        results = response.json().get("results", [])
                    except requests.RequestException as exc:
                        print(f"[WARN] openFDA request failed: {exc}")
                        break

                    if not results:
                        break

                    all_rows.extend(_openfda_results_to_rows(results, field))

                    fetched = len(results)
                    skip += fetched
                    print(f"  fetched={fetched}, total_rows_so_far={len(all_rows)}")
                    if progress_callback:
                        progress_callback(query_index - 1, total_queries, len(all_rows), f"Fetched {len(all_rows)} rows; query {query_index}/{total_queries}")

                    if skip >= max_skip_per_query or fetched < limit_per_query:
                        break

                    time.sleep(0.25)
                    if progress_callback:
                        progress_callback(query_index - 1, total_queries, len(all_rows), f"Waiting before next page; query {query_index}/{total_queries}")

                if progress_callback:
                    progress_callback(query_index, total_queries, len(all_rows), f"Completed query {query_index}/{total_queries}")

    if not all_rows:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    df = deduplicate_fda_maude_rows(pd.DataFrame(all_rows))
    removed_count = len(all_rows) - len(df)
    if removed_count:
        print(f"[INFO] Removed {removed_count:,} duplicate FDA MAUDE query hits.")
    for column in UNIFIED_COLUMNS:
        if column not in df.columns:
            df[column] = None
    return df[UNIFIED_COLUMNS]


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
        event_date = date_of_event or date_received

        brand_names: list[str] = []
        generic_names: list[str] = []
        devices = item.get("device", [])
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
                "event_id": str(event_id) if event_id is not None else None,
                "report_number": str(report_number) if report_number is not None else "",
                "date_of_event": date_of_event,
                "date_received": date_received,
                "report_date": date_received,
                "event_date": event_date,
                "year": parse_year_from_date(event_date),
                "received_year": parse_year_from_date(date_received),
                "product_name": "; ".join(brand_names)[:1000],
                "brand_names": "; ".join(generic_names)[:1000],
                "fda_match_field": search_field,
                "fda_match_category": fda_match_category(search_field),
                "narrative_text": extract_maude_narrative(item),
                "raw_link": OPENFDA_DEVICE_EVENT_ENDPOINT,
                "event_link": build_fda_maude_event_link(mdr_report_key, report_number),
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
        "mdr_text.text": "Narrative matches",
    }
    return mapping.get(str(search_field).strip(), "Other matches")


def fda_match_priority(category: object) -> int:
    """Prioritize device-field matches over narrative matches for duplicate events."""
    priorities = {
        "Brand name matches": 0,
        "Generic name matches": 1,
        "Narrative matches": 2,
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
