"""openFDA MAUDE device event source adapter."""

from __future__ import annotations

import time
from typing import Iterable

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
    limit_per_query: int = 100,
    max_skip_per_query: int = 1000,
    request_timeout: int = 60,
) -> pd.DataFrame:
    """Fetch FDA MAUDE device adverse events from the openFDA device/event API."""
    all_rows: list[dict[str, object]] = []
    search_fields = ["device.brand_name", "device.generic_name", "mdr_text.text"]

    print("Fetching FDA MAUDE records from openFDA...")

    for keyword in keywords:
        for year in years:
            start = f"{year}0101"
            end = f"{year}1231"

            for field in search_fields:
                search_query = f'{field}:"{keyword}" AND date_of_event:[{start} TO {end}]'
                skip = 0
                print(f"[QUERY] keyword={keyword}, year={year}, field={field}")

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

                    all_rows.extend(_openfda_results_to_rows(results))

                    fetched = len(results)
                    skip += fetched
                    print(f"  fetched={fetched}, total_rows_so_far={len(all_rows)}")

                    if skip >= max_skip_per_query or fetched < limit_per_query:
                        break

                    time.sleep(0.25)

    if not all_rows:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    df = pd.DataFrame(all_rows)
    for column in UNIFIED_COLUMNS:
        if column not in df.columns:
            df[column] = None
    return df[UNIFIED_COLUMNS]


def _openfda_results_to_rows(results: list[dict]) -> list[dict[str, object]]:
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
                "event_date": event_date,
                "year": parse_year_from_date(event_date),
                "received_year": parse_year_from_date(date_received),
                "product_name": "; ".join(brand_names)[:1000],
                "brand_names": "; ".join(generic_names)[:1000],
                "narrative_text": extract_maude_narrative(item),
                "raw_link": OPENFDA_DEVICE_EVENT_ENDPOINT,
                "event_link": build_fda_maude_event_link(mdr_report_key, report_number),
            }
        )

    return rows

