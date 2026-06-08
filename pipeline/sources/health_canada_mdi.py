"""Health Canada Medical Device Incidents connector."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import pandas as pd
import requests

from pipeline.progress import ProgressReporter
from pipeline.sources.base import BaseSourceConnector, SourceFetchResult, ensure_unified_columns
from pipeline.utils import date_or_year_in_range, format_fda_date, parse_year_from_date


logger = logging.getLogger(__name__)

SOURCE_NAME = "HEALTH_CANADA_MDI"
SOURCE_TYPE = "regulatory_adverse_event"
COUNTRY = "Canada"


@dataclass
class HealthCanadaMdiClientConfig:
    """HTTP settings for the Health Canada MDI public DataTables endpoint."""

    base_url: str = "https://hpr-rps.hres.ca"
    results_page_url: str = "https://hpr-rps.hres.ca/mdi_results.php"
    server_side_endpoint: str = "https://hpr-rps.hres.ca/mdi_dep/serverSideProcessing.php"
    timeout: float = 60.0
    page_size: int = 100
    user_agent: str = "Mozilla/5.0 (compatible; Codex Health Canada MDI Connector)"


class HealthCanadaMdiConnector(BaseSourceConnector):
    """Fetch Health Canada Medical Device Incident records."""

    source_name = SOURCE_NAME
    source_type = SOURCE_TYPE
    display_name = "Health Canada MDI"

    def __init__(self, client_config: HealthCanadaMdiClientConfig | None = None) -> None:
        self.client_config = client_config or HealthCanadaMdiClientConfig()

    def fetch(
        self,
        keywords: list[str],
        years: list[int],
        components: list[str] | None = None,
        accident_terms: list[str] | None = None,
        progress_reporter: ProgressReporter | None = None,
        **kwargs: Any,
    ) -> SourceFetchResult:
        """Fetch source records from Health Canada's public MDI DataTables API."""
        config = self.client_config
        if kwargs.get("request_timeout"):
            config.timeout = float(kwargs["request_timeout"])
        if kwargs.get("page_size"):
            config.page_size = int(kwargs["page_size"])
        max_pages = kwargs.get("max_pages")
        terms = _merge_terms(keywords, components, accident_terms)

        try:
            records, warnings = fetch_health_canada_mdi_direct(
                keywords=terms,
                years=years,
                start_date=kwargs.get("start_date"),
                end_date=kwargs.get("end_date"),
                client_config=config,
                progress_reporter=progress_reporter,
                max_pages=int(max_pages) if max_pages else None,
                debug=bool(kwargs.get("debug", False)),
            )
            return SourceFetchResult(
                source_name=self.source_name,
                records=ensure_unified_columns(records),
                success=True,
                warnings=warnings,
            )
        except Exception as exc:
            logger.exception("Health Canada MDI fetch failed")
            return SourceFetchResult(
                source_name=self.source_name,
                records=ensure_unified_columns(pd.DataFrame()),
                success=False,
                error_message=f"Health Canada MDI fetch failed: {exc}",
                warnings=[f"Health Canada MDI fetch failed: {exc}"],
            )


def fetch_health_canada_mdi_direct(
    keywords: list[str],
    years: list[int],
    start_date: object | None = None,
    end_date: object | None = None,
    client_config: HealthCanadaMdiClientConfig | None = None,
    session: requests.Session | None = None,
    progress_reporter: ProgressReporter | None = None,
    max_pages: int | None = None,
    debug: bool = False,
) -> tuple[pd.DataFrame, list[str]]:
    """Fetch and normalize Health Canada MDI records using OR-style keyword searches."""
    config = client_config or HealthCanadaMdiClientConfig()
    session = session or requests.Session()
    warnings: list[str] = []
    raw_records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    total_terms = max(1, len(keywords))

    if progress_reporter:
        progress_reporter.update_source(
            SOURCE_NAME,
            current_step=0,
            total_steps=total_terms,
            stage="Searching Health Canada MDI",
            message=f"Searching {total_terms} terms",
        )

    for term_index, keyword in enumerate(keywords or [""], start=1):
        start = 0
        page = 1
        page_size = config.page_size
        total_filtered: int | None = None
        while True:
            payload = build_health_canada_mdi_payload(keyword, start=start, length=page_size)
            response = session.post(
                config.server_side_endpoint,
                data=payload,
                headers=_headers(config),
                timeout=config.timeout,
            )
            if debug:
                logger.debug(
                    "Health Canada MDI keyword=%s page=%s status=%s snippet=%s",
                    keyword,
                    page,
                    response.status_code,
                    response.text[:1000],
                )
            if response.status_code >= 400:
                raise RuntimeError(
                    f"HTTP {response.status_code} for {config.server_side_endpoint}: {response.text[:1000]}"
                )
            data = parse_health_canada_mdi_response(response)
            total_filtered = data.get("recordsFiltered", total_filtered)
            page_records = data.get("data", [])
            for raw in page_records:
                incident = raw.get("incident", raw)
                incident_id = str(incident.get("incident_id", "")).strip()
                if incident_id and incident_id not in seen_ids:
                    raw_records.append(raw)
                    seen_ids.add(incident_id)

            if progress_reporter:
                progress_reporter.update_source(
                    SOURCE_NAME,
                    current_step=term_index,
                    total_steps=total_terms,
                    records_fetched=len(raw_records),
                    stage="Fetching Health Canada MDI",
                    message=f"keyword={keyword} page={page} fetched={len(page_records)} total={len(raw_records)}",
                )

            if not page_records:
                break
            if total_filtered is not None and start + len(page_records) >= int(total_filtered):
                break
            if max_pages and page >= max_pages:
                warnings.append(f"Stopped at max_pages={max_pages} for keyword={keyword}")
                break
            start += page_size
            page += 1

    search_url = build_health_canada_results_url(keywords, config)
    normalized = [normalize_health_canada_mdi_record(raw, search_url=search_url) for raw in raw_records]
    df = ensure_unified_columns(pd.DataFrame(normalized))
    if years and not df.empty:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        df = df[df["year"].isin(years)].copy()
    if (start_date is not None or end_date is not None) and not df.empty:
        df = df[df["event_date"].apply(lambda value: date_or_year_in_range(value, start_date, end_date))].copy()
    warnings.append(f"health_canada_mdi_raw_records={len(raw_records)}")
    warnings.append(f"health_canada_mdi_filtered_records={len(df)}")
    return df, warnings


def build_health_canada_results_url(keywords: list[str], client_config: HealthCanadaMdiClientConfig | None = None) -> str:
    """Build the public Health Canada MDI results page URL for the active query."""
    config = client_config or HealthCanadaMdiClientConfig()
    query = " ".join(str(term).strip() for term in keywords if str(term).strip())
    return f"{config.results_page_url}?{urlencode({'q': query})}" if query else config.results_page_url


def build_health_canada_mdi_payload(keyword: str, start: int = 0, length: int = 100) -> dict[str, str]:
    """Build a DataTables-compatible request payload for Health Canada MDI."""
    payload: dict[str, str] = {
        "draw": "1",
        "start": str(start),
        "length": str(length),
        "term_search": keyword,
        "order[0][column]": "0",
        "order[0][dir]": "desc",
    }
    columns = [
        "incident.incident_id",
        "incident.trade_name",
        "incident.device_desc_e",
        "incident.company_name",
        "incident.hazard_severity_code_e",
        "incident.problem_detail",
        "incident.problem_detail",
        "incident.receipt_date",
    ]
    for idx, column in enumerate(columns):
        payload[f"columns[{idx}][data]"] = column
        payload[f"columns[{idx}][name]"] = ""
        payload[f"columns[{idx}][searchable]"] = "true"
        payload[f"columns[{idx}][orderable]"] = "true"
        payload[f"columns[{idx}][search][value]"] = ""
        payload[f"columns[{idx}][search][regex]"] = "false"
    return payload


def parse_health_canada_mdi_response(response: requests.Response) -> dict[str, Any]:
    """Parse Health Canada MDI DataTables JSON response."""
    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Health Canada MDI returned non-JSON response: {response.text[:1000]}") from exc
    if not isinstance(data, dict):
        raise RuntimeError("Health Canada MDI response was not a JSON object")
    data.setdefault("data", [])
    return data


def normalize_health_canada_mdi_record(raw: dict[str, Any], search_url: str | None = None) -> dict[str, Any]:
    """Normalize one Health Canada MDI raw incident to the common schema."""
    incident = raw.get("incident", raw)
    incident_id = str(incident.get("incident_id", "")).strip()
    trade_names = _list_text(incident.get("trade_name"))
    companies = _list_text(incident.get("company_name"))
    device_desc = _list_text(incident.get("device_desc_e"))
    receipt_date = format_fda_date(incident.get("receipt_date"))
    problem_details = incident.get("problem_detail") or []
    if not isinstance(problem_details, list):
        problem_details = []
    device_problems = [
        str(item.get("desc_e", ""))
        for item in problem_details
        if str(item.get("code_type_e", "")).lower() == "medical device problem"
    ]
    health_effects = [
        str(item.get("desc_e", ""))
        for item in problem_details
        if str(item.get("code_type_e", "")).lower() == "health effect"
    ]
    all_problem_text = [str(item.get("desc_e", "")) for item in problem_details if item.get("desc_e")]
    narrative = "; ".join([*trade_names, *device_desc, *all_problem_text])
    source_specific = {
        "hazard_severity_code_e": incident.get("hazard_severity_code_e"),
        "mandatory_rt": incident.get("mandatory_rt"),
        "device_detail": incident.get("device_detail"),
        "company_detail": incident.get("company_detail"),
        "problem_detail": problem_details,
        "results_page_url": search_url,
    }
    record = {
        "source": SOURCE_NAME,
        "source_type": SOURCE_TYPE,
        "event_id": incident_id,
        "report_number": incident_id,
        "date_of_event": "",
        "date_received": receipt_date,
        "event_date": receipt_date,
        "year": parse_year_from_date(receipt_date),
        "received_year": parse_year_from_date(receipt_date),
        "country": COUNTRY,
        "manufacturer": "; ".join(companies),
        "product_name": "; ".join(trade_names),
        "brand_names": "; ".join(trade_names),
        "generic_name": "; ".join(device_desc),
        "device_model": "",
        "product_code": _first_device_code(incident.get("device_detail")),
        "event_type": str(incident.get("hazard_severity_code_e", "") or ""),
        "patient_outcome": "; ".join(health_effects),
        "device_problem_text": "; ".join(device_problems),
        "patient_problem_text": "; ".join(health_effects),
        "narrative_text": narrative,
        "raw_link": search_url or "",
        "event_link": search_url or "",
        "record_hash": "",
        "source_specific": json.dumps(source_specific, ensure_ascii=False, default=str),
        "raw_record": json.dumps(raw, ensure_ascii=False, default=str),
    }
    record["record_hash"] = _record_hash(record)
    return record


def _merge_terms(
    keywords: list[str],
    components: list[str] | None,
    accident_terms: list[str] | None,
) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    for value in [*keywords, *(components or []), *(accident_terms or [])]:
        term = str(value).strip()
        key = term.lower()
        if term and key not in seen:
            terms.append(term)
            seen.add(key)
    return terms


def _list_text(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def _first_device_code(device_detail: Any) -> str:
    if isinstance(device_detail, list) and device_detail:
        first = device_detail[0]
        if isinstance(first, dict):
            return str(first.get("pref_name_code", "") or "")
    return ""


def _record_hash(record: dict[str, Any]) -> str:
    basis = "|".join(
        [
            str(record.get("source", "")),
            str(record.get("event_id", "")),
            str(record.get("event_date", "")),
            str(record.get("product_name", "")),
            str(record.get("narrative_text", ""))[:300],
        ]
    )
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def _headers(config: HealthCanadaMdiClientConfig) -> dict[str, str]:
    return {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "User-Agent": config.user_agent,
        "X-Requested-With": "XMLHttpRequest",
        "Referer": config.results_page_url,
    }
