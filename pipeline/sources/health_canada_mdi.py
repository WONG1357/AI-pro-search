"""Health Canada Medical Device Incidents connector."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, replace
from typing import Any
from urllib.parse import urlencode

import pandas as pd
import requests

from pipeline.progress import ProgressReporter
from pipeline.sources.base import BaseSourceConnector, SourceFetchResult, ensure_unified_columns
from pipeline.utils import date_or_year_in_range, format_fda_date, parse_year_from_date
from pipeline.source_support import request_with_retry, with_coverage


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
        config = replace(self.client_config)
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
                max_records=int(kwargs["max_records"]) if kwargs.get("max_records") else None,
                session=kwargs.get("session"),
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
    max_records: int | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Fetch and normalize Health Canada MDI records using OR-style keyword searches."""
    config = client_config or HealthCanadaMdiClientConfig()
    session = session or requests.Session()
    warnings: list[str] = []
    raw_records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    total_terms = max(1, len(keywords))
    failures = 0
    incomplete = False
    unknown_total = False
    page_limit = int(max_pages) if max_pages is not None else 100
    if page_limit < 1 or config.page_size < 1 or (max_records is not None and max_records < 1):
        raise ValueError("Health Canada page and record limits must be positive")

    if progress_reporter:
        progress_reporter.update_source(
            SOURCE_NAME,
            current_step=0,
            total_steps=total_terms,
            stage="Searching Health Canada MDI",
            message=f"Searching {total_terms} terms",
        )

    for term_index, keyword in enumerate(keywords or [""], start=1):
        if max_records and len(raw_records) >= max_records:
            incomplete = True
            warnings.append(f"Health Canada retrieval capped at max_records={max_records}")
            break
        start = 0
        page = 1
        page_size = config.page_size
        total_filtered: int | None = None
        seen_pages: set[str] = set()
        while True:
            payload = build_health_canada_mdi_payload(keyword, start=start, length=page_size)
            try:
                response = request_with_retry(session, "POST", config.server_side_endpoint,
                                              data=payload, headers=_headers(config), timeout=config.timeout)
                data = parse_health_canada_mdi_response(response)
                reported_total = data.get("recordsFiltered")
                if reported_total is not None:
                    if reported_total < start + len(data["data"]):
                        raise ValueError("Health Canada total contradicts its returned rows")
                    if total_filtered is not None and reported_total != total_filtered:
                        raise ValueError("Health Canada total changed during pagination")
            except (requests.RequestException, RuntimeError, ValueError, TypeError) as exc:
                failures += 1
                warnings.append(f"Health Canada keyword={keyword} page={page} failed: {type(exc).__name__}: {exc}")
                break
            if debug:
                logger.debug(
                    "Health Canada MDI keyword=%s page=%s status=%s snippet=%s",
                    keyword,
                    page,
                    response.status_code,
                    response.text[:1000],
                )
            total_filtered = data.get("recordsFiltered", total_filtered)
            if total_filtered is None:
                unknown_total = True
            page_records = data.get("data", [])
            page_key = hashlib.sha256(json.dumps(page_records, sort_keys=True).encode()).hexdigest()
            if page_records and page_key in seen_pages:
                failures += 1
                warnings.append(f"Health Canada repeated page for keyword={keyword}; coverage incomplete")
                break
            seen_pages.add(page_key)
            for raw in page_records:
                incident = raw.get("incident", raw)
                raw_id = incident.get("incident_id")
                incident_id = str(raw_id).strip() if raw_id is not None else ""
                if not incident_id:
                    incident_id = "content:" + hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()
                    unknown_total = True
                    warnings.append("Health Canada record has no incident ID; retained with content-based identity")
                if incident_id not in seen_ids:
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
                if total_filtered is not None and start < int(total_filtered):
                    failures += 1
                    warnings.append("Health Canada returned an empty page before its reported total")
                break
            if max_records and len(raw_records) >= max_records:
                if len(raw_records) > max_records or total_filtered is None or start + len(page_records) < int(total_filtered) or term_index < total_terms:
                    incomplete = True
                    warnings.append(f"Health Canada retrieval capped at max_records={max_records}")
                raw_records = raw_records[:max_records]
                break
            if total_filtered is not None and start + len(page_records) >= int(total_filtered):
                break
            if page >= page_limit:
                incomplete = True
                warnings.append(f"Stopped at max_pages={page_limit} for keyword={keyword}")
                break
            if total_filtered is None and len(page_records) < page_size:
                break
            start += len(page_records)
            page += 1

    search_url = build_health_canada_results_url(keywords, config)
    normalized = []
    for raw in raw_records:
        try:
            normalized.append(normalize_health_canada_mdi_record(raw, search_url=search_url))
        except (AttributeError, TypeError, ValueError) as exc:
            failures += 1
            warnings.append(f"Health Canada malformed record could not be normalized: {type(exc).__name__}")
    df = ensure_unified_columns(pd.DataFrame(normalized))
    if years and not df.empty:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        df = df[df["year"].isin(years)].copy()
    if (start_date is not None or end_date is not None) and not df.empty:
        df = df[df["date_received"].apply(lambda value: date_or_year_in_range(value, start_date, end_date))].copy()
    warnings.append(f"health_canada_mdi_raw_records={len(raw_records)}")
    warnings.append(f"health_canada_mdi_filtered_records={len(df)}")
    coverage = "failed" if failures and df.empty else "partial" if failures or incomplete else "unverified" if unknown_total else "complete"
    with_coverage(df, coverage, warnings, retrieved_count=len(raw_records))
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
    if data.get("error") or "data" not in data or not isinstance(data["data"], list):
        raise RuntimeError("Health Canada response has an error or is missing the data array")
    if any(not isinstance(item, dict) or not isinstance(item.get("incident", item), dict) for item in data["data"]):
        raise RuntimeError("Health Canada incident rows have an unexpected shape")
    if "recordsFiltered" in data:
        try:
            data["recordsFiltered"] = int(data["recordsFiltered"])
        except (TypeError, ValueError):
            raise RuntimeError("Health Canada result count is invalid") from None
        if data["recordsFiltered"] < 0:
            raise RuntimeError("Health Canada result count is negative")
    return data


def normalize_health_canada_mdi_record(raw: dict[str, Any], search_url: str | None = None) -> dict[str, Any]:
    """Normalize one Health Canada MDI raw incident to the common schema."""
    incident = raw.get("incident", raw)
    raw_id = incident.get("incident_id")
    incident_id = str(raw_id).strip() if raw_id is not None else ""
    if not incident_id:
        incident_id = "content:" + hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()
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
        "date_basis": "receipt_date",
    }
    record = {
        "source": SOURCE_NAME,
        "source_type": SOURCE_TYPE,
        "event_id": incident_id,
        "report_number": incident_id,
        "date_of_event": "",
        "date_received": receipt_date,
        "report_date": receipt_date,
        "event_date": "",
        "search_date": receipt_date,
        "search_date_basis": "receipt_date" if receipt_date else "unknown",
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
