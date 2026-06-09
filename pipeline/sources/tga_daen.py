"""Direct TGA DAEN connector with print-report parsing."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from html import unescape
from io import BytesIO
from typing import Any

import pandas as pd
import requests

from pipeline.config import (
    TGA_DAEN_BASE_URL,
    TGA_DAEN_DEBUG,
    TGA_DAEN_DEFAULT_HEADERS,
    TGA_DAEN_DEVICE_SEARCH_ENDPOINT,
    TGA_DAEN_PAGE_SIZE,
    TGA_DAEN_REPORT_SEARCH_ENDPOINT,
    TGA_DAEN_SEARCH_ENDPOINT,
    TGA_DAEN_TIMEOUT,
    TGA_DAEN_USER_AGENT,
)
from pipeline.progress import ProgressReporter
from pipeline.sources.base import BaseSourceConnector, SourceFetchResult, ensure_unified_columns
from pipeline.utils import date_or_year_in_range, format_fda_date, parse_year_from_date

try:  # optional dependency, only needed for PDF print reports
    from pypdf import PdfReader
except Exception:  # pragma: no cover - dependency may be absent in some environments
    PdfReader = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)

TGA_DAEN_SOURCE_NAME = "TGA_DAEN"
TGA_DAEN_SOURCE_TYPE = "regulatory_adverse_event"
TGA_DAEN_COUNTRY = "Australia"


@dataclass
class TgaDaenClientConfig:
    """Configuration for the TGA DAEN HTTP client."""

    base_url: str = TGA_DAEN_BASE_URL
    search_page_url: str = TGA_DAEN_SEARCH_ENDPOINT
    device_search_endpoint: str = TGA_DAEN_DEVICE_SEARCH_ENDPOINT
    report_search_endpoint: str = TGA_DAEN_REPORT_SEARCH_ENDPOINT
    timeout: float = TGA_DAEN_TIMEOUT
    page_size: int = TGA_DAEN_PAGE_SIZE
    user_agent: str = TGA_DAEN_USER_AGENT
    default_headers: dict[str, str] | None = None
    debug: bool = TGA_DAEN_DEBUG

    def __post_init__(self) -> None:
        if not self.base_url:
            self.base_url = TGA_DAEN_BASE_URL
        if not self.search_page_url:
            self.search_page_url = TGA_DAEN_SEARCH_ENDPOINT
        if not self.device_search_endpoint:
            self.device_search_endpoint = TGA_DAEN_DEVICE_SEARCH_ENDPOINT
        if not self.report_search_endpoint:
            self.report_search_endpoint = TGA_DAEN_REPORT_SEARCH_ENDPOINT


class TgaDaenConnector(BaseSourceConnector):
    """Direct online connector for TGA DAEN adverse event records."""

    source_name = TGA_DAEN_SOURCE_NAME
    source_type = TGA_DAEN_SOURCE_TYPE
    display_name = "TGA DAEN"

    def __init__(self, client_config: TgaDaenClientConfig | None = None) -> None:
        self.client_config = client_config or TgaDaenClientConfig()

    def fetch(
        self,
        keywords: list[str],
        years: list[int],
        components: list[str] | None = None,
        accident_terms: list[str] | None = None,
        progress_reporter: ProgressReporter | None = None,
        **kwargs: Any,
    ) -> SourceFetchResult:
        """Fetch TGA DAEN records directly from the online print-report workflow."""
        debug = bool(kwargs.get("debug", self.client_config.debug))
        session = kwargs.get("session") or requests.Session()
        request_timeout = kwargs.get("request_timeout")
        max_pages = kwargs.get("max_pages")
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        csv_path = kwargs.get("csv_path") or kwargs.get("tga_daen_csv_path")

        try:
            if csv_path:
                records = load_tga_daen_csv(csv_path)
                filtered = _filter_records(records, _merge_terms(keywords, components, accident_terms), years)
                filtered = _filter_by_date_range(filtered, start_date, end_date)
                return SourceFetchResult(
                    source_name=self.source_name,
                    records=ensure_unified_columns(filtered),
                    success=True,
                    warnings=["Using explicit TGA DAEN CSV fallback import; direct online access was not used."],
                )

            if request_timeout:
                self.client_config.timeout = float(request_timeout)
            try:
                start_text, end_text = build_tga_date_range(
                    years,
                    explicit_start_date=start_date,
                    explicit_end_date=end_date,
                )
                result = fetch_tga_daen_direct(
                    keywords=keywords,
                    years=years,
                    start_date=start_text,
                    end_date=end_text,
                    components=components,
                    accident_terms=accident_terms,
                    client_config=self.client_config,
                    session=session,
                    progress_reporter=progress_reporter,
                    debug=debug,
                    max_pages=max_pages,
                )
            finally:
                if request_timeout:
                    self.client_config.timeout = TGA_DAEN_TIMEOUT

            return SourceFetchResult(
                source_name=self.source_name,
                records=ensure_unified_columns(result["records"]),
                success=True,
                warnings=result.get("warnings", []),
            )
        except Exception as exc:
            logger.exception("TGA DAEN fetch failed")
            message = str(exc)
            if "TGA DAEN direct fetch failed" not in message:
                message = f"TGA DAEN direct fetch failed. error={message}"
            if progress_reporter:
                progress_reporter.fail_source(self.source_name, message)
            return SourceFetchResult(
                source_name=self.source_name,
                records=ensure_unified_columns(pd.DataFrame()),
                success=False,
                error_message=message,
                warnings=[f"TGA DAEN fetch failed: {message}"],
            )


def build_tga_daen_query(
    keywords: list[str],
    years: list[int],
    start_date: object | None = None,
    end_date: object | None = None,
    components: list[str] | None = None,
    accident_terms: list[str] | None = None,
    client_config: TgaDaenClientConfig | None = None,
) -> dict[str, Any]:
    """Build a device-search request payload."""
    config = client_config or TgaDaenClientConfig()
    terms = _merge_terms(keywords, components, accident_terms)
    start_date, end_date = build_tga_date_range(years, explicit_start_date=start_date, explicit_end_date=end_date)
    return {
        "url": config.device_search_endpoint,
        "method": "POST",
        "headers": _build_headers(config, json_body=True),
        "json": {"prefix": " OR ".join(terms)},
        "params": None,
        "start_date": start_date,
        "end_date": end_date,
        "keywords": terms,
    }


def build_tga_date_range(
    years: list[int],
    explicit_start_date: str | date | None = None,
    explicit_end_date: str | date | None = None,
) -> tuple[str, str]:
    """Build a DAEN-compatible date range from years or explicit dates."""
    if explicit_start_date:
        start = _normalize_date(explicit_start_date)
    else:
        start = f"{min(years)}-01-01" if years else date.today().replace(month=1, day=1).isoformat()

    allowed_end = _daen_latest_allowed_date()
    if explicit_end_date:
        end = _normalize_date(explicit_end_date)
        if end > allowed_end:
            end = allowed_end
    else:
        latest_year = max(years) if years else date.today().year
        end = date(latest_year, 12, 31).isoformat()
        if end > allowed_end:
            end = allowed_end

    if start > end:
        raise ValueError(f"Invalid TGA date range: start={start} end={end}")
    return start, end


def _daen_latest_allowed_date() -> str:
    """Mirror the visible DAEN cap: first Thursday of the current month minus three months."""
    today = date.today()
    current_month_first = date(today.year, today.month, 1)
    first_thursday = current_month_first
    while first_thursday.weekday() != 3:
        first_thursday = first_thursday + timedelta(days=1)
    if first_thursday > today:
        prev_year, prev_month = _previous_month(today.year, today.month)
        first_thursday = date(prev_year, prev_month, 1)
        while first_thursday.weekday() != 3:
            first_thursday = first_thursday + timedelta(days=1)
    allowed = _shift_months(first_thursday, -3)
    return max(allowed, date(2013, 3, 27)).isoformat()


def _previous_month(year: int, month: int) -> tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def _shift_months(dt: date, months: int) -> date:
    year = dt.year + (dt.month - 1 + months) // 12
    month = (dt.month - 1 + months) % 12 + 1
    day = min(dt.day, _days_in_month(year, month))
    return date(year, month, day)


def _days_in_month(year: int, month: int) -> int:
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    return (next_month - timedelta(days=1)).day


def search_tga_devices(
    keyword: str,
    session: requests.Session | None = None,
    debug: bool = False,
    client_config: TgaDaenClientConfig | None = None,
    progress_reporter: ProgressReporter | None = None,
) -> dict[str, Any]:
    """Search the TGA DAEN device autocomplete endpoint."""
    config = client_config or TgaDaenClientConfig()
    http = session or requests.Session()
    response = _request(
        http,
        "POST",
        config.device_search_endpoint,
        json={"prefix": keyword},
        headers=_build_headers(config, json_body=True),
        timeout=config.timeout,
        debug=debug,
        label="TGA device search",
    )
    parsed = parse_tga_device_response(response)
    if progress_reporter:
        progress_reporter.update_source(
            TGA_DAEN_SOURCE_NAME,
            stage="Searching devices",
            message=f"keyword={keyword} found {len(parsed['records'])} device matches",
            records_fetched=len(parsed["records"]),
        )
    return {
        "records": parsed["records"],
        "selected_device_count": len(parsed["records"]),
        "debug": parsed["debug"],
        "raw": parsed["raw"],
    }


def extract_tga_device_options(response: requests.Response) -> list[dict[str, Any]]:
    """Extract device options from a TGA device search response."""
    return parse_tga_device_response(response)["records"]


def submit_tga_daen_search(
    selected_device_ids: list[str],
    start_date: date,
    end_date: date,
    session: requests.Session,
    debug: bool = False,
    client_config: TgaDaenClientConfig | None = None,
) -> requests.Response:
    """Submit selected device IDs and date range to DAEN."""
    config = client_config or TgaDaenClientConfig()
    payload = {
        "m": selected_device_ids,
        "medicine-name": "",
        "start-year": str(start_date.year),
        "start-month": str(start_date.month - 1),
        "start-day": str(start_date.day),
        "startDate": start_date.isoformat(),
        "end-year": str(end_date.year),
        "end-month": str(end_date.month - 1),
        "end-day": str(end_date.day),
        "endDate": end_date.isoformat(),
        "selectedDevices": selected_device_ids,
        "page": 1,
        "pageSize": config.page_size,
    }
    return _request(
        session,
        "POST",
        config.report_search_endpoint,
        data=payload,
        headers=_build_headers(config, form=True),
        timeout=config.timeout,
        debug=debug,
        label="TGA report search",
    )


def parse_tga_search_result_counts(html: str) -> dict[str, Any]:
    """Parse counts from a DAEN result page."""
    text = unescape(_strip_tags(html))
    selected_match = re.search(r"(\d+)\s+medical devices selected.*?(\d{2}/\d{2}/\d{4}).*?-\s*(\d{2}/\d{2}/\d{4})", text, re.S | re.I)
    reports_match = re.search(r"Number of reports:\s*(\d+)", text, re.I)
    return {
        "selected_devices_count": int(selected_match.group(1)) if selected_match else None,
        "expected_report_count": int(reports_match.group(1)) if reports_match else None,
        "search_start_date": _to_iso_date(selected_match.group(2)) if selected_match else None,
        "search_end_date": _to_iso_date(selected_match.group(3)) if selected_match else None,
    }


def find_tga_list_of_reports_url(html: str, base_url: str) -> str | None:
    """Find the URL for the list-of-reports view if present."""
    return _find_link_by_text(html, base_url, ["list of reports"], prefer_javascript=True)


def find_tga_print_version_url(html: str, base_url: str) -> str | None:
    """Find the URL for the print version of the DAEN report."""
    return _find_link_by_text(html, base_url, ["print version of this report", "print version", "print"])


def fetch_tga_print_report(
    print_url: str,
    session: requests.Session,
    debug: bool = False,
    client_config: TgaDaenClientConfig | None = None,
    source_html: str | None = None,
    source_url: str | None = None,
) -> tuple[bytes, str, str]:
    """Fetch the print report, returning bytes and content type."""
    config = client_config or TgaDaenClientConfig()
    if print_url.lower().startswith("javascript:"):
        if not source_html or not source_url:
            raise RuntimeError("TGA DAEN print link requires ASP.NET postback context but source HTML was not provided")
        response = _submit_tga_postback(
            session=session,
            page_html=source_html,
            page_url=source_url,
            javascript_href=print_url,
            config=config,
            debug=debug,
        )
        content_type = response.headers.get("Content-Type", "").lower()
        return response.content, content_type, response.url

    response = _request(
        session,
        "GET",
        print_url,
        headers=_build_headers(config),
        timeout=config.timeout,
        debug=debug,
        label="TGA print report",
    )
    content_type = response.headers.get("Content-Type", "").lower()
    return response.content, content_type, response.url


def extract_text_from_tga_pdf(pdf_bytes: bytes) -> str:
    """Extract text from a DAEN PDF using pypdf."""
    if PdfReader is None:
        raise RuntimeError("pypdf is required to parse TGA DAEN PDF print reports")
    reader = PdfReader(BytesIO(pdf_bytes))
    pages = []
    for page in reader.pages:
        pages.append(page.extract_text() or "")
    return "\n".join(pages)


def parse_tga_print_report_text(text: str, debug: bool = False) -> list[dict[str, Any]]:
    """Parse adverse-event rows from extracted print-report text."""
    if debug:
        logger.debug("Parsing TGA print report text length=%s", len(text))
    records = _parse_daen_report_blocks(text)
    if records:
        return records
    records = _parse_daen_markdown_rows(text)
    if records:
        return records
    return []


def parse_tga_print_report_html(html: str, debug: bool = False) -> list[dict[str, Any]]:
    """Parse adverse-event rows from a print-report HTML response."""
    if debug:
        logger.debug("Parsing TGA print report HTML length=%s", len(html))
    summary_records = parse_tga_report_summary_html(html)
    if summary_records:
        return summary_records
    tables = _extract_html_tables(html)
    records: list[dict[str, Any]] = []
    for table in tables:
        headers = [cell.lower().strip() for cell in table["headers"]]
        if not headers:
            continue
        if any("report" in header for header in headers) and any("description" in header for header in headers):
            for row in table["rows"]:
                record = {headers[idx]: row[idx] if idx < len(row) else "" for idx in range(len(headers))}
                records.append(_normalize_report_row(record))
    if records:
        return records
    return _parse_daen_report_blocks(html) or _parse_daen_markdown_rows(html)


def parse_tga_report_summary_html(html: str, base_url: str = "") -> list[dict[str, Any]]:
    """Parse the DAEN list-of-reports summary table."""
    records: list[dict[str, Any]] = []
    for table in _extract_html_tables_with_html(html):
        headers = [_normalize_tga_report_header(_html_cell_text(cell)) for cell in table["headers"]]
        required = {"report_number", "report_date", "trade_name"}
        if not required.issubset(set(headers)):
            continue
        if {"event_description", "outcome"}.intersection(headers):
            continue
        for row_html in table["rows"]:
            row = [_html_cell_text(cell) for cell in row_html]
            raw = {headers[idx]: row[idx] if idx < len(row) else "" for idx in range(len(headers))}
            report_cell = row_html[headers.index("report_number")] if "report_number" in headers and headers.index("report_number") < len(row_html) else ""
            report_link = _extract_first_href(report_cell, base_url)
            records.append(
                _normalize_report_row(
                    {
                        "report_number": raw.get("report_number", ""),
                        "report_date": raw.get("report_date", ""),
                        "trade_name": raw.get("trade_name", ""),
                        "manufacturer": raw.get("manufacturer", ""),
                        "sponsor": raw.get("sponsor", ""),
                        "artg_number": raw.get("artg_number", ""),
                        "gmdn_term": raw.get("gmdn_term", ""),
                        "event_description": raw.get("gmdn_term", "") or raw.get("trade_name", ""),
                        "outcome": "",
                        "event_link": "",
                    }
                )
            )
    return records


def normalize_tga_daen_report(raw_report: dict[str, Any], metadata: dict[str, Any]) -> dict[str, Any]:
    """Normalize one TGA DAEN report row to the unified schema."""
    report_number = _first_value(raw_report, ["report_number", "report #", "report#", "report", "case_number", "event_id"])
    report_date = format_fda_date(_first_value(raw_report, ["date", "report_date", "event_date"]))
    trade_name = _first_value(raw_report, ["trade_name", "product_name", "product", "device", "name"])
    model_ref = _first_value(raw_report, ["model/ref", "model_ref", "model", "device_model"])
    event_description = _first_value(raw_report, ["event description", "event_description", "description", "summary", "narrative_text"])
    outcome = _first_value(raw_report, ["outcome", "event_type", "patient_outcome"])
    manufacturer = _first_value(raw_report, ["manufacturer", "sponsor", "company"]) or metadata.get("manufacturer", "")
    product_code = _first_value(raw_report, ["artg_number", "product_code", "artg", "code"])
    generic_name = _first_value(raw_report, ["gmdn_term", "generic_name", "gmdn"])
    raw_link = metadata.get("raw_link", "")
    event_link = metadata.get("event_link", raw_link)

    normalized = {
        "source": TGA_DAEN_SOURCE_NAME,
        "source_type": TGA_DAEN_SOURCE_TYPE,
        "country": TGA_DAEN_COUNTRY,
        "event_id": report_number,
        "report_number": report_number,
        "date_of_event": report_date,
        "date_received": "",
        "event_date": report_date,
        "year": parse_year_from_date(report_date),
        "received_year": None,
        "manufacturer": manufacturer,
        "product_name": trade_name,
        "brand_names": trade_name,
        "generic_name": generic_name,
        "device_model": model_ref,
        "product_code": product_code,
        "event_type": outcome,
        "patient_outcome": outcome,
        "device_problem_text": event_description,
        "patient_problem_text": "",
        "narrative_text": event_description,
        "raw_link": raw_link,
        "event_link": event_link,
        "record_hash": "",
    }
    normalized["record_hash"] = _record_hash(normalized)
    return normalized


def load_tga_daen_csv(csv_path: str | Any) -> pd.DataFrame:
    """Optional fallback/debug CSV loader for TGA DAEN exports."""
    raw = pd.read_csv(csv_path)
    normalized = [normalize_tga_daen_record(row.to_dict()) for _, row in raw.iterrows()]
    return ensure_unified_columns(pd.DataFrame(normalized))


def fetch_tga_daen_direct(
    keywords: list[str],
    years: list[int] | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    components: list[str] | None = None,
    accident_terms: list[str] | None = None,
    client_config: TgaDaenClientConfig | None = None,
    session: requests.Session | None = None,
    debug: bool = False,
    progress_reporter: ProgressReporter | None = None,
    max_reports: int | None = None,
    max_pages: int | None = None,
) -> dict[str, Any]:
    """Fetch TGA DAEN records using device search followed by print report parsing."""
    config = client_config or TgaDaenClientConfig()
    session = session or requests.Session()
    warnings: list[str] = []
    terms = _merge_terms(keywords, components, accident_terms)
    years = years or []
    if start_date is None or end_date is None:
        start_text, end_text = build_tga_date_range(years)
    else:
        start_text = _normalize_date(start_date)
        end_text = _normalize_date(end_date)
        if start_text > end_text:
            raise ValueError(f"Invalid TGA date range: start={start_text} end={end_text}")

    start_dt = date.fromisoformat(start_text)
    end_dt = date.fromisoformat(end_text)
    if progress_reporter:
        progress_reporter.update_source(
            TGA_DAEN_SOURCE_NAME,
            stage="Initializing",
            message=f"date_range={start_text} to {end_text}",
        )

    _initialize_tga_session(session, config, debug=debug)

    device_matches: list[dict[str, Any]] = []
    seen_device_ids: set[str] = set()
    for keyword in terms:
        if progress_reporter:
            progress_reporter.update_source(
                TGA_DAEN_SOURCE_NAME,
                stage="Searching devices",
                message=f"TGA_DAEN searching devices for keyword='{keyword}'",
            )
        search_result = search_tga_devices(
            keyword,
            session=session,
            debug=debug,
            client_config=config,
            progress_reporter=progress_reporter,
        )
        warnings.append(f"device_search keyword={keyword} matches={search_result['selected_device_count']}")
        for record in search_result["records"]:
            device_key = str(record.get("HashKey") or record.get("Key") or record.get("SelectedValue") or "").strip()
            if device_key and device_key not in seen_device_ids:
                device_matches.append(record)
                seen_device_ids.add(device_key)

    if not device_matches:
        warnings.append(
            f"TGA DAEN found 0 device matches for keywords={keywords} date_range={start_text} to {end_text}"
        )
        empty_df = ensure_unified_columns(pd.DataFrame())
        return {"records": empty_df, "warnings": warnings, "metadata": {}, "raw_text": ""}

    selected_device_ids = _extract_device_ids(device_matches)
    if progress_reporter:
        progress_reporter.update_source(
            TGA_DAEN_SOURCE_NAME,
            stage="Submitting report search",
            message=f"TGA_DAEN found {len(selected_device_ids)} selected devices",
            records_fetched=len(selected_device_ids),
        )

    search_response = submit_tga_daen_search(
        selected_device_ids,
        start_dt,
        end_dt,
        session=session,
        debug=debug,
        client_config=config,
    )
    if search_response.status_code >= 400:
        raise RuntimeError(
            f"TGA DAEN report search failed status={search_response.status_code} url={search_response.url} "
            f"snippet={(search_response.text or '')[:1000]}"
        )

    search_counts = parse_tga_search_result_counts(search_response.text or "")
    warnings.append(f"selected_devices_count={search_counts.get('selected_devices_count')}")
    warnings.append(f"expected_report_count={search_counts.get('expected_report_count')}")

    list_url = find_tga_list_of_reports_url(search_response.text or "", search_response.url)
    print_url = find_tga_print_version_url(search_response.text or "", search_response.url)
    report_source_url = search_response.url
    summary_rows = parse_tga_report_summary_html(search_response.text or "", search_response.url)
    search_has_report_rows = bool(summary_rows)

    if progress_reporter:
        progress_reporter.update_source(
            TGA_DAEN_SOURCE_NAME,
            stage="Fetching print report",
            message=f"TGA_DAEN parsing report list from {report_source_url}",
        )

    if search_has_report_rows:
        report_content = (search_response.text or "").encode("utf-8")
        content_type = search_response.headers.get("Content-Type", "text/html").lower()
        final_url = search_response.url
    else:
        report_source = print_url or list_url or search_response.url
        if report_source.lower().startswith("javascript:"):
            report_content, content_type, final_url = fetch_tga_print_report(
                report_source,
                session=session,
                debug=debug,
                client_config=config,
                source_html=search_response.text or "",
                source_url=search_response.url,
            )
        else:
            report_content, content_type, final_url = fetch_tga_print_report(
                report_source,
                session=session,
                debug=debug,
                client_config=config,
                source_html=search_response.text or "",
                source_url=search_response.url,
            )
    warnings.append(f"print_report_url={final_url}")
    warnings.append(f"content_type={content_type}")

    if not report_content:
        raise RuntimeError(f"TGA DAEN print report fetch returned empty content for url={report_source_url}")

    if progress_reporter:
        progress_reporter.update_source(
            TGA_DAEN_SOURCE_NAME,
            stage="Extracting report content",
            message=f"TGA_DAEN downloaded print report {len(report_content):,} bytes",
        )

    raw_rows, extracted_text = _fetch_tga_report_rows(
        session=session,
        report_response_content=report_content,
        content_type=content_type,
        report_url=final_url,
        config=config,
        debug=debug,
        progress_reporter=progress_reporter,
        max_pages=max_pages,
        max_records=None,
    )
    if extracted_text and "pdf" in content_type:
        warnings.append(f"pdf_text_length={len(extracted_text)}")

    if progress_reporter:
        progress_reporter.update_source(
            TGA_DAEN_SOURCE_NAME,
            stage="Parsing report rows",
            message=f"TGA_DAEN parsing report records",
        )

    if not raw_rows:
        warnings.append(
            f"TGA DAEN found {len(selected_device_ids)} selected devices but returned 0 reports for date range {start_text} to {end_text}"
        )
        empty_df = ensure_unified_columns(pd.DataFrame())
        metadata = {
            "raw_link": "",
            "event_link": "",
            "print_report_link": final_url,
            "search_start_date": start_text,
            "search_end_date": end_text,
            "selected_devices_count": search_counts.get("selected_devices_count"),
            "expected_report_count": search_counts.get("expected_report_count"),
            "report_generation_date": _find_report_generation_date(extracted_text or ""),
        }
        return {"records": empty_df, "warnings": warnings, "metadata": metadata, "raw_text": extracted_text or ""}

    if raw_rows and not any(row.get("report_number") for row in raw_rows):
        raise RuntimeError(
            "TGA_DAEN appears to be parsing the selected device list instead of report records. "
            f"Detected {len(selected_device_ids)} repeated device rows and 0 report numbers. "
            "Please check List of reports / Print version URL parsing."
        )

    metadata = {
        "raw_link": "",
        "event_link": "",
        "print_report_link": final_url,
        "search_start_date": start_text,
        "search_end_date": end_text,
        "selected_devices_count": search_counts.get("selected_devices_count"),
        "expected_report_count": search_counts.get("expected_report_count"),
        "report_generation_date": _find_report_generation_date(extracted_text or ""),
    }
    normalized_rows = [normalize_tga_daen_report(row, metadata) for row in raw_rows]
    df = ensure_unified_columns(pd.DataFrame(normalized_rows))
    if not df.empty:
        df["source_query_match"] = True
        df["source_query_terms"] = ", ".join(terms)
        df["selected_devices_count"] = search_counts.get("selected_devices_count") or len(selected_device_ids)
        df["expected_report_count"] = search_counts.get("expected_report_count")
    if "report_number" in df.columns:
        df = df[df["report_number"].astype(str).str.strip() != ""].copy()
        df = df.drop_duplicates(subset=["report_number"], keep="first")

    if max_reports and len(df) > max_reports:
        df = df.head(max_reports).copy()
        warnings.append(f"Stopped at max_reports={max_reports}")
    if max_pages and extracted_text:
        warnings.append(f"max_pages={max_pages}")
    if (start_date is not None or end_date is not None) and not df.empty:
        df = _filter_by_date_range(df, start_date, end_date)

    parsed_count = len(df)
    expected_count = search_counts.get("expected_report_count")
    if progress_reporter:
        progress_reporter.update_source(
            TGA_DAEN_SOURCE_NAME,
            current_step=parsed_count,
            total_steps=expected_count or parsed_count or None,
            records_fetched=parsed_count,
            stage="Completed",
            message=f"TGA_DAEN parsed {parsed_count} unique report records",
        )

    if expected_count and parsed_count == 0:
        raise RuntimeError(
            f"TGA DAEN found {len(selected_device_ids)} device matches for keywords={keywords} but returned 0 reports for date range {start_text} to {end_text}"
        )
    if expected_count and parsed_count < expected_count:
        warnings.append(
            f"TGA DAEN expected {expected_count} reports but parsed {parsed_count} unique report records."
        )
    elif expected_count and parsed_count > expected_count:
        warnings.append(
            f"TGA DAEN parsed {parsed_count} reports which exceeds the expected report count {expected_count}; duplicate suppression may be incomplete."
        )

    return {"records": df, "warnings": warnings, "metadata": metadata, "raw_text": extracted_text or ""}


def _fetch_tga_report_rows(
    session: requests.Session,
    report_response_content: bytes,
    content_type: str,
    report_url: str,
    config: TgaDaenClientConfig,
    debug: bool = False,
    progress_reporter: ProgressReporter | None = None,
    max_pages: int | None = None,
    max_records: int | None = None,
) -> tuple[list[dict[str, Any]], str]:
    """Fetch and parse all paginated DAEN report rows from the summary table."""
    all_rows: list[dict[str, Any]] = []
    combined_text = ""

    if "pdf" in content_type or report_response_content[:4] == b"%PDF":
        combined_text = extract_text_from_tga_pdf(report_response_content)
        rows = parse_tga_print_report_text(combined_text, debug=debug)
        return rows, combined_text

    html = report_response_content.decode("utf-8", errors="replace")
    current_html = html
    seen_pages: set[str] = set()
    page_index = 1
    max_pages = max_pages or 100
    last_page_hash = None

    while page_index <= max_pages:
        page_hash = hashlib.sha256(current_html.encode("utf-8")).hexdigest()
        if page_hash == last_page_hash:
            break
        last_page_hash = page_hash
        rows = parse_tga_report_summary_html(current_html) or parse_tga_print_report_html(current_html, debug=debug)
        if not rows:
            rows = parse_tga_print_report_text(_strip_tags(current_html), debug=debug)
        new_rows = []
        seen_numbers = {str(row.get("report_number", "")).strip() for row in all_rows if row.get("report_number")}
        for row in rows:
            report_number = str(row.get("report_number", "")).strip()
            if report_number and report_number not in seen_numbers:
                new_rows.append(row)
                seen_numbers.add(report_number)
        if not new_rows:
            break
        all_rows.extend(new_rows)
        if progress_reporter:
            progress_reporter.update_source(
                TGA_DAEN_SOURCE_NAME,
                stage="Parsing report rows",
                records_fetched=len(all_rows),
                message=f"TGA_DAEN parsed {len(all_rows)} summary report rows on page {page_index}",
            )
        if max_records and len(all_rows) >= max_records:
            all_rows = all_rows[:max_records]
            break
        current_status = _extract_tga_page_status(current_html)
        if current_status and current_status[0] >= current_status[1]:
            break
        if not _has_tga_paging_control(current_html, "PageNext"):
            break
        next_html = _advance_tga_report_page(session, current_html, report_url, config, "PageNext", debug=debug)
        if not next_html or next_html == current_html:
            break
        current_html = next_html
        page_index += 1

    combined_text = _strip_tags(current_html)
    return all_rows, combined_text


def _advance_tga_report_page(
    session: requests.Session,
    page_html: str,
    page_url: str,
    config: TgaDaenClientConfig,
    button_name: str,
    debug: bool = False,
) -> str:
    payload = _extract_hidden_form_fields(page_html)
    actual_name, actual_value = _find_submit_button(page_html, button_name)
    if actual_name:
        payload[actual_name] = actual_value or actual_name
    else:
        payload[button_name] = button_name
    response = _request(
        session,
        "POST",
        page_url,
        data=payload,
        headers=_build_headers(config, form=True, ajax=False),
        timeout=config.timeout,
        debug=debug,
        label=f"TGA report page {button_name}",
    )
    if response.status_code >= 400:
        raise RuntimeError(
            f"TGA DAEN pagination failed status={response.status_code} url={response.url} snippet={(response.text or '')[:1000]}"
        )
    return response.text or ""


def _extract_tga_page_status(html: str) -> tuple[int, int] | None:
    match = re.search(r"Page\s+(\d+)\s+of\s+(\d+)", html, re.I)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _find_submit_button(html: str, button_name: str) -> tuple[str | None, str | None]:
    for match in re.finditer(r"<input\b[^>]*>", html, flags=re.I | re.S):
        tag = match.group(0)
        attrs = {name.lower(): unescape(value) for name, _, value in re.findall(r"([a-zA-Z_:][-a-zA-Z0-9_:.]*)\s*=\s*(['\"])(.*?)\2", tag, flags=re.I | re.S)}
        name = attrs.get("name", "")
        element_id = attrs.get("id", "")
        if button_name.lower() in name.lower() or button_name.lower() in element_id.lower():
            if attrs.get("type", "").lower() in {"submit", "button"}:
                return name or element_id or None, attrs.get("value", "")
    return None, None


def _has_tga_paging_control(html: str, control_name: str) -> bool:
    control = control_name.lower()
    for match in re.finditer(r"<input\b[^>]*>", html, flags=re.I | re.S):
        tag = match.group(0)
        attrs = {name.lower(): unescape(value) for name, _, value in re.findall(r"([a-zA-Z_:][-a-zA-Z0-9_:.]*)\s*=\s*(['\"])(.*?)\2", tag, flags=re.I | re.S)}
        name = attrs.get("name", "").lower()
        element_id = attrs.get("id", "").lower()
        if control in name or control in element_id:
            return True
    return False


def fetch_tga_reports_for_devices(
    device_ids: list[str],
    start_date: str,
    end_date: str,
    session: requests.Session | None = None,
    debug: bool = False,
    client_config: TgaDaenClientConfig | None = None,
    progress_reporter: ProgressReporter | None = None,
    max_pages: int | None = None,
    max_records: int | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Backward-compatible wrapper around the direct print-report fetch."""
    config = client_config or TgaDaenClientConfig()
    session = session or requests.Session()
    warnings: list[str] = []
    if not device_ids:
        return ensure_unified_columns(pd.DataFrame()), ["No device IDs provided"]

    if debug:
        logger.debug("fetch_tga_reports_for_devices device_ids=%s start=%s end=%s", device_ids, start_date, end_date)

    _initialize_tga_session(session, config, debug=debug)
    start_dt = date.fromisoformat(_normalize_date(start_date))
    end_dt = date.fromisoformat(_normalize_date(end_date))
    search_response = submit_tga_daen_search(
        device_ids,
        start_dt,
        end_dt,
        session=session,
        debug=debug,
        client_config=config,
    )
    if search_response.status_code >= 400:
        raise RuntimeError(
            f"TGA DAEN report search failed status={search_response.status_code} url={search_response.url} "
            f"snippet={(search_response.text or '')[:1000]}"
        )
    counts = parse_tga_search_result_counts(search_response.text or "")
    warnings.append(f"selected_devices_count={counts.get('selected_devices_count')}")
    warnings.append(f"expected_report_count={counts.get('expected_report_count')}")
    report_url = find_tga_print_version_url(search_response.text or "", search_response.url) or search_response.url
    content, content_type, final_url = fetch_tga_print_report(
        report_url,
        session=session,
        debug=debug,
        client_config=config,
    )
    if "pdf" in content_type or content[:4] == b"%PDF":
        text = extract_text_from_tga_pdf(content)
        raw_rows = parse_tga_print_report_text(text, debug=debug)
    else:
        html = content.decode("utf-8", errors="replace")
        raw_rows = parse_tga_print_report_html(html, debug=debug)
        text = _strip_tags(html)
    metadata = {
        "raw_link": "",
        "event_link": "",
        "print_report_link": final_url,
        "selected_devices_count": counts.get("selected_devices_count"),
    }
    records = ensure_unified_columns(pd.DataFrame([normalize_tga_daen_report(row, metadata) for row in raw_rows]))
    return records, warnings


def parse_tga_report_response(response: requests.Response) -> dict[str, Any]:
    return _parse_generic_response(response, prefer_report=True)


def parse_tga_device_response(response: requests.Response) -> dict[str, Any]:
    text = response.text or ""
    content_type = response.headers.get("Content-Type", "").lower()
    debug: dict[str, Any] = {"content_type": content_type, "response_snippet": text[:1000]}
    try:
        payload = response.json()
    except ValueError:
        payload = {}

    records: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        value = payload.get("d")
        if isinstance(value, list):
            records = [item for item in value if isinstance(item, dict)]
        elif isinstance(value, dict):
            records = [value]
    elif isinstance(payload, list):
        records = [item for item in payload if isinstance(item, dict)]
    debug["record_count"] = len(records)
    return {"records": records, "debug": debug, "raw": payload or text}


def parse_tga_daen_response(response: requests.Response) -> dict[str, Any]:
    return _parse_generic_response(response, prefer_report=False)


def normalize_tga_daen_record(record: dict[str, Any]) -> dict[str, object]:
    event_id = _first_value(record, ["event_id", "case_number", "report_id", "reference", "id"])
    date_of_event = format_fda_date(_first_value(record, ["event_date", "date", "notification_date", "report_date"]))
    date_received = format_fda_date(_first_value(record, ["date_received", "received_date", "notification_date"]))
    event_date = date_of_event or date_received
    product_name = _first_value(record, ["product_name", "device", "product", "trade_name", "DisplayName", "ProductName"])
    manufacturer = _first_value(record, ["manufacturer", "sponsor", "company", "Manufacturer"])
    brand_names = _first_value(record, ["brand_names", "brand_name", "trade_name"])
    generic_name = _first_value(record, ["generic_name", "gmdn", "device_descriptor", "GMDNTerm"])
    event_type = _first_value(record, ["event_type", "report_type", "ReactionTerm", "Name"])
    patient_outcome = _first_value(record, ["patient_outcome", "outcome"])
    device_problem_text = _first_value(record, ["device_problem_text", "device_problem", "problem"])
    patient_problem_text = _first_value(record, ["patient_problem_text", "patient_problem"])
    narrative_text = _first_value(record, ["narrative_text", "description", "summary", "adverse_event", "DisplayName"])
    normalized = {
        "source": TGA_DAEN_SOURCE_NAME,
        "source_type": TGA_DAEN_SOURCE_TYPE,
        "country": TGA_DAEN_COUNTRY,
        "event_id": event_id,
        "report_number": event_id,
        "date_of_event": date_of_event,
        "date_received": date_received,
        "event_date": event_date,
        "year": parse_year_from_date(event_date),
        "received_year": parse_year_from_date(date_received),
        "manufacturer": manufacturer,
        "product_name": product_name,
        "brand_names": brand_names,
        "generic_name": generic_name,
        "device_model": _first_value(record, ["device_model", "model", "model_number", "model_ref"]),
        "product_code": _first_value(record, ["product_code", "code"]),
        "event_type": event_type,
        "patient_outcome": patient_outcome,
        "device_problem_text": device_problem_text,
        "patient_problem_text": patient_problem_text,
        "narrative_text": narrative_text,
        "raw_link": "",
        "event_link": "",
        "record_hash": "",
    }
    normalized["record_hash"] = _record_hash(normalized)
    return normalized


def load_tga_daen_csv(csv_path: str | Any) -> pd.DataFrame:
    raw = pd.read_csv(csv_path)
    normalized = [normalize_tga_daen_record(row.to_dict()) for _, row in raw.iterrows()]
    return ensure_unified_columns(pd.DataFrame(normalized))


def _initialize_tga_session(
    session: requests.Session,
    config: TgaDaenClientConfig,
    debug: bool = False,
) -> None:
    entry_response = _request(
        session,
        "GET",
        config.search_page_url,
        headers=_build_headers(config),
        timeout=config.timeout,
        debug=debug,
        label="TGA entry page",
    )
    if entry_response.status_code >= 400:
        raise RuntimeError(
            f"TGA entry page failed status={entry_response.status_code} url={entry_response.url} "
            f"snippet={(entry_response.text or '')[:1000]}"
        )
    disclaimer_url = config.search_page_url.rstrip("/") + "/AgreedToDisclaimer"
    if config.search_page_url.lower().endswith("daen-entry.aspx"):
        disclaimer_url = config.search_page_url + "/AgreedToDisclaimer"
    disclaimer_response = _request(
        session,
        "POST",
        disclaimer_url,
        json={},
        headers=_build_headers(config, json_body=True),
        timeout=config.timeout,
        debug=debug,
        label="TGA disclaimer",
    )
    if disclaimer_response.status_code >= 400:
        raise RuntimeError(
            f"TGA disclaimer acceptance failed status={disclaimer_response.status_code} "
            f"url={disclaimer_response.url} snippet={(disclaimer_response.text or '')[:1000]}"
        )


def _parse_generic_response(response: requests.Response, prefer_report: bool) -> dict[str, Any]:
    text = response.text or ""
    content_type = response.headers.get("Content-Type", "").lower()
    debug: dict[str, Any] = {
        "content_type": content_type,
        "response_snippet": text[:1000],
    }
    if "json" in content_type or text.lstrip().startswith("{") or text.lstrip().startswith("["):
        try:
            payload = response.json()
            records = _extract_records_from_json(payload)
            debug["record_count"] = len(records)
            return {"records": records, "debug": debug, "raw": payload}
        except ValueError:
            pass
    records = _extract_records_from_html(text, prefer_report=prefer_report)
    debug["record_count"] = len(records)
    return {"records": records, "debug": debug, "raw": text}


def _extract_records_from_json(payload: Any) -> pd.DataFrame:
    candidates: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        for key in ["results", "records", "data", "items", "rows", "d"]:
            value = payload.get(key)
            if isinstance(value, list):
                candidates = [item for item in value if isinstance(item, dict)]
                break
    elif isinstance(payload, list):
        candidates = [item for item in payload if isinstance(item, dict)]
    normalized = [normalize_tga_daen_record(item) for item in candidates]
    return ensure_unified_columns(pd.DataFrame(normalized))


def _extract_records_from_html(html_text: str, prefer_report: bool) -> pd.DataFrame:
    tables = _extract_html_tables(html_text)
    rows: list[dict[str, Any]] = []
    for table in tables:
        headers = [cell.lower().strip() for cell in table["headers"]]
        if not headers:
            continue
        if prefer_report and any("report" in header for header in headers):
            for row in table["rows"]:
                record = {headers[idx]: row[idx] if idx < len(row) else "" for idx in range(len(headers))}
                rows.append(_normalize_report_row(record))
        elif not prefer_report:
            for row in table["rows"]:
                record = {headers[idx]: row[idx] if idx < len(row) else "" for idx in range(len(headers))}
                rows.append(normalize_tga_daen_record(record))
        if rows:
            break
    if rows:
        return ensure_unified_columns(pd.DataFrame(rows))
    if prefer_report:
        return ensure_unified_columns(pd.DataFrame(_parse_daen_report_blocks(html_text) or _parse_daen_markdown_rows(html_text)))
    return ensure_unified_columns(pd.DataFrame())


def _parse_daen_markdown_rows(text: str) -> list[dict[str, Any]]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    records: list[dict[str, Any]] = []
    for line in lines:
        if not line.startswith("|") or line.count("|") < 6:
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) < 6:
            continue
        header = [cell.lower() for cell in cells[:6]]
        if {"report #", "date", "trade name"}.issubset(set(header)):
            continue
        record = {
            "report_number": cells[0],
            "report_date": _normalize_report_date(cells[1]),
            "trade_name": cells[2],
            "model_ref": cells[3],
            "event_description": cells[4],
            "outcome": cells[5],
        }
        records.append(_normalize_report_row(record))
    return records


def _parse_daen_report_blocks(text: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    block: dict[str, str] = {}
    field_map = {
        "report": "report_number",
        "report #": "report_number",
        "report#": "report_number",
        "date": "report_date",
        "trade name": "trade_name",
        "model/ref": "model_ref",
        "model / ref": "model_ref",
        "event description": "event_description",
        "outcome": "outcome",
    }

    for raw_line in [line.strip() for line in text.splitlines() if line.strip()]:
        lowered = raw_line.lower()
        matched = False
        for prefix, field in field_map.items():
            if lowered.startswith(prefix):
                value = raw_line.split(":", 1)[1].strip() if ":" in raw_line else raw_line[len(prefix):].strip()
                if field == "report_number" and value.startswith("#"):
                    value = value.lstrip("#").strip()
                block[field] = value
                matched = True
                break
        if matched:
            continue
        if block and raw_line and not raw_line.lower().startswith(("report", "date", "trade name", "model/ref", "outcome")):
            block["event_description"] = (block.get("event_description", "") + " " + raw_line).strip()
        if "report_number" in block and "outcome" in block and "event_description" in block and "trade_name" in block and "model_ref" in block and "report_date" in block:
            records.append(_normalize_report_row(block))
            block = {}

    if block and {"report_number", "report_date", "trade_name", "model_ref", "event_description", "outcome"}.issubset(block):
        records.append(_normalize_report_row(block))
    return records


def _normalize_report_row(raw_report: dict[str, Any]) -> dict[str, Any]:
    normalized_keys = {}
    for key, value in raw_report.items():
        normalized_key = re.sub(r"[^a-z0-9]+", "_", str(key).lower()).strip("_")
        normalized_keys[normalized_key] = value
    report_number = _first_value(normalized_keys, ["report_number", "report", "case_number", "event_id"])
    report_date = format_fda_date(_first_value(normalized_keys, ["date", "report_date", "event_date"]))
    trade_name = _first_value(normalized_keys, ["trade_name", "product_name", "product", "device", "name"])
    model_ref = _first_value(normalized_keys, ["model_ref", "model/ref", "model", "device_model"])
    event_description = _first_value(normalized_keys, ["event_description", "event description", "description", "summary", "narrative_text"])
    outcome = _first_value(normalized_keys, ["outcome", "event_type", "patient_outcome"])
    return {
        "report_number": report_number,
        "report_date": report_date,
        "trade_name": trade_name,
        "model_ref": model_ref,
        "event_description": event_description,
        "outcome": outcome,
        "event_link": _first_value(normalized_keys, ["event_link", "report_link", "case_link", "url", "link"]),
    }


def _extract_html_tables(html_text: str) -> list[dict[str, list[list[str]]]]:
    tables: list[dict[str, list[list[str]]]] = []
    for table in _extract_html_tables_with_html(html_text):
        headers = [_html_cell_text(cell) for cell in table["headers"]]
        rows = [[_html_cell_text(cell) for cell in row] for row in table["rows"]]
        if headers and rows:
            tables.append({"headers": headers, "rows": rows})
    return tables


def _extract_html_tables_with_html(html_text: str) -> list[dict[str, list[list[str]]]]:
    tables: list[dict[str, list[list[str]]]] = []
    for table_html in re.findall(r"<table.*?>.*?</table>", html_text, flags=re.I | re.S):
        header_rows = re.findall(r"<tr.*?>(.*?)</tr>", table_html, flags=re.I | re.S)
        if not header_rows:
            continue
        headers: list[str] = []
        rows: list[list[str]] = []
        for idx, row_html in enumerate(header_rows):
            cells = re.findall(r"<t[dh]\b.*?>(.*?)</t[dh]>", row_html, flags=re.I | re.S)
            if not cells:
                continue
            if idx == 0:
                headers = cells
            else:
                rows.append(cells)
        if headers and rows:
            tables.append({"headers": headers, "rows": rows})
    return tables


def _html_cell_text(cell_html: str) -> str:
    return unescape(_strip_tags(cell_html)).strip()


def _extract_first_href(cell_html: str, base_url: str = "") -> str:
    match = re.search(r"<a\b[^>]+href=[\"']([^\"']+)[\"']", cell_html, flags=re.I | re.S)
    if not match:
        return ""
    href = unescape(match.group(1)).strip()
    if not href or href.lower().startswith("javascript:"):
        return href
    return requests.compat.urljoin(base_url, href)


def _find_link_by_text(
    html: str,
    base_url: str,
    needles: list[str],
    prefer_javascript: bool = False,
) -> str | None:
    for href, text in re.findall(r'<a[^>]+href=[\"\']([^\"\']+)[\"\'][^>]*>(.*?)</a>', html, flags=re.I | re.S):
        normalized = _collapse_ws(_strip_tags(text)).lower()
        if any(needle in normalized for needle in needles):
            href = unescape(href)
            if prefer_javascript and href.lower().startswith("javascript:"):
                return href
            if not prefer_javascript and href.lower().startswith("javascript:"):
                continue
            return requests.compat.urljoin(base_url, href)
    return None


def _normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _normalize_tga_report_header(value: str) -> str:
    header = _normalize_header(value)
    aliases = {
        "report": "report_number",
        "report_no": "report_number",
        "report_num": "report_number",
        "date": "report_date",
    }
    return aliases.get(header, header)


def _find_report_generation_date(text: str) -> str | None:
    match = re.search(r"Report Generation Date[:\s]*([0-9]{2}/[0-9]{2}/[0-9]{4})", text, re.I)
    return _to_iso_date(match.group(1)) if match else None


def _submit_tga_postback(
    session: requests.Session,
    page_html: str,
    page_url: str,
    javascript_href: str,
    config: TgaDaenClientConfig,
    debug: bool = False,
) -> requests.Response:
    """Submit an ASP.NET __doPostBack link from a DAEN result page."""
    target, argument = _parse_do_postback(javascript_href)
    if not target:
        raise RuntimeError(f"Unable to parse DAEN postback target from href={javascript_href!r}")

    payload = _extract_hidden_form_fields(page_html)
    payload["__EVENTTARGET"] = target
    payload["__EVENTARGUMENT"] = argument or ""
    payload.setdefault("__LASTFOCUS", "")
    return _request(
        session,
        "POST",
        page_url,
        data=payload,
        headers=_build_headers(config, form=True, ajax=False),
        timeout=config.timeout,
        debug=debug,
        label="TGA print postback",
    )


def _parse_do_postback(href: str) -> tuple[str | None, str | None]:
    decoded = unescape(href)
    match = re.search(r"__doPostBack\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]*)['\"]\s*\)", decoded, re.I)
    if not match:
        return None, None
    return match.group(1), match.group(2)


def _extract_hidden_form_fields(html: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    input_pattern = re.compile(r"<input\b[^>]*>", re.I | re.S)
    attr_pattern = re.compile(r"([a-zA-Z_:][-a-zA-Z0-9_:.]*)\s*=\s*(['\"])(.*?)\2", re.I | re.S)
    for input_html in input_pattern.findall(html):
        attrs = {name.lower(): unescape(value) for name, _, value in attr_pattern.findall(input_html)}
        input_type = attrs.get("type", "").lower()
        name = attrs.get("name")
        if name and input_type == "hidden":
            fields[name] = attrs.get("value", "")
    return fields


def _request(
    session: requests.Session,
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    json: Any | None = None,
    data: Any | None = None,
    timeout: float | None = None,
    debug: bool = False,
    label: str = "",
) -> requests.Response:
    request_method = getattr(session, "request", None)
    if callable(request_method):
        response = request_method(method, url, headers=headers, json=json, data=data, timeout=timeout)
    else:
        method_lower = method.lower()
        if method_lower == "get" and hasattr(session, "get"):
            response = session.get(url, headers=headers, timeout=timeout)
        elif method_lower == "post" and hasattr(session, "post"):
            response = session.post(url, headers=headers, json=json, data=data, timeout=timeout)
        else:
            raise AttributeError("Session object does not support request/get/post methods")
    if debug:
        logger.debug(
            "%s method=%s url=%s status=%s content_type=%s params=%s json=%s data=%s snippet=%s",
            label,
            method,
            response.url,
            response.status_code,
            response.headers.get("Content-Type", ""),
            getattr(response.request, "path_url", ""),
            json,
            data,
            (response.text or "")[:1000],
        )
    return response


def _build_headers(
    config: TgaDaenClientConfig,
    json_body: bool = False,
    form: bool = False,
    ajax: bool = True,
) -> dict[str, str]:
    headers = {
        "Accept": "application/json, text/html;q=0.9, */*;q=0.8",
        "User-Agent": config.user_agent,
    }
    if ajax:
        headers["X-Requested-With"] = "XMLHttpRequest"
    if json_body:
        headers["Content-Type"] = "application/json; charset=UTF-8"
    if form:
        headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
    if config.default_headers:
        headers.update(config.default_headers)
    return headers


def _merge_terms(
    keywords: list[str],
    components: list[str] | None,
    accident_terms: list[str] | None,
) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for value in [*keywords, *(components or []), *(accident_terms or [])]:
        term = str(value).strip()
        if not term:
            continue
        key = term.lower()
        if key not in seen:
            merged.append(term)
            seen.add(key)
    return merged


def _extract_device_ids(device_matches: list[dict[str, Any]]) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for item in device_matches:
        key = str(item.get("HashKey") or item.get("Key") or item.get("SelectedValue") or "").strip()
        if key and key not in seen:
            ids.append(key)
            seen.add(key)
    return ids


def _first_value(record: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        if key in record:
            value = record.get(key)
            if value not in (None, "") and not pd.isna(value):
                return str(value).strip()
    lower_map = {str(k).lower(): v for k, v in record.items()}
    for key in keys:
        value = lower_map.get(key.lower())
        if value not in (None, "") and not pd.isna(value):
            return str(value).strip()
    return ""


def _record_hash(record: dict[str, Any]) -> str:
    basis = "|".join(
        [
            str(record.get("event_id", "")),
            str(record.get("event_date", "")),
            str(record.get("product_name", "")),
            str(record.get("manufacturer", "")),
            str(record.get("narrative_text", ""))[:250],
        ]
    )
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def _format_daen_date(value: str) -> str:
    dt = datetime.strptime(value, "%Y-%m-%d")
    return dt.strftime("%d/%m/%Y")


def _normalize_date(value: str | date) -> str:
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", text):
        return datetime.strptime(text, "%d/%m/%Y").date().isoformat()
    return datetime.strptime(text, "%Y-%m-%d").date().isoformat()


def _normalize_report_date(value: str) -> str:
    text = str(value).strip()
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", text):
        return datetime.strptime(text, "%d/%m/%Y").date().isoformat()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    return format_fda_date(text)


def _to_iso_date(value: str) -> str:
    return datetime.strptime(value, "%d/%m/%Y").date().isoformat()


def _strip_tags(html: str) -> str:
    return re.sub(r"<[^>]+>", " ", html)


def _collapse_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _filter_records(df: pd.DataFrame, keywords: list[str], years: list[int]) -> pd.DataFrame:
    if df.empty:
        return df
    filtered = df.copy()
    filtered["year"] = pd.to_numeric(filtered["year"], errors="coerce").astype("Int64")
    if years:
        filtered = filtered[filtered["year"].isin(years)].copy()
    if not keywords:
        return filtered
    search = (
        filtered["product_name"].fillna("").astype(str)
        + " "
        + filtered["manufacturer"].fillna("").astype(str)
        + " "
        + filtered["narrative_text"].fillna("").astype(str)
    )
    mask = search.apply(lambda text: any(term.lower() in text.lower() for term in keywords))
    return filtered[mask].copy()


def _filter_by_date_range(df: pd.DataFrame, start_date: object | None, end_date: object | None) -> pd.DataFrame:
    if df.empty or (start_date is None and end_date is None):
        return df
    return df[df["event_date"].apply(lambda value: date_or_year_in_range(value, start_date, end_date))].copy()
