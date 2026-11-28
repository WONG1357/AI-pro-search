"""UK MHRA field safety notice connector via GOV.UK."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, replace
from datetime import date
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from pipeline.config import MHRA_FSCA_BASE_URL, MHRA_FSCA_SEARCH_ENDPOINT, MHRA_FSCA_TIMEOUT, MHRA_FSCA_USER_AGENT
from pipeline.progress import ProgressReporter
from pipeline.sources.base import BaseSourceConnector, SourceFetchResult, ensure_unified_columns
from pipeline.utils import date_or_year_in_range, format_fda_date, parse_year_from_date
from pipeline.source_support import request_with_retry, reject_challenge_html, with_coverage


logger = logging.getLogger(__name__)

SOURCE_NAME = "MHRA_FSCA"
SOURCE_TYPE = "regulatory_recall"
COUNTRY = "United Kingdom"


@dataclass
class MhraFscaClientConfig:
    """HTTP settings for GOV.UK MHRA field safety notices."""

    base_url: str = MHRA_FSCA_BASE_URL
    search_endpoint: str = MHRA_FSCA_SEARCH_ENDPOINT
    timeout: float = MHRA_FSCA_TIMEOUT
    user_agent: str = MHRA_FSCA_USER_AGENT

    @property
    def search_url(self) -> str:
        return urljoin(f"{self.base_url}/", self.search_endpoint.lstrip("/"))


class MhraFscaConnector(BaseSourceConnector):
    """Fetch UK MHRA field safety notice records."""

    source_name = SOURCE_NAME
    source_type = SOURCE_TYPE
    display_name = "UK MHRA field safety notices"

    def __init__(self, client_config: MhraFscaClientConfig | None = None) -> None:
        self.client_config = client_config or MhraFscaClientConfig()

    def fetch(
        self,
        keywords: list[str],
        years: list[int],
        components: list[str] | None = None,
        accident_terms: list[str] | None = None,
        progress_reporter: ProgressReporter | None = None,
        **kwargs: Any,
    ) -> SourceFetchResult:
        """Fetch MHRA FSN records for the requested keywords and years."""
        config = replace(self.client_config)
        if kwargs.get("request_timeout"):
            config.timeout = float(kwargs["request_timeout"])
        try:
            records, warnings = fetch_mhra_fsca_direct(
                keywords=_merge_terms(keywords, components, accident_terms),
                years=years,
                start_date=kwargs.get("start_date"),
                end_date=kwargs.get("end_date"),
                client_config=config,
                progress_reporter=progress_reporter,
                max_pages=int(kwargs["max_pages"]) if kwargs.get("max_pages") else None,
                max_records=int(kwargs["max_records"]) if kwargs.get("max_records") else None,
                debug=bool(kwargs.get("debug", False)),
                session=kwargs.get("session"),
            )
            return SourceFetchResult(
                source_name=self.source_name,
                records=ensure_unified_columns(records),
                success=True,
                warnings=warnings,
            )
        except Exception as exc:
            logger.exception("MHRA FSCA fetch failed")
            return SourceFetchResult(
                source_name=self.source_name,
                records=ensure_unified_columns(pd.DataFrame()),
                success=False,
                error_message=f"MHRA FSCA fetch failed: {exc}",
                warnings=[f"MHRA FSCA fetch failed: {exc}"],
            )


def build_mhra_search_params(keyword: str, page: int = 1) -> dict[str, str]:
    """Build GOV.UK field safety notice search parameters."""
    return {
        "alert_type": "field-safety-notices",
        "keywords": keyword,
        "page": str(page),
    }


def fetch_mhra_search_page(
    keyword: str,
    page: int,
    session: requests.Session,
    client_config: MhraFscaClientConfig,
    debug: bool = False,
) -> requests.Response:
    """Fetch one GOV.UK MHRA FSN search page."""
    params = build_mhra_search_params(keyword, page=page)
    response = request_with_retry(session, "GET", client_config.search_url, params=params, headers=_headers(client_config), timeout=client_config.timeout)
    if debug:
        logger.debug("MHRA GET %s params=%s status=%s snippet=%s", client_config.search_url, params, response.status_code, response.text[:1000])
    reject_challenge_html(response.text, "MHRA")
    return response


def parse_mhra_search_results(html: str, base_url: str = MHRA_FSCA_BASE_URL) -> list[dict[str, Any]]:
    """Parse GOV.UK finder result cards into weekly FSN page links."""
    soup = BeautifulSoup(html, "html.parser")
    records: list[dict[str, Any]] = []
    for item in soup.select(".gem-c-document-list__item"):
        link = item.find("a", href=True)
        if not link:
            continue
        text = item.get_text(" ", strip=True)
        issued_match = re.search(r"Issued:\s*([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4})", text)
        records.append(
            {
                "title": link.get_text(" ", strip=True),
                "url": urljoin(base_url, link["href"]),
                "summary": text,
                "issued_date": issued_match.group(1) if issued_match else "",
            }
        )
    return records


def parse_mhra_notice_page(html: str, page_url: str, keyword: str = "") -> list[dict[str, Any]]:
    """Parse one weekly MHRA FSN page into individual manufacturer/device records."""
    soup = BeautifulSoup(html, "html.parser")
    title = (soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "").strip()
    weekly_issued_date = ""
    for label in soup.find_all(["dt", "span"]):
        if label.get_text(" ", strip=True).rstrip(":").casefold() == "issued":
            value = label.find_next_sibling(["dd", "span"])
            if value and parse_year_from_date(value.get_text(" ", strip=True)):
                weekly_issued_date = format_fda_date(value.get_text(" ", strip=True))
                break
    content = soup.select_one(".gem-c-govspeak") or soup.select_one("#contents") or soup
    records: list[dict[str, Any]] = []
    headings = content.find_all(["h2", "h3"])
    for heading in headings:
        heading_text = heading.get_text(" ", strip=True)
        if not heading_text or heading_text.lower() in {"overview", "latest fsns", "contents"}:
            continue
        block_text_parts: list[str] = []
        links: list[dict[str, str]] = []
        node = heading.find_next_sibling()
        while node and getattr(node, "name", None) not in {"h2", "h3"}:
            if hasattr(node, "get_text"):
                part = node.get_text(" ", strip=True)
                if part:
                    block_text_parts.append(part)
                for link in node.find_all("a", href=True):
                    links.append({"text": link.get_text(" ", strip=True), "url": urljoin(page_url, link["href"])})
            node = node.find_next_sibling()
        block_text = " ".join(block_text_parts)
        if keyword and keyword.lower() not in f"{heading_text} {block_text}".lower():
            continue
        notice_date = _extract_notice_date(block_text_parts[0]) if block_text_parts else ""
        records.append(
            {
                "weekly_title": title,
                "weekly_url": page_url,
                "heading": heading_text,
                "body": block_text,
                "notice_date": notice_date,
                "weekly_issued_date": weekly_issued_date,
                "issued_date": notice_date or weekly_issued_date,
                "links": links,
            }
        )
    return records


def fetch_mhra_fsca_direct(
    keywords: list[str],
    years: list[int],
    start_date: object | None = None,
    end_date: object | None = None,
    client_config: MhraFscaClientConfig | None = None,
    progress_reporter: ProgressReporter | None = None,
    max_pages: int | None = None,
    debug: bool = False,
    session: requests.Session | None = None,
    max_records: int | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Fetch MHRA field safety notices using keyword OR semantics."""
    config = client_config or MhraFscaClientConfig()
    session = session or requests.Session()
    terms = _merge_terms(keywords, None, None)
    warnings: list[str] = []
    normalized: list[dict[str, Any]] = []
    seen_records: set[str] = set()
    detail_cache: dict[str, str] = {}
    total_terms = max(1, len(terms))
    failures = 0
    capped = False
    page_limit = int(max_pages) if max_pages is not None else 100
    if page_limit < 1 or (max_records is not None and max_records < 1):
        raise ValueError("MHRA retrieval limits must be positive")

    if progress_reporter:
        progress_reporter.update_source(SOURCE_NAME, current_step=0, total_steps=total_terms, records_fetched=0, stage="Searching MHRA FSNs")

    for term_index, keyword in enumerate(terms or [""], start=1):
        if max_records and len(normalized) >= max_records:
            capped = True
            break
        page = 1
        seen_pages: set[str] = set()
        while True:
            try:
                response = fetch_mhra_search_page(keyword, page, session, config, debug=debug)
            except (requests.RequestException, RuntimeError) as exc:
                failures += 1
                warnings.append(f"MHRA keyword={keyword} page={page} failed: {type(exc).__name__}: {exc}")
                break
            page_hash = hashlib.sha256(response.content).hexdigest()
            if page_hash in seen_pages:
                failures += 1
                warnings.append(f"Repeated MHRA search page for keyword={keyword} page={page}; stopped pagination")
                break
            seen_pages.add(page_hash)
            result_pages = parse_mhra_search_results(response.text, config.base_url)
            if not result_pages:
                if not re.search(r"no (?:results|matches)|0 (?:results|notices|alerts)", response.text, re.I):
                    failures += 1
                    warnings.append("MHRA listing format not recognized; empty search not verified")
                break
            for result_page in result_pages:
                try:
                    if result_page["url"] not in detail_cache:
                        detail = request_with_retry(session, "GET", result_page["url"], headers=_headers(config), timeout=config.timeout)
                        reject_challenge_html(detail.text, "MHRA detail")
                        if not BeautifulSoup(detail.text, "html.parser").find_all(["h2", "h3"]):
                            raise RuntimeError("MHRA notice structure not recognized")
                        detail_cache[result_page["url"]] = detail.text
                    raw_records = parse_mhra_notice_page(detail_cache[result_page["url"]], result_page["url"], keyword=keyword)
                except (requests.RequestException, RuntimeError) as exc:
                    failures += 1
                    warnings.append(f"MHRA detail fetch failed: {type(exc).__name__}: {exc}")
                    continue
                for raw in raw_records:
                    if not raw["weekly_issued_date"]:
                        raw["weekly_issued_date"] = format_fda_date(result_page.get("issued_date"))
                        raw["issued_date"] = raw["notice_date"] or raw["weekly_issued_date"]
                    record = normalize_mhra_fsca_record(raw)
                    record["source_query_match"] = True
                    if years and record.get("year") not in years:
                        continue
                    if (start_date is not None or end_date is not None) and not date_or_year_in_range(record.get("event_date"), start_date, end_date):
                        continue
                    key = record["event_id"]
                    if key not in seen_records:
                        normalized.append(record)
                        seen_records.add(key)
                if max_records and len(normalized) >= max_records:
                    capped = True
                    normalized = normalized[:max_records]
                    break
            if progress_reporter:
                progress_reporter.update_source(
                    SOURCE_NAME,
                    current_step=term_index,
                    total_steps=total_terms,
                    records_fetched=len(normalized),
                    stage="Fetching MHRA FSNs",
                    message=f"keyword={keyword} page={page} weekly_pages={len(result_pages)} records={len(normalized)}",
                )
            if capped:
                break
            if not _has_next_page(response.text):
                break
            if page >= page_limit:
                capped = True
                warnings.append(f"Stopped at max_pages={page_limit} for keyword={keyword}")
                break
            page += 1

    df = ensure_unified_columns(pd.DataFrame(normalized))
    warnings.append(f"mhra_fsca_records={len(df)}")
    if capped:
        warnings.append(f"MHRA retrieval capped: max_pages={page_limit}, max_records={max_records}")
    warnings.append("MHRA retrieval covers indexed notice headings and excerpts; full attachment content and exhaustive coverage are unverified")
    warnings.append(
        "MHRA filters by individual notice date when supplied, otherwise by weekly publication date. "
        "Partial dates qualify only when the whole known period is inside the requested range."
    )
    coverage = "failed" if failures and not normalized else "partial" if failures or capped else "unverified"
    return with_coverage(df, coverage, warnings), warnings


def normalize_mhra_fsca_record(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize one MHRA field safety notice item."""
    heading = raw.get("heading", "")
    manufacturer, product = _split_heading(heading)
    notice_date = format_fda_date(raw.get("notice_date"))
    weekly_date = format_fda_date(raw.get("weekly_issued_date"))
    # Older callers supplied a single issued date with no distinction between levels.
    legacy_date = format_fda_date(raw.get("issued_date")) if "notice_date" not in raw else ""
    issued_date = notice_date or weekly_date or legacy_date
    report_date = weekly_date or legacy_date
    if notice_date:
        date_basis = "manufacturer_notice_date"
    elif weekly_date:
        date_basis = "weekly_publication_date"
    elif legacy_date:
        date_basis = "source_issued_date"
    else:
        date_basis = "unknown"
    links = raw.get("links") if isinstance(raw.get("links"), list) else []
    fsn_link = _preferred_fsn_link(links) or raw.get("weekly_url", "")
    event_id = _stable_id(raw.get("weekly_url", ""), heading, fsn_link)
    narrative = " | ".join(part for part in [heading, raw.get("body", ""), raw.get("weekly_title", "")] if part)
    source_specific = {
        "weekly_title": raw.get("weekly_title"),
        "weekly_url": raw.get("weekly_url"),
        "date_basis": date_basis,
        "notice_date": notice_date,
        "weekly_issued_date": weekly_date,
        "heading": heading,
        "links": links,
    }
    record = {
        "source": SOURCE_NAME,
        "source_type": SOURCE_TYPE,
        "event_id": event_id,
        "report_number": event_id,
        "date_of_event": "",
        "date_received": report_date,
        "report_date": report_date,
        "event_date": issued_date,
        "search_date_basis": date_basis,
        "year": parse_year_from_date(issued_date),
        "received_year": parse_year_from_date(report_date),
        "country": COUNTRY,
        "manufacturer": manufacturer,
        "product_name": product or heading,
        "brand_names": product or heading,
        "generic_name": "",
        "device_model": _extract_model(raw.get("body", "")),
        "product_code": "",
        "event_type": "Field safety notice",
        "patient_outcome": "",
        "device_problem_text": raw.get("body", ""),
        "patient_problem_text": "",
        "narrative_text": narrative,
        "raw_link": raw.get("weekly_url", ""),
        "event_link": fsn_link,
        "source_category": "Field safety notice",
        "source_category_name": "Field safety notice",
        "source_category_system": "GOV.UK alert type",
        "record_hash": "",
        "source_specific": json.dumps(source_specific, ensure_ascii=False, default=str),
        "raw_record": json.dumps(raw, ensure_ascii=False, default=str),
    }
    record["record_hash"] = _record_hash(record)
    return record


def _has_next_page(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    return bool(soup.find("a", rel=lambda value: value and "next" in value))


def _split_heading(heading: str) -> tuple[str, str]:
    if ":" in heading:
        manufacturer, product = heading.split(":", 1)
        return manufacturer.strip(), product.strip()
    return "", heading.strip()


def _preferred_fsn_link(links: list[dict[str, str]]) -> str:
    for link in links:
        if "filecamp.com" in str(link.get("url", "")):
            return str(link["url"])
    return str(links[0].get("url", "")) if links else ""


def _extract_model(text: str) -> str:
    match = re.search(r"Model:\s*([^|]+?)(?:\s+MHRA reference:|$)", text)
    return match.group(1).strip() if match else ""


def _extract_notice_date(text: str) -> str:
    """Read the leading FSN date while excluding model numbers and prose dates."""
    match = re.fullmatch(
        r"^(?:(?:issued|date|FSN date)\s*:\s*)?"
        r"(\d{4}(?:-\d{2}){0,2}|\d{1,2}/\d{1,2}/\d{4}|(?:\d{1,2}\s+)?[A-Za-z]+\s+\d{4})"
        r"(?:\s+FSN-[A-Z0-9][A-Z0-9./-]*)?",
        text.strip(), re.I,
    )
    if match and parse_year_from_date(match[1]) is not None:
        return format_fda_date(match[1])
    return ""


def _merge_terms(keywords: list[str], components: list[str] | None, accident_terms: list[str] | None) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    for value in [*keywords, *(components or []), *(accident_terms or [])]:
        term = str(value).strip()
        key = term.lower()
        if term and key not in seen:
            terms.append(term)
            seen.add(key)
    return terms


def _headers(config: MhraFscaClientConfig) -> dict[str, str]:
    return {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "User-Agent": config.user_agent}


def _stable_id(*parts: str) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()[:16]


def _record_hash(record: dict[str, Any]) -> str:
    return hashlib.sha256(
        "|".join(
            [
                str(record.get("source", "")),
                str(record.get("event_id", "")),
                str(record.get("event_date", "")),
                str(record.get("product_name", "")),
            ]
        ).encode("utf-8")
    ).hexdigest()
