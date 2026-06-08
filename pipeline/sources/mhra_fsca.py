"""UK MHRA field safety notice connector via GOV.UK."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
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
        config = self.client_config
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
    response = session.get(client_config.search_url, params=params, headers=_headers(client_config), timeout=client_config.timeout)
    if debug:
        logger.debug("MHRA GET %s params=%s status=%s snippet=%s", client_config.search_url, params, response.status_code, response.text[:1000])
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code} for {response.url}: {response.text[:1000]}")
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
    issued_text = ""
    for dt in soup.find_all(["dt", "span"]):
        if "Issued" in dt.get_text(" ", strip=True):
            dd = dt.find_next(["dd", "span"])
            issued_text = dd.get_text(" ", strip=True) if dd else ""
            break
    issued_date = format_fda_date(issued_text)
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
                block_text_parts.append(node.get_text(" ", strip=True))
                for link in node.find_all("a", href=True):
                    links.append({"text": link.get_text(" ", strip=True), "url": urljoin(page_url, link["href"])})
            node = node.find_next_sibling()
        block_text = " ".join(part for part in block_text_parts if part)
        if keyword and keyword.lower() not in f"{heading_text} {block_text}".lower():
            continue
        records.append(
            {
                "weekly_title": title,
                "weekly_url": page_url,
                "heading": heading_text,
                "body": block_text,
                "issued_date": issued_date or _extract_notice_date(block_text),
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
) -> tuple[pd.DataFrame, list[str]]:
    """Fetch MHRA field safety notices using keyword OR semantics."""
    config = client_config or MhraFscaClientConfig()
    session = session or requests.Session()
    terms = _merge_terms(keywords, None, None)
    warnings: list[str] = []
    normalized: list[dict[str, Any]] = []
    seen_pages: set[str] = set()
    seen_records: set[str] = set()
    total_terms = max(1, len(terms))

    if progress_reporter:
        progress_reporter.update_source(SOURCE_NAME, current_step=0, total_steps=total_terms, records_fetched=0, stage="Searching MHRA FSNs")

    for term_index, keyword in enumerate(terms or [""], start=1):
        page = 1
        while True:
            response = fetch_mhra_search_page(keyword, page, session, config, debug=debug)
            page_hash = hashlib.sha256(response.content).hexdigest()
            if page_hash in seen_pages:
                warnings.append(f"Repeated MHRA search page for keyword={keyword} page={page}; stopped pagination")
                break
            seen_pages.add(page_hash)
            result_pages = parse_mhra_search_results(response.text, config.base_url)
            if not result_pages:
                break
            for result_page in result_pages:
                detail = session.get(result_page["url"], headers=_headers(config), timeout=config.timeout)
                if detail.status_code >= 400:
                    warnings.append(f"MHRA detail fetch failed {detail.status_code}: {result_page['url']}")
                    continue
                raw_records = parse_mhra_notice_page(detail.text, result_page["url"], keyword=keyword)
                for raw in raw_records:
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
            if progress_reporter:
                progress_reporter.update_source(
                    SOURCE_NAME,
                    current_step=term_index,
                    total_steps=total_terms,
                    records_fetched=len(normalized),
                    stage="Fetching MHRA FSNs",
                    message=f"keyword={keyword} page={page} weekly_pages={len(result_pages)} records={len(normalized)}",
                )
            if max_pages and page >= max_pages:
                warnings.append(f"Stopped at max_pages={max_pages} for keyword={keyword}")
                break
            if not _has_next_page(response.text):
                break
            page += 1

    df = ensure_unified_columns(pd.DataFrame(normalized))
    warnings.append(f"mhra_fsca_records={len(df)}")
    return df, warnings


def normalize_mhra_fsca_record(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize one MHRA field safety notice item."""
    heading = raw.get("heading", "")
    manufacturer, product = _split_heading(heading)
    issued_date = format_fda_date(raw.get("issued_date"))
    links = raw.get("links") if isinstance(raw.get("links"), list) else []
    fsn_link = _preferred_fsn_link(links) or raw.get("weekly_url", "")
    event_id = _stable_id(raw.get("weekly_url", ""), heading, fsn_link)
    narrative = " | ".join(part for part in [heading, raw.get("body", ""), raw.get("weekly_title", "")] if part)
    source_specific = {
        "weekly_title": raw.get("weekly_title"),
        "weekly_url": raw.get("weekly_url"),
        "heading": heading,
        "links": links,
    }
    record = {
        "source": SOURCE_NAME,
        "source_type": SOURCE_TYPE,
        "event_id": event_id,
        "report_number": event_id,
        "date_of_event": "",
        "date_received": issued_date,
        "event_date": issued_date,
        "year": parse_year_from_date(issued_date),
        "received_year": parse_year_from_date(issued_date),
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
    """Extract source-provided full or partial FSN date text from a notice block."""
    patterns = [
        r"\b\d{1,2}\s+[A-Za-z]+\s+\d{4}\b",
        r"\b[A-Za-z]+\s+\d{4}\b",
        r"\b\d{4}-\d{2}-\d{2}\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return format_fda_date(match.group(0))
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
