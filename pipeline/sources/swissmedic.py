"""Swissmedic FSCA medical device recall connector."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import date
from typing import Any
from urllib.parse import urlencode, urljoin

import pandas as pd
import requests

from pipeline.config import (
    SWISSMEDIC_FSCA_BASE_URL,
    SWISSMEDIC_FSCA_PAGE_SIZE,
    SWISSMEDIC_FSCA_SEARCH_ENDPOINT,
    SWISSMEDIC_FSCA_TIMEOUT,
    SWISSMEDIC_FSCA_USER_AGENT,
)
from pipeline.progress import ProgressReporter
from pipeline.sources.base import BaseSourceConnector, SourceFetchResult, ensure_unified_columns
from pipeline.utils import format_fda_date, parse_year_from_date


logger = logging.getLogger(__name__)

SOURCE_NAME = "SWISSMEDIC_FSCA"
SOURCE_TYPE = "regulatory_recall"
COUNTRY = "Switzerland"


@dataclass
class SwissmedicFscaClientConfig:
    """HTTP settings for the Swissmedic FSCA publication API."""

    base_url: str = SWISSMEDIC_FSCA_BASE_URL
    search_endpoint: str = SWISSMEDIC_FSCA_SEARCH_ENDPOINT
    timeout: float = SWISSMEDIC_FSCA_TIMEOUT
    page_size: int = SWISSMEDIC_FSCA_PAGE_SIZE
    user_agent: str = SWISSMEDIC_FSCA_USER_AGENT

    @property
    def search_url(self) -> str:
        return urljoin(f"{self.base_url}/", self.search_endpoint.lstrip("/"))

    @property
    def search_page_url(self) -> str:
        return f"{self.base_url}/#/"


class SwissmedicFscaConnector(BaseSourceConnector):
    """Fetch Swissmedic field safety corrective action recall publications."""

    source_name = SOURCE_NAME
    source_type = SOURCE_TYPE
    display_name = "Swissmedic FSCA recalls"

    def __init__(self, client_config: SwissmedicFscaClientConfig | None = None) -> None:
        self.client_config = client_config or SwissmedicFscaClientConfig()

    def fetch(
        self,
        keywords: list[str],
        years: list[int],
        components: list[str] | None = None,
        accident_terms: list[str] | None = None,
        progress_reporter: ProgressReporter | None = None,
        **kwargs: Any,
    ) -> SourceFetchResult:
        """Fetch recall records for the requested keywords and years."""
        config = self.client_config
        if kwargs.get("request_timeout"):
            config.timeout = float(kwargs["request_timeout"])
        if kwargs.get("page_size"):
            config.page_size = int(kwargs["page_size"])
        try:
            records, warnings = fetch_swissmedic_fsca_direct(
                keywords=_merge_terms(keywords, components, accident_terms),
                years=years,
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
            logger.exception("Swissmedic FSCA fetch failed")
            return SourceFetchResult(
                source_name=self.source_name,
                records=ensure_unified_columns(pd.DataFrame()),
                success=False,
                error_message=f"Swissmedic FSCA fetch failed: {exc}",
                warnings=[f"Swissmedic FSCA fetch failed: {exc}"],
            )


def build_swissmedic_date_range(
    years: list[int],
    explicit_start_date: str | date | None = None,
    explicit_end_date: str | date | None = None,
) -> tuple[str, str]:
    """Build Swissmedic recall date range from selected years."""
    if explicit_start_date:
        start = str(explicit_start_date)
    else:
        start = f"{min(years) if years else date.today().year}-01-01"
    if explicit_end_date:
        end = str(explicit_end_date)
    else:
        latest_year = max(years) if years else date.today().year
        today = date.today()
        end = today.isoformat() if latest_year >= today.year else f"{latest_year}-12-31"
    return start, end


def build_swissmedic_payload(keyword: str, start_date: str, end_date: str, only_updates: bool = False) -> dict[str, Any]:
    """Build the Swissmedic FSCA search payload."""
    return {
        "queryTerm": keyword,
        "fromDate": start_date,
        "toDate": end_date,
        "onlyUpdates": only_updates,
    }


def fetch_swissmedic_page(
    keyword: str,
    page_number: int,
    start_date: str,
    end_date: str,
    session: requests.Session,
    client_config: SwissmedicFscaClientConfig,
    debug: bool = False,
) -> requests.Response:
    """Fetch one Swissmedic FSCA search page."""
    payload = build_swissmedic_payload(keyword, start_date, end_date)
    params = {
        "pageNumber": str(page_number),
        "sortingProperty": "PUBLICATION_DATE",
        "direction": "DESC",
    }
    response = session.post(
        client_config.search_url,
        params=params,
        json=payload,
        headers=_headers(client_config),
        timeout=client_config.timeout,
    )
    if debug:
        logger.debug(
            "Swissmedic POST %s params=%s payload=%s status=%s snippet=%s",
            client_config.search_url,
            params,
            payload,
            response.status_code,
            response.text[:1000],
        )
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code} for {response.url}: {response.text[:1000]}")
    return response


def parse_swissmedic_response(response_or_data: requests.Response | dict[str, Any]) -> dict[str, Any]:
    """Parse Swissmedic FSCA JSON response."""
    if isinstance(response_or_data, requests.Response):
        try:
            data = response_or_data.json()
        except ValueError as exc:
            raise RuntimeError(f"Swissmedic FSCA returned non-JSON response: {response_or_data.text[:1000]}") from exc
    else:
        data = response_or_data
    content = data.get("content", [])
    if not isinstance(content, list):
        raise RuntimeError("Swissmedic FSCA response content was not a list")
    return {
        "content": content,
        "total_elements": int(data.get("totalElements") or len(content)),
        "total_pages": int(data.get("totalPages") or 1),
        "page_number": int(data.get("number") or 0),
        "last": bool(data.get("last", True)),
    }


def fetch_swissmedic_fsca_direct(
    keywords: list[str],
    years: list[int],
    client_config: SwissmedicFscaClientConfig | None = None,
    progress_reporter: ProgressReporter | None = None,
    max_pages: int | None = None,
    debug: bool = False,
    session: requests.Session | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Fetch Swissmedic FSCA recall records with OR keyword semantics."""
    config = client_config or SwissmedicFscaClientConfig()
    session = session or requests.Session()
    start_date, end_date = build_swissmedic_date_range(years)
    warnings: list[str] = []
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    seen_hashes_by_term: dict[str, set[str]] = {}
    terms = _merge_terms(keywords, None, None)
    total_terms = max(1, len(terms))

    if progress_reporter:
        progress_reporter.update_source(
            SOURCE_NAME,
            current_step=0,
            total_steps=total_terms,
            records_fetched=0,
            stage="Searching Swissmedic FSCA",
            message=f"Date range {start_date} to {end_date}",
        )

    for term_index, keyword in enumerate(terms or [""], start=1):
        page = 0
        term_hashes = seen_hashes_by_term.setdefault(keyword.lower(), set())
        while True:
            response = fetch_swissmedic_page(keyword, page, start_date, end_date, session, config, debug=debug)
            page_hash = hashlib.sha256(response.content).hexdigest()
            if page_hash in term_hashes:
                warnings.append(f"Repeated Swissmedic response for keyword={keyword} page={page}; stopped pagination")
                break
            term_hashes.add(page_hash)
            parsed = parse_swissmedic_response(response)
            page_records = parsed["content"]
            new_count = 0
            for raw in page_records:
                normalized = normalize_swissmedic_record(raw, config)
                normalized["source_query_match"] = True
                key = normalized["event_id"]
                if key not in seen:
                    records.append(normalized)
                    seen.add(key)
                    new_count += 1

            if progress_reporter:
                progress_reporter.update_source(
                    SOURCE_NAME,
                    current_step=term_index,
                    total_steps=total_terms,
                    records_fetched=len(records),
                    stage="Fetching Swissmedic FSCA",
                    message=f"keyword={keyword} page={page + 1}/{parsed['total_pages']} fetched={len(page_records)} new={new_count}",
                )

            if parsed["last"] or page + 1 >= parsed["total_pages"]:
                break
            if max_pages and page + 1 >= max_pages:
                warnings.append(f"Stopped at max_pages={max_pages} for keyword={keyword}")
                break
            if not page_records or new_count == 0:
                break
            page += 1

    df = ensure_unified_columns(pd.DataFrame(records))
    warnings.append(f"swissmedic_fsca_records={len(df)}")
    return df, warnings


def normalize_swissmedic_record(raw: dict[str, Any], client_config: SwissmedicFscaClientConfig | None = None) -> dict[str, Any]:
    """Normalize one Swissmedic FSCA publication to the common schema."""
    config = client_config or SwissmedicFscaClientConfig()
    ref = str(raw.get("swissmedicRef") or "").strip()
    publication_date = format_fda_date(raw.get("publikationsDatum"))
    devices = raw.get("devices") if isinstance(raw.get("devices"), list) else []
    device_names = [str(device.get("handelsname") or "").strip() for device in devices if device.get("handelsname")]
    models = [str(device.get("model") or "").strip() for device in devices if device.get("model")]
    lots = [str(device.get("lot") or "").strip() for device in devices if device.get("lot")]
    classes = [str(device.get("beschreibungKlasse") or "").strip() for device in devices if device.get("beschreibungKlasse")]
    documents = raw.get("documents") if isinstance(raw.get("documents"), list) else []
    document_links = [
        {
            **doc,
            "download_url": build_swissmedic_document_url(ref, index, config),
        }
        for index, doc in enumerate(documents)
    ]
    event_link = _preferred_document_link(document_links) or build_swissmedic_search_link("", publication_date, publication_date)
    narrative_parts = [
        raw.get("begruendung"),
        raw.get("status"),
        "; ".join(device_names),
        "; ".join(classes),
        "; ".join(models),
        "; ".join(lots),
    ]
    source_specific = {
        "swissmedic_ref": ref,
        "publication_date": publication_date,
        "status": raw.get("status"),
        "status_date": format_fda_date(raw.get("statusDatum")),
        "reason": raw.get("begruendung"),
        "released": raw.get("freigeschaltet"),
        "devices": devices,
        "documents": document_links,
    }
    record = {
        "source": SOURCE_NAME,
        "source_type": SOURCE_TYPE,
        "event_id": ref,
        "report_number": ref,
        "date_of_event": "",
        "date_received": publication_date,
        "event_date": publication_date,
        "year": parse_year_from_date(publication_date),
        "received_year": parse_year_from_date(publication_date),
        "country": COUNTRY,
        "manufacturer": str(raw.get("hersteller") or "").strip(),
        "product_name": "; ".join(device_names),
        "brand_names": "; ".join(device_names),
        "generic_name": "; ".join(classes),
        "device_model": "; ".join(models),
        "product_code": "",
        "event_type": str(raw.get("status") or "Recall"),
        "patient_outcome": "",
        "device_problem_text": str(raw.get("begruendung") or ""),
        "patient_problem_text": "",
        "narrative_text": " | ".join(str(part) for part in narrative_parts if part),
        "raw_link": build_swissmedic_search_link("", publication_date, publication_date),
        "event_link": event_link,
        "source_category": str(raw.get("status") or ""),
        "source_category_name": str(raw.get("status") or ""),
        "source_category_system": "Swissmedic FSCA status",
        "record_hash": "",
        "source_specific": json.dumps(source_specific, ensure_ascii=False, default=str),
        "raw_record": json.dumps(raw, ensure_ascii=False, default=str),
    }
    record["record_hash"] = _record_hash(record)
    return record


def build_swissmedic_document_url(reference: str, document_index: int, client_config: SwissmedicFscaClientConfig | None = None) -> str:
    """Build direct Swissmedic FSCA document URL."""
    config = client_config or SwissmedicFscaClientConfig()
    return f"{config.base_url}/api/publications/{reference}/documents/{document_index}"


def build_swissmedic_search_link(query: str, start_date: str, end_date: str) -> str:
    """Build human-facing Swissmedic FSCA search URL."""
    params = {
        "q": query,
        "from": start_date,
        "to": end_date,
        "onlyUpdates": "false",
        "sort": "PUBLICATION_DATE",
        "direction": "DESC",
    }
    return f"https://fsca.swissmedic.ch/mep/#/?{urlencode(params)}"


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


def _headers(config: SwissmedicFscaClientConfig) -> dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "User-Agent": config.user_agent,
        "Referer": f"{config.base_url}/",
    }


def _record_hash(record: dict[str, Any]) -> str:
    basis = "|".join(
        [
            str(record.get("source", "")),
            str(record.get("event_id", "")),
            str(record.get("event_date", "")),
            str(record.get("manufacturer", "")),
            str(record.get("product_name", ""))[:300],
        ]
    )
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def _preferred_document_link(documents: list[dict[str, Any]]) -> str:
    """Prefer the English Swissmedic document link when available."""
    if not documents:
        return ""
    for doc in documents:
        if str(doc.get("language") or "").upper() == "EN":
            return str(doc.get("download_url") or "")
    return str(documents[0].get("download_url") or "")
