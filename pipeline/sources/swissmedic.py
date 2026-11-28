"""Swissmedic FSCA medical device recall connector."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, replace
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
from pipeline.source_support import request_with_retry, with_coverage


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
        config = replace(self.client_config)
        if kwargs.get("request_timeout"):
            config.timeout = float(kwargs["request_timeout"])
        if kwargs.get("page_size"):
            config.page_size = int(kwargs["page_size"])
        try:
            records, warnings = fetch_swissmedic_fsca_direct(
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
        start = explicit_start_date.isoformat() if isinstance(explicit_start_date, date) else str(explicit_start_date)
    else:
        start = f"{min(years) if years else date.today().year}-01-01"
    if explicit_end_date:
        end = explicit_end_date.isoformat() if isinstance(explicit_end_date, date) else str(explicit_end_date)
    else:
        latest_year = max(years) if years else date.today().year
        today = date.today()
        end = today.isoformat() if latest_year >= today.year else f"{latest_year}-12-31"
    if end and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", end):
        end = format_fda_date(end)
    if start and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", start):
        start = format_fda_date(start)
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
    response = request_with_retry(session, "POST",
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
    if not isinstance(data, dict) or data.get("error") or "content" not in data:
        raise RuntimeError("Swissmedic response has an error or is missing the content array")
    content = data["content"]
    if not isinstance(content, list):
        raise RuntimeError("Swissmedic FSCA response content was not a list")
    if any(not isinstance(item, dict) for item in content):
        raise RuntimeError("Swissmedic result entry is not an object")
    pagination_verified = all(key in data for key in ("totalElements", "totalPages", "number", "last"))
    if "last" in data and not isinstance(data["last"], bool):
        raise RuntimeError("Swissmedic pagination last flag is not boolean")
    total_elements = int(data.get("totalElements", len(content)))
    total_pages = int(data.get("totalPages", 1))
    page_number = int(data.get("number", 0))
    if total_elements < len(content) or total_elements < 0 or total_pages < 0 or page_number < 0:
        raise RuntimeError("Swissmedic pagination counts contradict its returned rows")
    if content and (total_pages == 0 or page_number >= total_pages):
        raise RuntimeError("Swissmedic page number contradicts the total pages")
    return {
        "content": content,
        "total_elements": total_elements,
        "total_pages": total_pages,
        "page_number": page_number,
        "last": bool(data.get("last", True)),
        "pagination_verified": pagination_verified,
    }


def fetch_swissmedic_fsca_direct(
    keywords: list[str],
    years: list[int],
    start_date: object | None = None,
    end_date: object | None = None,
    client_config: SwissmedicFscaClientConfig | None = None,
    progress_reporter: ProgressReporter | None = None,
    max_pages: int | None = None,
    debug: bool = False,
    session: requests.Session | None = None,
    max_records: int | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Fetch Swissmedic FSCA recall records with OR keyword semantics."""
    config = client_config or SwissmedicFscaClientConfig()
    session = session or requests.Session()
    start_date, end_date = build_swissmedic_date_range(years, explicit_start_date=start_date, explicit_end_date=end_date)
    warnings: list[str] = []
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    seen_hashes_by_term: dict[str, set[str]] = {}
    terms = _merge_terms(keywords, None, None)
    total_terms = max(1, len(terms))
    failures = 0
    capped = False
    pagination_unverified = False
    page_limit = int(max_pages) if max_pages is not None else 100
    if page_limit < 1 or (max_records is not None and max_records < 1):
        raise ValueError("Swissmedic retrieval limits must be positive")

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
        if max_records and len(records) >= max_records:
            capped = True
            break
        page = 0
        fetched_for_term = 0
        expected_for_term: int | None = None
        term_hashes = seen_hashes_by_term.setdefault(keyword.lower(), set())
        while True:
            try:
                response = fetch_swissmedic_page(keyword, page, start_date, end_date, session, config, debug=debug)
                parsed = parse_swissmedic_response(response)
                if parsed["pagination_verified"] and parsed["page_number"] != page:
                    raise RuntimeError("Swissmedic returned an unexpected page number")
                if parsed["pagination_verified"]:
                    if expected_for_term is not None and parsed["total_elements"] != expected_for_term:
                        raise RuntimeError("Swissmedic total changed during pagination")
                    if fetched_for_term + len(parsed["content"]) > parsed["total_elements"]:
                        raise RuntimeError("Swissmedic total contradicts the paginated rows")
                    expected_for_term = parsed["total_elements"]
            except (requests.RequestException, RuntimeError, ValueError, TypeError) as exc:
                failures += 1
                warnings.append(f"Swissmedic keyword={keyword} page={page} failed: {type(exc).__name__}: {exc}")
                break
            page_hash = hashlib.sha256(json.dumps(parsed["content"], sort_keys=True).encode()).hexdigest()
            if page_hash in term_hashes:
                failures += 1
                warnings.append(f"Repeated Swissmedic response for keyword={keyword} page={page}; stopped pagination")
                break
            term_hashes.add(page_hash)
            pagination_unverified = pagination_unverified or not parsed["pagination_verified"]
            page_records = parsed["content"]
            fetched_for_term += len(page_records)
            new_count = 0
            for raw in page_records:
                try:
                    normalized = normalize_swissmedic_record(
                        raw, config, search_link=build_swissmedic_search_link(keyword, start_date, end_date),
                    )
                except (AttributeError, TypeError, ValueError) as exc:
                    failures += 1
                    warnings.append(f"Swissmedic malformed record could not be normalized: {type(exc).__name__}")
                    continue
                normalized["source_query_match"] = True
                key = normalized["event_id"]
                if not key:
                    key = hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()
                    normalized["event_id"] = key
                    normalized["report_number"] = key
                    pagination_unverified = True
                    warnings.append("Swissmedic record without reference retained with content identity")
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

            if max_records and len(records) > max_records:
                records = records[:max_records]
                capped = True
                break
            if parsed["last"] or page + 1 >= parsed["total_pages"]:
                if parsed["pagination_verified"] and fetched_for_term < parsed["total_elements"]:
                    failures += 1
                    warnings.append(f"Swissmedic keyword={keyword} ended before its reported total")
                break
            if page + 1 >= page_limit or (max_records and len(records) >= max_records):
                capped = True
                warnings.append(f"Swissmedic retrieval capped: max_pages={page_limit}, max_records={max_records}")
                break
            if not page_records:
                failures += 1
                warnings.append("Swissmedic returned an empty page before pagination finished")
                break
            page += 1

    df = ensure_unified_columns(pd.DataFrame(records))
    warnings.append(f"swissmedic_fsca_records={len(df)}")
    coverage = "failed" if failures and not records else "partial" if failures or capped else "unverified" if pagination_unverified else "complete"
    if capped and not any("capped" in warning for warning in warnings):
        warnings.append(f"Swissmedic retrieval capped at max_records={max_records}")
    return with_coverage(df, coverage, warnings), warnings


def normalize_swissmedic_record(
    raw: dict[str, Any],
    client_config: SwissmedicFscaClientConfig | None = None,
    search_link: str | None = None,
) -> dict[str, Any]:
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
    search_link = search_link or build_swissmedic_search_link("", publication_date, publication_date)
    event_link = search_link
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
        "date_basis": "publication_date",
        "status": raw.get("status"),
        "status_date": format_fda_date(raw.get("statusDatum")),
        "reason": raw.get("begruendung"),
        "released": raw.get("freigeschaltet"),
        "devices": devices,
        "documents": document_links,
        "search_link": search_link,
        "download_link": _preferred_document_link(document_links),
    }
    record = {
        "source": SOURCE_NAME,
        "source_type": SOURCE_TYPE,
        "event_id": ref,
        "report_number": ref,
        "date_of_event": "",
        "date_received": "",
        "report_date": publication_date,
        "event_date": "",
        "search_date": publication_date,
        "search_date_basis": "publication_date" if publication_date else "unknown",
        "year": parse_year_from_date(publication_date),
        "received_year": None,
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
        "raw_link": search_link,
        "event_link": event_link,
        "device_link": _preferred_document_link(document_links),
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
            str(record.get("search_date", "")),
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
