"""BfArM medical device recall / manufacturer action connector."""

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

from pipeline.progress import ProgressReporter
from pipeline.sources.base import BaseSourceConnector, SourceFetchResult, ensure_unified_columns
from pipeline.utils import format_fda_date, parse_year_from_date


logger = logging.getLogger(__name__)

SOURCE_NAME = "BFARM_RECALLS"
SOURCE_TYPE = "regulatory_recall"
COUNTRY = "Germany"


@dataclass
class BfarmClientConfig:
    """HTTP settings for BfArM manufacturer measures pages."""

    base_url: str = "https://www.bfarm.de"
    search_path: str = "/DE/Medizinprodukte/Aufgaben/Risikobewertung-und-Forschung/Massnahmen-von-Herstellern/_node.html"
    timeout: float = 60.0
    user_agent: str = "Mozilla/5.0 (compatible; Codex BfArM Recall Connector)"

    @property
    def search_url(self) -> str:
        return urljoin(self.base_url.rstrip("/") + "/", self.search_path.lstrip("/"))


class BfarmRecallConnector(BaseSourceConnector):
    """Fetch German BfArM recall / field safety information records."""

    source_name = SOURCE_NAME
    source_type = SOURCE_TYPE
    display_name = "BfArM medical device recalls"

    def __init__(self, client_config: BfarmClientConfig | None = None) -> None:
        self.client_config = client_config or BfarmClientConfig()

    def fetch(
        self,
        keywords: list[str],
        years: list[int],
        components: list[str] | None = None,
        accident_terms: list[str] | None = None,
        progress_reporter: ProgressReporter | None = None,
        **kwargs: Any,
    ) -> SourceFetchResult:
        config = self.client_config
        try:
            df, warnings = fetch_bfarm_recalls_direct(
                keywords=_merge_terms(keywords, components, accident_terms),
                years=years,
                client_config=config,
                progress_reporter=progress_reporter,
                max_pages=int(kwargs["max_pages"]) if kwargs.get("max_pages") else None,
                max_records=int(kwargs["max_records"]) if kwargs.get("max_records") else None,
                debug=bool(kwargs.get("debug", False)),
                session=kwargs.get("session"),
            )
            return SourceFetchResult(
                source_name=self.source_name,
                records=ensure_unified_columns(df),
                success=True,
                warnings=warnings,
            )
        except Exception as exc:
            logger.exception("BfArM recall fetch failed")
            return SourceFetchResult(
                source_name=self.source_name,
                records=ensure_unified_columns(pd.DataFrame()),
                success=False,
                error_message=f"BfArM recall fetch failed: {exc}",
                warnings=[f"BfArM recall fetch failed: {exc}"],
            )


def fetch_bfarm_recalls_direct(
    keywords: list[str],
    years: list[int],
    client_config: BfarmClientConfig | None = None,
    progress_reporter: ProgressReporter | None = None,
    max_pages: int | None = None,
    max_records: int | None = None,
    debug: bool = False,
    session: requests.Session | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Fetch BfArM recall notices from the public manufacturer measures page."""
    config = client_config or BfarmClientConfig()
    session = session or requests.Session()
    warnings: list[str] = []
    response = session.get(config.search_url, headers={"User-Agent": config.user_agent}, timeout=config.timeout)
    if debug:
        logger.debug("BfArM GET %s status=%s snippet=%s", response.url, response.status_code, response.text[:1000])
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code} for {response.url}: {response.text[:1000]}")

    records = parse_bfarm_results(response.text, response.url)
    if progress_reporter:
        progress_reporter.update_source(
            SOURCE_NAME,
            current_step=1,
            total_steps=1,
            records_fetched=len(records),
            stage="Parsing BfArM recalls",
            message=f"records={len(records)}",
        )

    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in records:
        rec = normalize_bfarm_record(raw)
        if years and rec.get("year") not in years:
            continue
        if keywords and not _matches_keywords(rec, keywords):
            continue
        key = rec["event_id"]
        if key in seen:
            continue
        seen.add(key)
        normalized.append(rec)
        if max_records and len(normalized) >= max_records:
            warnings.append(f"Stopped at max_records={max_records}")
            break

    if progress_reporter:
        progress_reporter.complete_source(SOURCE_NAME, records_fetched=len(normalized), message="BfArM completed")
    warnings.append(f"bfarm_records={len(normalized)}")
    return ensure_unified_columns(pd.DataFrame(normalized)), warnings


def parse_bfarm_results(html: str, page_url: str) -> list[dict[str, Any]]:
    """Parse BfArM manufacturer measure cards into raw records."""
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("article, .c-teaser, .modul, .tile, li")
    records: list[dict[str, Any]] = []
    for card in cards:
        text = card.get_text(" ", strip=True)
        if not text:
            continue
        link = card.find("a", href=True)
        title = link.get_text(" ", strip=True) if link else text[:200]
        records.append(
            {
                "title": title,
                "text": text,
                "url": urljoin(page_url, link["href"]) if link else page_url,
            }
        )
    return records


def normalize_bfarm_record(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize one BfArM recall record to the unified schema."""
    title = str(raw.get("title") or "").strip()
    text = str(raw.get("text") or "").strip()
    event_id = _stable_id(title, text, str(raw.get("url") or ""))
    event_date = _extract_date(text)
    record = {
        "source": SOURCE_NAME,
        "source_type": SOURCE_TYPE,
        "event_id": event_id,
        "report_number": event_id,
        "date_of_event": "",
        "date_received": event_date,
        "event_date": event_date,
        "year": parse_year_from_date(event_date),
        "received_year": parse_year_from_date(event_date),
        "country": COUNTRY,
        "manufacturer": _extract_manufacturer(title),
        "product_name": title,
        "brand_names": title,
        "generic_name": "",
        "device_model": "",
        "product_code": "",
        "event_type": "Recall notice",
        "patient_outcome": "",
        "device_problem_text": text,
        "patient_problem_text": "",
        "narrative_text": text,
        "raw_link": raw.get("url", ""),
        "event_link": raw.get("url", ""),
        "source_category": "Recall notice",
        "source_category_name": "Recall notice",
        "source_category_system": "BfArM manufacturer measures",
        "record_hash": "",
        "source_specific": json.dumps(raw, ensure_ascii=False, default=str),
        "raw_record": json.dumps(raw, ensure_ascii=False, default=str),
    }
    record["record_hash"] = hashlib.sha256(
        "|".join([str(record.get("source")), str(record.get("event_id")), str(record.get("event_date")), str(record.get("product_name"))]).encode("utf-8")
    ).hexdigest()
    return record


def _matches_keywords(record: dict[str, Any], keywords: list[str]) -> bool:
    haystack = " ".join([str(record.get("product_name", "")), str(record.get("manufacturer", "")), str(record.get("narrative_text", ""))]).lower()
    return any(keyword.lower() in haystack for keyword in keywords)


def _extract_manufacturer(title: str) -> str:
    if " - " in title:
        return title.split(" - ", 1)[0].strip()
    return ""


def _extract_date(text: str) -> str:
    for pattern in [
        r"\b\d{4}-\d{2}-\d{2}\b",
        r"\b\d{1,2}\.\d{1,2}\.\d{4}\b",
        r"\b\d{1,2}\s+[A-Za-zÄÖÜäöüß]+\s+\d{4}\b",
    ]:
        match = re.search(pattern, text)
        if match:
            return format_fda_date(match.group(0))
    return ""


def _merge_terms(keywords: list[str], components: list[str] | None, accident_terms: list[str] | None) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    for value in [*keywords, *(components or []), *(accident_terms or [])]:
        term = str(value).strip()
        if term and term.lower() not in seen:
            seen.add(term.lower())
            terms.append(term)
    return terms


def _stable_id(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]
