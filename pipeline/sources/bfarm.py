"""BfArM medical device recall / manufacturer action connector."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, replace
from datetime import date
from typing import Any
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

from pipeline.progress import ProgressReporter
from pipeline.sources.base import BaseSourceConnector, SourceFetchResult, ensure_unified_columns
from pipeline.utils import date_or_year_in_range, format_fda_date, parse_year_from_date
from pipeline.source_support import request_with_retry, reject_challenge_html, with_coverage


logger = logging.getLogger(__name__)

SOURCE_NAME = "BFARM_RECALLS"
SOURCE_TYPE = "regulatory_recall"
COUNTRY = "Germany"


@dataclass
class BfarmClientConfig:
    """HTTP settings for BfArM expert search."""

    base_url: str = "https://www.bfarm.de"
    search_path: str = "/SiteGlobals/Forms/Suche/EN/Expertensuche_Formular.html"
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
        config = replace(self.client_config)
        if kwargs.get("request_timeout"):
            config.timeout = float(kwargs["request_timeout"])
        try:
            df, warnings = fetch_bfarm_recalls_direct(
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
    start_date: object | None = None,
    end_date: object | None = None,
    client_config: BfarmClientConfig | None = None,
    progress_reporter: ProgressReporter | None = None,
    max_pages: int | None = None,
    debug: bool = False,
    session: requests.Session | None = None,
    max_records: int | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Fetch BfArM recall notices from the public expert search form."""
    config = client_config or BfarmClientConfig()
    session = session or requests.Session()
    warnings: list[str] = []
    start_date_text = _normalize_bfarm_date(start_date) if start_date is not None else None
    end_date_text = _normalize_bfarm_date(end_date) if end_date is not None else None
    terms = _bfarm_search_terms(_merge_terms(keywords, None, None))
    records_by_key: dict[str, dict[str, Any]] = {}
    desired_years = set(int(year) for year in years)
    total_steps = max(1, len(terms) * 2)
    step = 0
    failures = 0
    capped = False
    page_limit = int(max_pages) if max_pages is not None else 100
    pages_fetched = 0
    if page_limit < 1 or (max_records is not None and max_records < 1):
        raise ValueError("BfArM retrieval limits must be positive")

    for keyword in terms or [""]:
        if pages_fetched >= page_limit or (max_records and len(records_by_key) >= max_records):
            capped = True
            break
        step += 1
        try:
            initial_response = fetch_bfarm_search_page(keyword, session=session, client_config=config, debug=debug)
        except (requests.RequestException, RuntimeError) as exc:
            failures += 1
            warnings.append(str(exc))
            continue
        period_links = extract_bfarm_period_links(initial_response.text, initial_response.url)
        selected_links = [
            link
            for link in period_links
            if link["year"] in desired_years
        ]
        if not selected_links:
            selected_links = [{"year": 0, "url": initial_response.url, "label": "all"}]
        if progress_reporter:
            progress_reporter.update_source(
                SOURCE_NAME,
                current_step=step,
                total_steps=total_steps,
                records_fetched=len(records_by_key),
                stage="Selecting BfArM period filters",
                message=f"keyword={keyword} period_links={len(selected_links)}",
            )

        for link in selected_links:
            step += 1
            page_url: str | None = link["url"]
            seen_page_urls: set[str] = set()
            seen_page_hashes: set[str] = set()
            while page_url:
                if pages_fetched >= page_limit or (max_records and len(records_by_key) >= max_records):
                    capped = True
                    break
                if page_url in seen_page_urls:
                    failures += 1
                    warnings.append("BfArM pagination repeated a URL; coverage incomplete")
                    break
                seen_page_urls.add(page_url)
                try:
                    response = fetch_bfarm_url(page_url, session=session, client_config=config, debug=debug)
                    pages_fetched += 1
                    page_hash = hashlib.sha256(response.content).hexdigest()
                    if page_hash in seen_page_hashes:
                        raise RuntimeError("BfArM pagination repeated a result page")
                    seen_page_hashes.add(page_hash)
                    records = parse_bfarm_results(response.text, response.url)
                    if not records and not re.search(r"no (?:results|matches)|0 results|keine (?:treffer|ergebnisse)", response.text, re.I):
                        raise RuntimeError("BfArM listing format not recognized; empty search not verified")
                    records = [record for record in records if _is_medical_device_customer_information(record)]
                    _add_bfarm_records(records, records_by_key, desired_years, start_date=start_date_text, end_date=end_date_text)
                    if max_records and len(records_by_key) > max_records:
                        records_by_key = dict(list(records_by_key.items())[:max_records])
                        capped = True
                    page_url = _bfarm_next_page(response.text, response.url, config.base_url)
                except (requests.RequestException, RuntimeError) as exc:
                    failures += 1
                    warnings.append(str(exc))
                    break
            if progress_reporter:
                progress_reporter.update_source(
                    SOURCE_NAME,
                    current_step=step,
                    total_steps=max(total_steps, step),
                    records_fetched=len(records_by_key),
                    stage="Fetching BfArM period results",
                    message=f"keyword={keyword} period={link['label']} records={len(records_by_key)}",
                )

    normalized = list(records_by_key.values())
    if progress_reporter:
        progress_reporter.complete_source(SOURCE_NAME, records_fetched=len(normalized), message="BfArM completed")
    warnings.append(f"bfarm_records={len(normalized)}")
    if capped:
        warnings.append(f"BfArM retrieval capped: max_pages={page_limit}, max_records={max_records}")
    warnings.append("BfArM HTML listing coverage is unverified; retrieved notices are not an exhaustive incident database")
    df = ensure_unified_columns(pd.DataFrame(normalized))
    coverage = "failed" if failures and not normalized else "partial" if failures or capped else "unverified"
    return with_coverage(df, coverage, warnings), warnings


def _bfarm_next_page(html: str, page_url: str, base_url: str) -> str | None:
    """Follow only an explicit next link on the configured official host."""
    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.find_all("a", href=True):
        label = " ".join([anchor.get_text(" ", strip=True), str(anchor.get("aria-label", "")), str(anchor.get("title", ""))])
        rel = anchor.get("rel", [])
        if "next" not in rel and not re.search(r"\b(next|weiter|nächste)\b|^[›»]$", label.strip(), re.I):
            continue
        url = _absolute_bfarm_url(page_url, anchor["href"])
        if urlparse(url).scheme not in {"http", "https"} or urlparse(url).hostname != urlparse(base_url).hostname:
            raise RuntimeError("BfArM next-page link points outside the configured source")
        return url
    return None


def _has_records_for_all_desired_years(records_by_key: dict[str, dict[str, Any]], desired_years: set[int]) -> bool:
    if not desired_years or not records_by_key:
        return False
    found_years = {
        int(record["year"])
        for record in records_by_key.values()
        if record.get("year") is not None
    }
    return desired_years.issubset(found_years)


def fetch_bfarm_search_page(
    keyword: str,
    session: requests.Session,
    client_config: BfarmClientConfig,
    debug: bool = False,
    year_facet: str | None = None,
) -> requests.Response:
    """Submit one BfArM expert search request."""
    params = build_bfarm_search_payload(keyword, year_facet=year_facet)
    response = request_with_retry(session, "GET",
        client_config.search_url,
        params=params,
        headers=_headers(client_config),
        timeout=client_config.timeout,
    )
    if debug:
        logger.debug("BfArM GET %s params=%s status=%s snippet=%s", client_config.search_url, params, response.status_code, response.text[:1000])
    reject_challenge_html(response.text, "BfArM")
    return response


def fetch_bfarm_url(
    url: str,
    session: requests.Session,
    client_config: BfarmClientConfig,
    debug: bool = False,
) -> requests.Response:
    """Fetch a BfArM result/filter URL with a small retry for rate limiting."""
    response = request_with_retry(session, "GET", url, headers=_headers(client_config), timeout=client_config.timeout)
    reject_challenge_html(response.text, "BfArM")
    return response


def build_bfarm_search_payload(keyword: str, year_facet: str | None = None) -> dict[str, str]:
    """Build the BfArM expert search query parameters."""
    params = {
        "templateQueryString": keyword,
        "submit": "Send",
        "resultsPerPage": "100",
        "sortOrder": "dateOfIssue_dt desc",
    }
    if year_facet:
        params["dateOfIssue_dt"] = year_facet
    return params


def build_bfarm_year_facets(years: list[int]) -> list[str]:
    """Map selected years to BfArM relative date facets.

    BfArM exposes year filters as relative facets, not explicit start/end
    dates. In 2026 the useful mappings are:
    thisyear=2026, lastyear=2025, penultimateyear=2024.
    Older years are fetched through ``older`` and then filtered locally.
    """
    current_year = date.today().year
    facets: list[str] = []
    mapping = {
        current_year: "thisyear",
        current_year - 1: "lastyear",
        current_year - 2: "penultimateyear",
    }
    for year in sorted(set(int(y) for y in years), reverse=True):
        facet = mapping.get(year, "older")
        if facet not in facets:
            facets.append(facet)
    return facets


def extract_bfarm_period_links(html: str, page_url: str) -> list[dict[str, Any]]:
    """Extract BfArM Period facet links from an initial search result page."""
    soup = BeautifulSoup(html, "html.parser")
    links: list[dict[str, Any]] = []
    seen: set[int] = set()
    for anchor in soup.find_all("a", href=True):
        label = anchor.get_text(" ", strip=True)
        href = anchor["href"]
        if "dateOfIssue_dt=" not in href:
            continue
        match = re.search(r"\b(20\d{2})\b", label)
        if not match:
            continue
        year = int(match.group(1))
        if year in seen:
            continue
        seen.add(year)
        links.append({"year": year, "label": label, "url": _absolute_bfarm_url(page_url, href)})
    return links


def parse_bfarm_results(html: str, page_url: str) -> list[dict[str, Any]]:
    """Parse BfArM manufacturer measure cards into raw records."""
    soup = BeautifulSoup(html, "html.parser")
    results = soup.select_one("#results") or soup
    cards = results.select("li.l-teaser-list__item")
    if not cards:
        cards = results.select(".c-icon-teaser, article, .c-teaser")
    records: list[dict[str, Any]] = []
    for card in cards:
        text = card.get_text(" ", strip=True)
        if not text:
            continue
        link = card.find("a", href=True)
        title = link.get_text(" ", strip=True) if link else text[:200]
        title = re.sub(r"\s*PDF,.*$", "", title).strip()
        date_node = card.select_one(".c-icon-teaser__date")
        topic_node = card.select_one(".c-icon-teaser__topic")
        type_node = card.select_one(".c-icon-teaser__category")
        reference = _extract_reference(text)
        records.append(
            {
                "title": title,
                "text": text,
                "url": _absolute_bfarm_url(page_url, link["href"]) if link else page_url,
                "date": _clean_labeled_text(date_node.get_text(" ", strip=True)) if date_node else "",
                "topic": _clean_labeled_text(topic_node.get_text(" ", strip=True)) if topic_node else "",
                "notice_type": _clean_labeled_text(type_node.get_text(" ", strip=True)) if type_node else "",
                "reference": reference,
            }
        )
    return records


def normalize_bfarm_record(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize one BfArM recall record to the unified schema."""
    title = str(raw.get("title") or "").strip()
    text = str(raw.get("text") or "").strip()
    reference = str(raw.get("reference") or "").strip()
    event_id = reference or _stable_id(title, text, str(raw.get("url") or ""))
    publication_date = format_fda_date(raw.get("date"))
    search_date = publication_date or _extract_date(text)
    date_basis = "notice_publication_date" if publication_date else "listing_text_date" if search_date else "unknown"
    notice_type = str(raw.get("notice_type") or "Customer information").strip()
    record = {
        "source": SOURCE_NAME,
        "source_type": SOURCE_TYPE,
        "event_id": event_id,
        "report_number": event_id,
        "date_of_event": "",
        "date_received": "",
        "report_date": publication_date,
        "event_date": "",
        "search_date": search_date,
        "search_date_basis": date_basis,
        "year": parse_year_from_date(search_date),
        "received_year": None,
        "country": COUNTRY,
        "manufacturer": _extract_manufacturer(title),
        "product_name": title,
        "brand_names": title,
        "generic_name": "",
        "device_model": "",
        "product_code": "",
        "event_type": notice_type,
        "patient_outcome": "",
        "device_problem_text": text,
        "patient_problem_text": "",
        "narrative_text": text,
        "raw_link": raw.get("url", ""),
        "event_link": raw.get("url", ""),
        "source_category": notice_type,
        "source_category_name": notice_type,
        "source_category_code": reference,
        "source_category_system": "BfArM expert search",
        "record_hash": "",
        "source_specific": json.dumps({**raw, "date_basis": date_basis}, ensure_ascii=False, default=str),
        "raw_record": json.dumps(raw, ensure_ascii=False, default=str),
    }
    record["record_hash"] = hashlib.sha256(
        "|".join([str(record.get("source")), str(record.get("event_id")), search_date, str(record.get("product_name"))]).encode("utf-8")
    ).hexdigest()
    return record


def _add_bfarm_records(
    records: list[dict[str, Any]],
    records_by_key: dict[str, dict[str, Any]],
    desired_years: set[int],
    start_date: object | None = None,
    end_date: object | None = None,
) -> None:
    for raw in records:
        rec = normalize_bfarm_record(raw)
        rec["source_query_match"] = True
        if desired_years and rec.get("year") not in desired_years:
            continue
        if (start_date is not None or end_date is not None) and not date_or_year_in_range(rec.get("search_date"), start_date, end_date):
            continue
        key = rec["event_id"]
        existing = records_by_key.get(key)
        if existing is None or _is_english_record(rec) and not _is_english_record(existing):
            records_by_key[key] = rec


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


def _normalize_bfarm_date(value: object) -> str:
    text = str(value).strip()
    if not text:
        return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    return format_fda_date(text)


def _is_medical_device_customer_information(raw: dict[str, Any]) -> bool:
    topic = str(raw.get("topic") or "").lower()
    notice_type = str(raw.get("notice_type") or "").lower()
    text = str(raw.get("text") or "").lower()
    return (
        "medical devices" in topic
        and "customer information" in notice_type
        and "reference" in text
    )


def _absolute_bfarm_url(page_url: str, href: str) -> str:
    if href.startswith("SharedDocs/") or href.startswith("SiteGlobals/"):
        return urljoin("https://www.bfarm.de/", href)
    return urljoin(page_url, href)


def _is_english_record(record: dict[str, Any]) -> bool:
    link = str(record.get("event_link") or record.get("raw_link") or "")
    raw = str(record.get("raw_record") or "")
    return "/EN/" in link or "/EN/" in raw or "_en." in link.lower() or "_en." in raw.lower()


def _extract_reference(text: str) -> str:
    match = re.search(r"\bReference\s+([0-9]{5}/[0-9]{2})\b", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else ""


def _clean_labeled_text(text: str) -> str:
    cleaned = re.sub(r"\b(Date|Topics|Type):\s*", "", text, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _headers(config: BfarmClientConfig) -> dict[str, str]:
    return {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": config.user_agent,
        "Referer": config.search_url,
    }


def _merge_terms(keywords: list[str], components: list[str] | None, accident_terms: list[str] | None) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    for value in [*keywords, *(components or []), *(accident_terms or [])]:
        term = str(value).strip()
        if term and term.lower() not in seen:
            seen.add(term.lower())
            terms.append(term)
    return terms


def _bfarm_search_terms(terms: list[str]) -> list[str]:
    """Use focused BfArM product terms to avoid rate limiting on broad incident words."""
    # Choosing a query budget may limit a run, but must never silently discard
    # requested brands or replace them with a generic product term.
    return _dedupe_terms(["Trocar" if term.lower() == "trocar" else term for term in terms])


def _dedupe_terms(terms: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for term in terms:
        key = term.lower()
        if key not in seen:
            seen.add(key)
            result.append(term)
    return result


def _stable_id(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]
