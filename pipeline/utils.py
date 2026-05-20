"""Shared cleaning, date parsing, and filtering helpers."""

from __future__ import annotations

import re
from typing import Iterable

from dateutil import parser as dt_parser


def normalize_text(value: object | None) -> str:
    """Return lowercase text with null-like values removed and whitespace collapsed."""
    if value is None:
        return ""
    text = str(value)
    if text.lower() == "nan":
        return ""
    return re.sub(r"\s+", " ", text.lower()).strip()


def parse_year_from_date(value: object | None) -> int | None:
    """Parse a year from FDA YYYYMMDD strings or general date-like values."""
    if value is None:
        return None

    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None

    if re.fullmatch(r"\d{8}", text):
        return int(text[:4])

    try:
        return dt_parser.parse(text, fuzzy=True).year
    except (TypeError, ValueError, OverflowError):
        return None


def format_fda_date(value: object | None) -> str:
    """Convert complete date strings to YYYY-MM-DD, preserving partial dates.

    ``dateutil`` fills missing day/month values from today's date. That is unsafe
    for recall sources that sometimes publish incomplete dates such as
    ``March 2024`` or ``2024``. Those values are kept as source-provided text.
    """
    if value is None:
        return ""

    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""

    if re.fullmatch(r"\d{8}", text):
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"

    if not _looks_like_complete_date(text):
        return text

    try:
        parsed = dt_parser.parse(text, fuzzy=True)
        return parsed.strftime("%Y-%m-%d")
    except (TypeError, ValueError, OverflowError):
        return text


def _looks_like_complete_date(text: str) -> bool:
    """Return True when text appears to include year, month, and day."""
    normalized = text.strip()
    if re.fullmatch(r"\d{4}[-/.]\d{1,2}[-/.]\d{1,2}", normalized):
        return True
    if re.fullmatch(r"\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4}", normalized):
        return True
    month_names = (
        "jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|"
        "aug|august|sep|sept|september|oct|october|nov|november|dec|december|"
        "januar|februar|märz|maerz|april|mai|juni|juli|august|september|"
        "oktober|november|dezember"
    )
    return bool(
        re.search(rf"\b\d{{1,2}}\s+({month_names})\s+\d{{4}}\b", normalized, flags=re.IGNORECASE)
        or re.search(rf"\b\d{{1,2}}\.\s*({month_names})\s+\d{{4}}\b", normalized, flags=re.IGNORECASE)
        or re.search(rf"\b({month_names})\s+\d{{1,2}},?\s+\d{{4}}\b", normalized, flags=re.IGNORECASE)
    )


def contains_any_keyword(text: object | None, keywords: Iterable[str]) -> bool:
    """Return True when normalized text contains at least one keyword."""
    normalized = normalize_text(text)
    return any(keyword.lower() in normalized for keyword in keywords)
