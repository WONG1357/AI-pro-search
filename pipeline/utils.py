"""Shared cleaning, date parsing, and filtering helpers."""

from __future__ import annotations

import re
from datetime import date, datetime
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

    parsed = parse_date_value(value)
    if parsed is not None:
        return parsed.year
    # Only explicit, valid partial dates may supply a year. Never fill it from
    # today's date, nor rescue an invalid full date as a valid year.
    interval = _partial_date_interval(text)
    return interval[0].year if interval else None


def parse_date_value(value: object | None) -> date | None:
    """Parse a complete date value into ``date``; return None for partial dates."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None

    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    if re.fullmatch(r"\d{8}", text):
        text = f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    if not _looks_like_complete_date(text):
        return None

    try:
        return dt_parser.parse(_translate_months(text), fuzzy=False, dayfirst=_day_first(text)).date()
    except (TypeError, ValueError, OverflowError):
        return None


def date_in_range(value: object | None, start_date: object | None = None, end_date: object | None = None) -> bool:
    """Return True when a complete date is inside the inclusive range."""
    parsed = parse_date_value(value)
    if parsed is None:
        return False
    start = parse_date_value(start_date)
    end = parse_date_value(end_date)
    if start and parsed < start:
        return False
    if end and parsed > end:
        return False
    return True


def date_or_year_in_range(
    value: object | None,
    start_date: object | None = None,
    end_date: object | None = None,
) -> bool:
    """Return True when a complete date or extracted year is inside the inclusive range."""
    parsed = parse_date_value(value)
    if parsed is not None:
        return date_in_range(value, start_date=start_date, end_date=end_date)
    interval = _partial_date_interval(str(value).strip())
    if interval is None:
        return False
    start = parse_date_value(start_date)
    end = parse_date_value(end_date)
    # A partial date is eligible only when its entire possible interval lies
    # inside the requested window. A month/year is never invented as a day.
    if start and interval[0] < start:
        return False
    if end and interval[1] > end:
        return False
    return True


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

    parsed = parse_date_value(value)
    return parsed.isoformat() if parsed else text


_MONTH_TRANSLATIONS = {
    "januar": "January", "februar": "February", "märz": "March",
    "maerz": "March", "mai": "May", "juni": "June", "juli": "July",
    "oktober": "October", "dezember": "December",
}


def _translate_months(text: str) -> str:
    for original, translated in _MONTH_TRANSLATIONS.items():
        text = re.sub(rf"\b{original}\b", translated, text, flags=re.IGNORECASE)
    return text


def _day_first(text: str) -> bool:
    # Non-ISO numeric dates from the UK, Australia and European sources use
    # day/month/year; FDA dates are compact YYYYMMDD or ISO.
    return bool(re.match(r"^\d{1,2}[-/.]\d{1,2}[-/.]\d{4}$", text.strip()))


def _partial_date_interval(text: str) -> tuple[date, date] | None:
    import calendar
    if re.fullmatch(r"\d{4}", text):
        year = int(text)
        return (date(year, 1, 1), date(year, 12, 31)) if 1 <= year <= 9999 else None
    translated = _translate_months(text)
    match = re.fullmatch(r"(\d{4})[-/.](\d{1,2})", translated)
    try:
        if match:
            year, month = map(int, match.groups())
        elif re.fullmatch(r"[A-Za-z]+\s+\d{4}", translated):
            parsed = dt_parser.parse(translated, fuzzy=False, default=datetime(2000, 1, 1))
            year, month = parsed.year, parsed.month
        else:
            return None
        return date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])
    except (ValueError, OverflowError):
        return None


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
    for value in keywords:
        keyword = normalize_text(value)
        if not keyword:
            continue
        escaped = re.escape(keyword).replace(r"\ ", r"\s+")
        # Match Latin names as words (port must not match report), while CJK
        # queries retain their normal substring semantics.
        left = r"(?<![\w])" if re.match(r"[a-z0-9]", keyword) else ""
        right = r"(?![\w])" if re.search(r"[a-z0-9]$", keyword) else ""
        if re.search(left + escaped + right, normalized):
            return True
    return False
