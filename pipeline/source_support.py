"""Shared bounded HTTP and coverage contracts for source connectors.

Coverage describes the declared retrieval query, never the incidence of harm.
Warnings and counts travel with DataFrame attrs for legacy adapter compatibility.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import pandas as pd
import requests


class SourceRequestError(RuntimeError):
    """An upstream transport failure; messages intentionally exclude query secrets."""


def request_with_retry(session: Any, method: str, url: str, *, attempts: int = 3,
                       allowed_statuses: tuple[int, ...] = (), **kwargs: Any) -> requests.Response:
    """Retry transient read requests within a finite budget, respecting Retry-After.

    These connectors only POST search/filter forms. They do not mutate source data.
    Never copy request URLs, authorization headers or response bodies into errors.
    """
    for attempt in range(attempts):
        try:
            response = getattr(session, method.lower())(url, **kwargs)
        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt + 1 == attempts:
                raise SourceRequestError(f"{type(exc).__name__} after {attempts} attempts") from None
            time.sleep(min(0.5 * 2**attempt, 2.0))
            continue
        if response.status_code in allowed_statuses:
            return response
        if response.status_code in {429, 500, 502, 503, 504} and attempt + 1 < attempts:
            retry_after = response.headers.get("Retry-After", "")
            try:
                delay = float(retry_after)
            except ValueError:
                try:
                    target = parsedate_to_datetime(retry_after)
                    if target.tzinfo is None:
                        target = target.replace(tzinfo=timezone.utc)
                    delay = (target - datetime.now(timezone.utc)).total_seconds()
                except (TypeError, ValueError, OverflowError):
                    delay = 0.5 * 2**attempt
            # Do not ignore a long cooldown and immediately retry the source.
            if delay > 5:
                raise SourceRequestError(f"HTTP {response.status_code}; Retry-After exceeds retry budget")
            time.sleep(max(0, delay))
            continue
        if response.status_code >= 400:
            raise SourceRequestError(f"HTTP {response.status_code} from source")
        return response
    raise SourceRequestError("Source retry budget exhausted")


def reject_challenge_html(html: str, source: str) -> None:
    text = html.lower()
    challenge_markers = ("cf-chl-", "verify you are human", "checking your browser", "captcha", "access denied")
    if any(marker in text for marker in challenge_markers):
        raise SourceRequestError(f"{source} returned an access challenge; source coverage is unavailable")


def with_coverage(df: pd.DataFrame, completeness: str, warnings: list[str], *,
                  retrieved_count: int | None = None, total_available: int | None = None) -> pd.DataFrame:
    df.attrs.update(completeness=completeness, warnings=list(warnings),
                    retrieved_count=len(df) if retrieved_count is None else retrieved_count,
                    total_available=total_available)
    return df
