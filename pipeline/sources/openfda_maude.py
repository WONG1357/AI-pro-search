"""FDA MAUDE source connector backed by the openFDA device/event API."""

from __future__ import annotations

from typing import Any

import pandas as pd

from pipeline.progress import ProgressReporter
from pipeline.sources.base import BaseSourceConnector, SourceFetchResult, ensure_unified_columns
from pipeline.sources_openfda import fetch_fda_maude_openfda


class OpenFdaMaudeConnector(BaseSourceConnector):
    """Fetch FDA MAUDE adverse event records from openFDA."""

    source_name = "FDA_MAUDE"
    source_type = "adverse_event"
    display_name = "FDA MAUDE"

    def fetch(
        self,
        keywords: list[str],
        years: list[int],
        components: list[str] | None = None,
        accident_terms: list[str] | None = None,
        progress_reporter: ProgressReporter | None = None,
        **kwargs: Any,
    ) -> SourceFetchResult:
        """Fetch and normalize FDA MAUDE records."""
        try:
            search_terms = _merge_terms(keywords, components, accident_terms)
            total_steps = max(1, len(search_terms) * len(years) * 3)
            if progress_reporter:
                progress_reporter.update_source(
                    self.source_name,
                    current_step=0,
                    total_steps=total_steps,
                    stage="Fetching openFDA MAUDE",
                    message=f"Searching {len(search_terms)} terms across {len(years)} years",
                )
            records = fetch_fda_maude_openfda(
                search_terms,
                years,
                limit_per_query=kwargs.get("limit_per_query", 100),
                max_skip_per_query=kwargs.get("max_skip_per_query", 1000),
                request_timeout=int(kwargs.get("request_timeout") or 60),
            )
            if progress_reporter:
                progress_reporter.update_source(
                    self.source_name,
                    current_step=total_steps,
                    total_steps=total_steps,
                    records_fetched=len(records),
                    stage="Fetched records",
                    message=f"Fetched {len(records)} raw FDA MAUDE records",
                )
            return SourceFetchResult(
                source_name=self.source_name,
                records=ensure_unified_columns(records),
                success=True,
            )
        except Exception as exc:
            return SourceFetchResult(
                source_name=self.source_name,
                records=ensure_unified_columns(pd.DataFrame()),
                success=False,
                error_message=str(exc),
            )


def _merge_terms(
    keywords: list[str],
    components: list[str] | None,
    accident_terms: list[str] | None,
) -> list[str]:
    """Merge search term groups while preserving order and uniqueness."""
    terms: list[str] = []
    seen: set[str] = set()
    for value in [*keywords, *(components or []), *(accident_terms or [])]:
        term = str(value).strip()
        key = term.lower()
        if term and key not in seen:
            terms.append(term)
            seen.add(key)
    return terms
