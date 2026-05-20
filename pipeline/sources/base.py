"""Common source connector interfaces and helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from pipeline.config import UNIFIED_COLUMNS
from pipeline.progress import ProgressReporter


@dataclass
class SourceFetchResult:
    """Result returned by every source connector fetch operation."""

    source_name: str
    records: pd.DataFrame
    success: bool
    error_message: str | None = None
    warnings: list[str] = field(default_factory=list)


class BaseSourceConnector:
    """Base class for medical device accident source connectors."""

    source_name: str
    source_type: str

    def fetch(
        self,
        keywords: list[str],
        years: list[int],
        components: list[str] | None = None,
        accident_terms: list[str] | None = None,
        progress_reporter: ProgressReporter | None = None,
        **kwargs: Any,
    ) -> SourceFetchResult:
        """Fetch source records normalized to the unified column schema."""
        raise NotImplementedError


def ensure_unified_columns(df: pd.DataFrame | None) -> pd.DataFrame:
    """Return a dataframe with all unified columns present and ordered first."""
    if df is None:
        normalized = pd.DataFrame()
    else:
        normalized = df.copy()

    for column in UNIFIED_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = None

    extra_columns = [column for column in normalized.columns if column not in UNIFIED_COLUMNS]
    return normalized[UNIFIED_COLUMNS + extra_columns]


class NotImplementedSourceConnector(BaseSourceConnector):
    """Placeholder connector for planned sources."""

    source_name = "UNIMPLEMENTED"
    source_type = "placeholder"
    display_name = "Unimplemented source"

    def fetch(
        self,
        keywords: list[str],
        years: list[int],
        components: list[str] | None = None,
        accident_terms: list[str] | None = None,
        progress_reporter: ProgressReporter | None = None,
        **kwargs: Any,
    ) -> SourceFetchResult:
        """Return an empty successful result with a warning for planned sources."""
        warning = f"{self.display_name} connector is not implemented yet."
        if progress_reporter:
            progress_reporter.update_source(
                self.source_name,
                current_step=1,
                total_steps=1,
                records_fetched=0,
                stage="Skipped",
                message=warning,
            )
        return SourceFetchResult(
            source_name=self.source_name,
            records=ensure_unified_columns(pd.DataFrame()),
            success=True,
            warnings=[warning],
        )
