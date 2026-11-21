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
    completeness: str = "unverified"
    retrieved_count: int | None = None
    total_available: int | None = None

    def __post_init__(self) -> None:
        """Keep legacy adapters compatible while exposing explicit coverage."""
        attrs = self.records.attrs
        self.completeness = attrs.get("completeness", self.completeness)
        if not self.success and self.completeness == "unverified":
            self.completeness = "partial" if not self.records.empty else "failed"
        if self.completeness == "failed":
            self.success = False
        if self.retrieved_count is None:
            self.retrieved_count = attrs.get("retrieved_count", len(self.records))
        if self.total_available is None:
            self.total_available = attrs.get("total_available")
        self.warnings = list(dict.fromkeys([*self.warnings, *attrs.get("warnings", [])]))
        if not self.success and not self.error_message:
            self.error_message = self.warnings[0] if self.warnings else "Source retrieval failed"
        if self.completeness not in {"complete", "partial", "failed", "unverified"}:
            raise ValueError("Invalid source completeness")


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
        """Return an explicit unavailable result for a planned source."""
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
            success=False,
            completeness="failed",
            warnings=[warning],
        )
