"""Generic progress reporting for source connectors and pipeline runs."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Callable


TERMINAL_STATUSES = {"completed", "failed", "skipped"}


@dataclass
class SourceProgress:
    """Progress state for a single data source."""

    source_name: str
    status: str = "pending"
    stage: str = ""
    current_step: int = 0
    total_steps: int | None = None
    percent_complete: float | None = None
    records_fetched: int = 0
    elapsed_seconds: float = 0.0
    eta_seconds: float | None = None
    message: str = ""
    started_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass
class PipelineProgress:
    """Progress state for the whole pipeline."""

    overall_status: str = "pending"
    current_source: str | None = None
    overall_percent_complete: float | None = None
    elapsed_seconds: float = 0.0
    eta_seconds: float | None = None
    sources: dict[str, SourceProgress] = field(default_factory=dict)
    logs: list[str] = field(default_factory=list)


class ProgressReporter:
    """Track source and overall pipeline progress."""

    def __init__(self, source_names: list[str] | None = None, on_update: Callable[[PipelineProgress], None] | None = None) -> None:
        self.started_at = _now()
        self.sources: dict[str, SourceProgress] = {
            source_name: SourceProgress(source_name=source_name)
            for source_name in (source_names or [])
        }
        self.logs: list[str] = []
        self.current_source: str | None = None
        self.on_update = on_update

    def start_source(self, source_name: str, total_steps: int | None = None, message: str = "") -> None:
        """Mark a source as running."""
        now = _now()
        source = self._source(source_name)
        source.status = "running"
        source.stage = "Starting"
        source.current_step = 0
        source.total_steps = total_steps
        source.records_fetched = 0
        source.started_at = now
        source.updated_at = now
        source.message = message
        self.current_source = source_name
        self._recalculate(source)
        self.log(f"Starting {source_name}" + (f": {message}" if message else ""))
        self._notify()

    def update_source(
        self,
        source_name: str,
        current_step: int | None = None,
        total_steps: int | None = None,
        records_fetched: int | None = None,
        stage: str | None = None,
        message: str = "",
    ) -> None:
        """Update progress fields for a source."""
        source = self._source(source_name)
        if source.status == "pending":
            self.start_source(source_name, total_steps=total_steps, message=message)
        if current_step is not None:
            source.current_step = current_step
        if total_steps is not None:
            source.total_steps = total_steps
        if records_fetched is not None:
            source.records_fetched = records_fetched
        if stage is not None:
            source.stage = stage
        if message:
            source.message = message
            self.log(f"{source_name} {message}")
        source.updated_at = _now()
        self.current_source = source_name
        self._recalculate(source)
        self._notify()

    def complete_source(
        self,
        source_name: str,
        records_fetched: int | None = None,
        message: str = "",
    ) -> None:
        """Mark a source as completed."""
        source = self._source(source_name)
        source.status = "completed"
        source.stage = "Completed"
        if records_fetched is not None:
            source.records_fetched = records_fetched
        if source.total_steps is not None:
            source.current_step = source.total_steps
        source.percent_complete = 100.0
        source.eta_seconds = 0.0
        source.updated_at = _now()
        source.message = message or f"Completed with {source.records_fetched} records"
        self.log(f"{source_name} {source.message}")
        self._notify()

    def fail_source(self, source_name: str, error_message: str) -> None:
        """Mark a source as failed."""
        source = self._source(source_name)
        source.status = "failed"
        source.stage = "Failed"
        source.percent_complete = 100.0
        source.eta_seconds = 0.0
        source.updated_at = _now()
        source.message = error_message
        self.log(f"{source_name} failed: {error_message}")
        self._notify()

    def skip_source(self, source_name: str, reason: str) -> None:
        """Mark a source as skipped."""
        source = self._source(source_name)
        source.status = "skipped"
        source.stage = "Skipped"
        source.percent_complete = 100.0
        source.eta_seconds = 0.0
        source.updated_at = _now()
        source.message = reason
        self.log(f"{source_name} skipped: {reason}")
        self._notify()

    def get_snapshot(self) -> PipelineProgress:
        """Return an immutable-style snapshot of current progress."""
        elapsed = (_now() - self.started_at).total_seconds()
        overall_percent = self._overall_percent()
        eta = None
        if overall_percent and overall_percent > 0:
            eta = elapsed * (100.0 - overall_percent) / overall_percent
        status = self._overall_status()
        return PipelineProgress(
            overall_status=status,
            current_source=self.current_source,
            overall_percent_complete=overall_percent,
            elapsed_seconds=elapsed,
            eta_seconds=eta,
            sources={name: SourceProgress(**asdict(progress)) for name, progress in self.sources.items()},
            logs=list(self.logs[-200:]),
        )

    def to_dict(self) -> dict:
        """Return a JSON-friendly snapshot dictionary."""
        return asdict(self.get_snapshot())

    def log(self, message: str) -> None:
        """Append a timestamped log message."""
        elapsed = (_now() - self.started_at).total_seconds()
        self.logs.append(f"[{_format_duration(elapsed)}] {message}")
        self._notify()

    def _source(self, source_name: str) -> SourceProgress:
        if source_name not in self.sources:
            self.sources[source_name] = SourceProgress(source_name=source_name)
        return self.sources[source_name]

    def _recalculate(self, source: SourceProgress) -> None:
        if source.started_at is None:
            source.started_at = _now()
        now = _now()
        source.updated_at = now
        source.elapsed_seconds = (now - source.started_at).total_seconds()
        if source.total_steps and source.total_steps > 0:
            percent = max(0.0, min(100.0, source.current_step / source.total_steps * 100.0))
            source.percent_complete = percent
            if percent > 0 and percent < 100:
                source.eta_seconds = source.elapsed_seconds * (100.0 - percent) / percent
            elif percent >= 100:
                source.eta_seconds = 0.0
        else:
            source.percent_complete = None
            source.eta_seconds = None

    def _overall_percent(self) -> float | None:
        if not self.sources:
            return None
        values = []
        for source in self.sources.values():
            if source.status in TERMINAL_STATUSES:
                values.append(100.0)
            elif source.percent_complete is not None:
                values.append(source.percent_complete)
            else:
                values.append(0.0)
        return sum(values) / len(values)

    def _overall_status(self) -> str:
        statuses = {source.status for source in self.sources.values()}
        if not statuses:
            return "pending"
        if any(status == "running" for status in statuses):
            return "running"
        if all(status in TERMINAL_STATUSES for status in statuses):
            return "completed"
        return "pending"

    def _notify(self) -> None:
        if self.on_update:
            self.on_update(self.get_snapshot())


def format_seconds(value: float | None) -> str:
    """Format seconds for display."""
    if value is None:
        return "ETA unavailable"
    return _format_duration(value)


def _format_duration(seconds: float) -> str:
    total = int(max(0, seconds))
    minutes, sec = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{sec:02d}"
    return f"{minutes:02d}:{sec:02d}"


def _now() -> datetime:
    return datetime.now(timezone.utc)
