"""Source connector registry."""

from __future__ import annotations

from pipeline.sources.base import BaseSourceConnector, SourceFetchResult, ensure_unified_columns
from pipeline.sources.bfarm import BfarmRecallConnector
from pipeline.sources.health_canada_mdi import HealthCanadaMdiConnector
from pipeline.sources.mhra_fsca import MhraFscaConnector
from pipeline.sources.openfda_maude import OpenFdaMaudeConnector
from pipeline.sources.swissmedic import SwissmedicFscaConnector
from pipeline.sources.tga_daen import TgaDaenConnector


SOURCE_REGISTRY: dict[str, type[BaseSourceConnector]] = {
    "FDA_MAUDE": OpenFdaMaudeConnector,
    "TGA_DAEN": TgaDaenConnector,
    "HEALTH_CANADA_MDI": HealthCanadaMdiConnector,
    "SWISSMEDIC_FSCA": SwissmedicFscaConnector,
    "MHRA_FSCA": MhraFscaConnector,
    "BFARM_RECALLS": BfarmRecallConnector,
}

SOURCE_DISPLAY_NAMES: dict[str, str] = {
    source_id: getattr(connector, "display_name", source_id)
    for source_id, connector in SOURCE_REGISTRY.items()
}

DEFAULT_SELECTED_SOURCES = ["FDA_MAUDE"]

__all__ = [
    "BaseSourceConnector",
    "SourceFetchResult",
    "ensure_unified_columns",
    "SOURCE_REGISTRY",
    "SOURCE_DISPLAY_NAMES",
    "DEFAULT_SELECTED_SOURCES",
]
