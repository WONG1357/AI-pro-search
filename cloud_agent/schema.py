"""Strict, deterministic search requests; never accept executable source options."""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo


SOURCE_IDS = (
    "FDA_MAUDE", "TGA_DAEN", "HEALTH_CANADA_MDI", "SWISSMEDIC_FSCA",
    "MHRA_FSCA", "BFARM_RECALLS",
)
SOURCE_INFO = {
    "FDA_MAUDE": ("FDA MAUDE", "https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfmaude/search.cfm", "adverse_event_report"),
    "TGA_DAEN": ("TGA DAEN", "https://apps.tga.gov.au/prod/DEVICES/daen-entry.aspx", "adverse_event_report"),
    "HEALTH_CANADA_MDI": ("Health Canada MDI", "https://hpr-rps.hres.ca/mdi_results.php", "adverse_event_report"),
    "SWISSMEDIC_FSCA": ("Swissmedic FSCA", "https://fsca.swissmedic.ch/mep/", "safety_notice"),
    "MHRA_FSCA": ("MHRA field safety notices", "https://www.gov.uk/drug-device-alerts", "safety_notice"),
    "BFARM_RECALLS": ("BfArM manufacturer actions", "https://www.bfarm.de/EN/Medical-devices/Tasks/Field-corrective-actions/_node.html", "safety_notice"),
}
SOURCE_DOMAINS = {
    "FDA_MAUDE": {"api.fda.gov", "www.accessdata.fda.gov", "www.fda.gov"},
    "TGA_DAEN": {"apps.tga.gov.au", "www.tga.gov.au", "tga.gov.au"},
    "HEALTH_CANADA_MDI": {"hpr-rps.hres.ca", "recalls-rappels.canada.ca", "www.canada.ca"},
    "SWISSMEDIC_FSCA": {"fsca.swissmedic.ch", "www.swissmedic.ch"},
    "MHRA_FSCA": {"www.gov.uk", "assets.publishing.service.gov.uk", "www.mhra.gov.uk"},
    "BFARM_RECALLS": {"www.bfarm.de", "bfarm.de"},
}


def current_date() -> date:
    return datetime.now(ZoneInfo("Asia/Hong_Kong")).date()


def _terms(value: Any, field: str, *, required: bool = False) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or any(not isinstance(v, str) for v in value):
        raise ValueError(f"{field} must be a list of strings")
    cleaned = tuple(dict.fromkeys(v.strip() for v in value if v.strip()))
    if required and not cleaned:
        raise ValueError(f"{field} must contain at least one device, brand, model, or product-code term")
    if len(cleaned) > 20 or any(len(v) > 120 for v in cleaned):
        raise ValueError(f"{field} supports up to 20 terms of 120 characters each")
    if any(any(ord(c) < 32 for c in v) for v in cleaned):
        raise ValueError(f"{field} cannot contain control characters")
    return cleaned


@dataclass(frozen=True)
class SearchRequest:
    keywords: tuple[str, ...]
    start_date: str
    end_date: str
    sources: tuple[str, ...] = SOURCE_IDS
    question: str = "Medical-device incident surveillance"
    components: tuple[str, ...] = ()
    accident_terms: tuple[str, ...] = ()
    classifier_profile: str = "Custom"
    max_pages: int = 10
    max_records: int = 1000
    request_timeout: int = 20
    source_timeout: int = 120

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SearchRequest":
        if not isinstance(data, dict):
            raise ValueError("Search request must be a JSON object")
        unknown = set(data) - {f.name for f in fields(cls)}
        if unknown:
            raise ValueError(f"Unsupported request fields: {', '.join(sorted(unknown))}")
        values = dict(data)
        values["keywords"] = _terms(values.get("keywords", []), "keywords", required=True)
        generic = {"death", "injury", "infection", "leak", "failure", "accident", "malfunction"}
        if any(t.casefold() in generic for t in values["keywords"]):
            raise ValueError("Use device identity in keywords; put generic outcomes in accident_terms")
        for key in ("components", "accident_terms"):
            values[key] = _terms(values.get(key, []), key)
        today = current_date()
        values.setdefault("end_date", today.isoformat())
        values.setdefault("start_date", (today - timedelta(days=365)).isoformat())
        try:
            start = date.fromisoformat(values["start_date"])
            end = date.fromisoformat(values["end_date"])
        except (ValueError, TypeError):
            raise ValueError("start_date and end_date must be valid ISO dates (YYYY-MM-DD)") from None
        if start > end or start.year < 1970 or end > today:
            raise ValueError("Date range must start in 1970 or later, be ordered, and end no later than today")
        values["sources"] = _terms(values.get("sources", list(SOURCE_IDS)), "sources", required=True)
        if set(values["sources"]) - set(SOURCE_IDS):
            raise ValueError("Unknown source identifier")
        values.setdefault("classifier_profile", "Trocar" if any("trocar" in t.lower() for t in values["keywords"]) else "Custom")
        if values["classifier_profile"] not in ("Trocar", "Custom"):
            raise ValueError("classifier_profile must be Trocar or Custom")
        if not isinstance(values.get("question", ""), str) or len(values.get("question", "")) > 12000:
            raise ValueError("question must be text of at most 12,000 characters")
        limits = {"max_pages": (1, 100), "max_records": (1, 20000), "request_timeout": (2, 60), "source_timeout": (5, 600)}
        for name, (lo, hi) in limits.items():
            value = values.get(name, cls.__dataclass_fields__[name].default)
            if type(value) is not int or not lo <= value <= hi:
                raise ValueError(f"{name} must be an integer from {lo} to {hi}")
        request = cls(**values)
        if request.source_timeout < request.request_timeout:
            raise ValueError("source_timeout must be at least request_timeout")
        return request

    @property
    def years(self) -> list[int]:
        return list(range(date.fromisoformat(self.start_date).year, date.fromisoformat(self.end_date).year + 1))

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
