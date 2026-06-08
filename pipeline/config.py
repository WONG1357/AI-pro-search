"""Configuration values for the trocar incident pipeline."""

from __future__ import annotations

import os
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
EXPORTS_DIR = DATA_DIR / "exports"

OPENFDA_API_KEY = os.getenv("OPENFDA_API_KEY", "").strip()
OPENFDA_DEVICE_EVENT_ENDPOINT = "https://api.fda.gov/device/event.json"

TGA_DAEN_BASE_URL = os.getenv("TGA_DAEN_BASE_URL", "https://apps.tga.gov.au").strip()
TGA_DAEN_SEARCH_ENDPOINT = os.getenv(
    "TGA_DAEN_SEARCH_ENDPOINT",
    "https://apps.tga.gov.au/prod/DEVICES/daen-entry.aspx",
).strip()
TGA_DAEN_DEVICE_SEARCH_ENDPOINT = os.getenv(
    "TGA_DAEN_DEVICE_SEARCH_ENDPOINT",
    "https://apps.tga.gov.au/PROD/DEVICES/daen-entry.aspx/GetSearchItems",
).strip()
TGA_DAEN_REPORT_SEARCH_ENDPOINT = os.getenv(
    "TGA_DAEN_REPORT_SEARCH_ENDPOINT",
    "https://apps.tga.gov.au/PROD/DEVICES/daen-report.aspx",
).strip()
TGA_DAEN_TIMEOUT = float(os.getenv("TGA_DAEN_TIMEOUT", "60"))
TGA_DAEN_PAGE_SIZE = int(os.getenv("TGA_DAEN_PAGE_SIZE", "50"))
TGA_DAEN_DEFAULT_HEADERS = json.loads(os.getenv("TGA_DAEN_DEFAULT_HEADERS", "{}") or "{}")
TGA_DAEN_USER_AGENT = os.getenv(
    "TGA_DAEN_USER_AGENT",
    "Mozilla/5.0 (compatible; Codex TGA DAEN Connector)",
).strip()
TGA_DAEN_DEBUG = os.getenv("TGA_DAEN_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}

SWISSMEDIC_FSCA_BASE_URL = os.getenv("SWISSMEDIC_FSCA_BASE_URL", "https://fsca.swissmedic.ch/mep").strip().rstrip("/")
SWISSMEDIC_FSCA_SEARCH_ENDPOINT = os.getenv("SWISSMEDIC_FSCA_SEARCH_ENDPOINT", "/api/publications/search").strip()
SWISSMEDIC_FSCA_TIMEOUT = float(os.getenv("SWISSMEDIC_FSCA_TIMEOUT", "60"))
SWISSMEDIC_FSCA_PAGE_SIZE = int(os.getenv("SWISSMEDIC_FSCA_PAGE_SIZE", "100"))
SWISSMEDIC_FSCA_USER_AGENT = os.getenv(
    "SWISSMEDIC_FSCA_USER_AGENT",
    "Mozilla/5.0 (compatible; Codex Swissmedic FSCA Connector)",
).strip()

MHRA_FSCA_BASE_URL = os.getenv("MHRA_FSCA_BASE_URL", "https://www.gov.uk").strip().rstrip("/")
MHRA_FSCA_SEARCH_ENDPOINT = os.getenv("MHRA_FSCA_SEARCH_ENDPOINT", "/drug-device-alerts").strip()
MHRA_FSCA_TIMEOUT = float(os.getenv("MHRA_FSCA_TIMEOUT", "60"))
MHRA_FSCA_USER_AGENT = os.getenv(
    "MHRA_FSCA_USER_AGENT",
    "Mozilla/5.0 (compatible; Codex MHRA FSN Connector)",
).strip()

DEFAULT_KEYWORDS = [
    "Xcel",
    "Versaport",
    "VersaOne",
    "Kii",
    "Apple Trocar",
    "Lina Port",
    "Trocar",
    "leak",
    "fixation",
    "puncture",
    "death",
    "injury",
    "infection",
    "blade",
    "pyramidal tip",
]
DEFAULT_TARGET_YEARS = [2024, 2025, 2026]

UNIFIED_COLUMNS = [
    "source",
    "source_type",
    "event_id",
    "report_number",
    "date_of_event",
    "date_received",
    "report_date",
    "event_date",
    "year",
    "received_year",
    "search_date",
    "search_year",
    "country",
    "manufacturer",
    "product_name",
    "brand_names",
    "generic_name",
    "device_model",
    "product_code",
    "event_type",
    "fda_match_field",
    "fda_match_category",
    "patient_outcome",
    "device_problem_text",
    "patient_problem_text",
    "narrative_text",
    "raw_link",
    "event_link",
    "record_hash",
    "source_specific",
    "raw_record",
    "device_link",
    "source_category",
    "source_category_name",
    "source_category_code",
    "source_category_system",
    "source_risk_class",
    "eudamed_emdn_code",
    "eudamed_emdn_term",
    "eudamed_risk_class",
    "platform_category",
    "category_normalized",
    "category_confidence",
    "category_reason",
]
