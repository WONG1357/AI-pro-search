import pandas as pd

from pipeline.pipeline import clean_records, filter_records
from pipeline.utils import format_fda_date, parse_year_from_date
from app import count_table


def test_parse_year_from_fda_yyyymmdd() -> None:
    assert parse_year_from_date("20250131") == 2025


def test_parse_year_from_text_date() -> None:
    assert parse_year_from_date("Jan 31, 2026") == 2026


def test_parse_year_from_blank_returns_none() -> None:
    assert parse_year_from_date("") is None


def test_format_fda_date() -> None:
    assert format_fda_date("20240105") == "2024-01-05"


def test_format_fda_date_preserves_partial_dates() -> None:
    assert format_fda_date("March 2024") == "March 2024"
    assert format_fda_date("2024") == "2024"


def test_filter_records_keeps_source_query_match_without_keyword_repeat() -> None:
    df = clean_records(
        pd.DataFrame(
            [
                {
                    "source": "TGA_DAEN",
                    "event_id": "99093",
                    "event_date": "2024-08-14",
                    "year": 2024,
                    "product_name": "Kii FIOS ADVFIX",
                    "brand_names": "",
                    "narrative_text": "Bowel perforated upon entry with the device.",
                    "source_query_match": True,
                }
            ]
        )
    )
    filtered = filter_records(df, ["trocar"], [2024])
    assert len(filtered) == 1
    assert filtered.iloc[0]["event_id"] == "99093"


def test_count_table_handles_nullable_integer_missing_values() -> None:
    df = pd.DataFrame({"year": pd.Series([2024, None], dtype="Int64")})
    counts = count_table(df, "year")
    assert set(counts["year"]) == {"2024", "Unknown"}
