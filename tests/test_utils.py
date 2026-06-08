import pandas as pd

from pipeline.pipeline import clean_records, filter_records, summarize_by_year_category
from pipeline.utils import format_fda_date, parse_year_from_date
from app import count_fda_category, count_table, display_year_column, filter_values, normalize_search_date_range, source_result_order


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


def test_filter_records_uses_fda_maude_report_date_range() -> None:
    df = clean_records(
        pd.DataFrame(
            [
                {
                    "source": "FDA_MAUDE",
                    "event_id": "18562812",
                    "date_of_event": "2023-12-20",
                    "date_received": "2024-01-05",
                    "event_date": "2023-12-20",
                    "product_name": "Trocar",
                    "brand_names": "",
                    "narrative_text": "Trocar issue reported.",
                },
                {
                    "source": "TGA_DAEN",
                    "event_id": "TGA-1",
                    "date_received": "2024-01-05",
                    "event_date": "2023-12-20",
                    "product_name": "Trocar",
                    "brand_names": "",
                    "narrative_text": "Trocar issue reported.",
                },
            ]
        )
    )

    filtered = filter_records(df, ["trocar"], [2024], start_date="2024-01-01", end_date="2024-12-31")

    assert list(filtered["event_id"]) == ["18562812"]
    assert filtered.iloc[0]["report_date"] == "2024-01-05"
    assert filtered.iloc[0]["search_date"] == "2024-01-05"
    assert filtered.iloc[0]["search_year"] == 2024


def test_clean_records_builds_classification_text_from_problem_fields() -> None:
    df = clean_records(
        pd.DataFrame(
            [
                {
                    "source": "FDA_MAUDE",
                    "event_id": "1",
                    "event_date": "2024-01-01",
                    "product_name": "Trocar",
                    "device_problem_text": "Seal broke",
                    "patient_problem_text": "No patient injury",
                    "narrative_text": "Procedure was completed.",
                }
            ]
        )
    )

    assert "seal broke" in df.iloc[0]["classification_text"]


def test_display_year_column_uses_search_year_for_fda_report_date_counts() -> None:
    df = clean_records(
        pd.DataFrame(
            [
                {
                    "source": "FDA_MAUDE",
                    "event_id": "1",
                    "date_of_event": "2021-08-01",
                    "date_received": "2024-01-02",
                    "event_date": "2021-08-01",
                    "product_name": "Trocar",
                    "narrative_text": "Trocar issue.",
                },
                {
                    "source": "FDA_MAUDE",
                    "event_id": "2",
                    "date_of_event": "2009-01-01",
                    "date_received": "2024-01-03",
                    "event_date": "2009-01-01",
                    "product_name": "Trocar",
                    "narrative_text": "Trocar issue.",
                },
            ]
        )
    )

    counts = count_table(df, display_year_column(df))

    assert list(counts["search_year"]) == ["2024"]
    assert list(counts["record_count"]) == [2]


def test_summary_year_category_uses_search_year() -> None:
    df = pd.DataFrame(
        [
            {"event_id": "1", "year": 2021, "search_year": 2024, "category": "Other"},
            {"event_id": "2", "year": 2009, "search_year": 2024, "category": "Other"},
        ]
    )

    summary = summarize_by_year_category(df)

    assert list(summary["year"]) == [2024]
    assert list(summary["incident_count"]) == [2]


def test_count_fda_category_counts_match_bucket() -> None:
    df = pd.DataFrame(
        {
            "fda_match_category": [
                "Brand name matches",
                "Narrative matches",
                "Brand name matches",
            ]
        }
    )

    assert count_fda_category(df, "Brand name matches") == 2


def test_count_table_handles_nullable_integer_missing_values() -> None:
    df = pd.DataFrame({"year": pd.Series([2024, None], dtype="Int64")})
    counts = count_table(df, "year")
    assert set(counts["year"]) == {"2024", "Unknown"}


def test_normalize_search_date_range_handles_single_streamlit_value() -> None:
    start, end = normalize_search_date_range((pd.Timestamp("2024-01-05").date(),))

    assert start == pd.Timestamp("2024-01-05").date()
    assert end == pd.Timestamp("2024-01-05").date()


def test_filter_values_does_not_cap_large_value_sets() -> None:
    df = pd.DataFrame({"manufacturer": [f"M{i:03d}" for i in range(250)]})

    values = filter_values(df, "manufacturer")

    assert len(values) == 250
    assert values[0] == "M000"
    assert values[-1] == "M249"


def test_source_result_order_includes_unexpected_result_sources() -> None:
    order = source_result_order(pd.DataFrame({"source": ["CUSTOM_SOURCE"]}))

    assert "FDA_MAUDE" in order
    assert order[-1] == "CUSTOM_SOURCE"
