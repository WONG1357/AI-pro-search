"""Command-line entrypoint and orchestration for the trocar incident pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from pipeline.classifier import classify_incident
from pipeline.config import DEFAULT_KEYWORDS, DEFAULT_TARGET_YEARS, EXPORTS_DIR
from pipeline.export import export_pipeline_results
from pipeline.progress import ProgressReporter
from pipeline.sources import DEFAULT_SELECTED_SOURCES, SOURCE_REGISTRY, ensure_unified_columns
from pipeline.utils import contains_any_keyword, format_fda_date, parse_year_from_date


def run_pipeline(
    keywords: Iterable[str] = DEFAULT_KEYWORDS,
    years: Iterable[int] = DEFAULT_TARGET_YEARS,
    output_dir: str | Path = EXPORTS_DIR,
    export_results: bool = True,
    export_file_prefix: str = "",
    selected_sources: list[str] | None = None,
    components: list[str] | None = None,
    accident_terms: list[str] | None = None,
    source_options: dict[str, dict[str, Any]] | None = None,
    mhra_csv_path: str | Path | None = None,
    **kwargs: Any,
) -> dict[str, pd.DataFrame]:
    """Run ingestion, cleaning, filtering, deduplication, classification, and export.

    The legacy ``mhra_csv_path`` argument is accepted for backward compatibility
    but ignored. The pipeline now uses FDA MAUDE only.
    """
    keywords = list(keywords)
    years = list(years)
    selected_sources = list(selected_sources or DEFAULT_SELECTED_SOURCES)
    source_options = source_options or {}
    global_options = source_options.pop("_global", {})
    progress_reporter = kwargs.pop("progress_reporter", None) or ProgressReporter(selected_sources)
    request_timeout = global_options.get("request_timeout")
    max_pages = global_options.get("max_pages")
    max_records = global_options.get("max_records")
    debug = bool(global_options.get("debug", False))

    if mhra_csv_path is not None:
        print("[WARN] mhra_csv_path is deprecated and ignored; FDA MAUDE only is supported.")
    if kwargs:
        print(f"[WARN] Ignoring unsupported run_pipeline kwargs: {sorted(kwargs.keys())}")

    source_frames: list[pd.DataFrame] = []
    source_errors: list[dict[str, str]] = []
    source_warnings: list[dict[str, str]] = []

    for source_id in selected_sources:
        connector_cls = SOURCE_REGISTRY.get(source_id)
        if connector_cls is None:
            source_errors.append({"source_name": source_id, "error_message": "Unknown source id"})
            progress_reporter.skip_source(source_id, "Unknown source id")
            continue

        connector = connector_cls()
        progress_reporter.start_source(source_id, message="Source queued")
        result = connector.fetch(
            keywords=keywords,
            years=years,
            components=components,
            accident_terms=accident_terms,
            progress_reporter=progress_reporter,
            request_timeout=request_timeout,
            max_pages=max_pages,
            max_records=max_records,
            debug=debug,
            **source_options.get(source_id, {}),
        )

        if result.warnings:
            source_warnings.extend(
                {"source_name": result.source_name, "warning_message": warning}
                for warning in result.warnings
            )
        if not result.success:
            progress_reporter.fail_source(result.source_name, result.error_message or "Unknown source error")
            source_errors.append(
                {
                    "source_name": result.source_name,
                    "error_message": result.error_message or "Unknown source error",
                }
            )
            continue

        progress_reporter.complete_source(result.source_name, records_fetched=len(result.records))
        source_frames.append(result.records)

    df_all = pd.concat(source_frames, ignore_index=True) if source_frames else ensure_unified_columns(pd.DataFrame())

    empty_results = {
        "all_raw": df_all,
        "filtered": pd.DataFrame(),
        "summary_year_category": pd.DataFrame(),
        "summary_source_category": pd.DataFrame(),
        "source_errors": pd.DataFrame(source_errors),
        "source_warnings": pd.DataFrame(source_warnings),
        "progress": progress_reporter.get_snapshot(),
    }
    if df_all.empty:
        print("[WARN] No records retrieved.")
        if export_results:
            export_pipeline_results(empty_results, output_dir, file_prefix=export_file_prefix)
        return empty_results

    df_all = clean_records(df_all)
    df_filtered = filter_records(df_all, keywords, years)
    df_filtered = deduplicate_records(df_filtered)

    print(f"[INFO] Classifying {len(df_filtered):,} filtered records...")
    df_filtered["category"] = df_filtered["combined_text"].apply(classify_incident)
    if "platform_category" in df_filtered.columns:
        df_filtered["platform_category"] = df_filtered["category"]
    if "category_normalized" in df_filtered.columns:
        df_filtered["category_normalized"] = df_filtered["category"]

    summary_year_category = summarize_by_year_category(df_filtered)
    summary_source_category = summarize_by_source_category(df_filtered)

    results = {
        "all_raw": df_all,
        "filtered": df_filtered,
        "summary_year_category": summary_year_category,
        "summary_source_category": summary_source_category,
        "source_errors": pd.DataFrame(source_errors),
        "source_warnings": pd.DataFrame(source_warnings),
        "progress": progress_reporter.get_snapshot(),
    }

    if export_results:
        files = export_pipeline_results(results, output_dir, file_prefix=export_file_prefix)
        print("[OK] Pipeline completed.")
        print(f"Raw records:      {len(df_all):,}")
        print(f"Filtered records: {len(df_filtered):,}")
        print(f"Saved to folder:  {Path(output_dir).resolve()}")
        print(f"Excel report:     {files['excel'].resolve()}")

    return results


def clean_records(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize text and date columns and prepare searchable combined text."""
    cleaned = df.copy()

    for column in [
        "source",
        "source_type",
        "event_id",
        "report_number",
        "event_date",
        "product_name",
        "manufacturer",
        "narrative_text",
        "brand_names",
        "generic_name",
        "device_model",
        "product_code",
        "raw_link",
        "event_link",
        "device_link",
        "country",
        "event_type",
        "patient_outcome",
        "device_problem_text",
        "patient_problem_text",
        "record_hash",
        "date_of_event",
        "date_received",
        "source_specific",
        "raw_record",
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
    ]:
        if column not in cleaned.columns:
            cleaned[column] = ""

    for column in [
        "product_name",
        "manufacturer",
        "narrative_text",
        "brand_names",
        "generic_name",
        "device_model",
        "product_code",
        "country",
        "event_type",
        "patient_outcome",
        "device_problem_text",
        "patient_problem_text",
        "record_hash",
        "source_type",
        "report_number",
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
    ]:
        cleaned[column] = cleaned[column].fillna("").astype(str)

    for date_column in ["date_of_event", "date_received", "event_date"]:
        if date_column not in cleaned.columns:
            cleaned[date_column] = ""
        cleaned[date_column] = cleaned[date_column].fillna("").astype(str).apply(format_fda_date)

    missing_event_date = cleaned["event_date"].fillna("").astype(str).str.strip() == ""
    cleaned.loc[missing_event_date, "event_date"] = cleaned.loc[missing_event_date, "date_of_event"]

    missing_event_date = cleaned["event_date"].fillna("").astype(str).str.strip() == ""
    cleaned.loc[missing_event_date, "event_date"] = cleaned.loc[missing_event_date, "date_received"]

    cleaned["year"] = pd.to_numeric(cleaned["event_date"].apply(parse_year_from_date), errors="coerce").astype("Int64")
    cleaned["received_year"] = pd.to_numeric(
        cleaned["date_received"].apply(parse_year_from_date),
        errors="coerce",
    ).astype("Int64")

    cleaned["combined_text"] = (
        cleaned["product_name"].str.lower()
        + " "
        + cleaned["brand_names"].str.lower()
        + " "
        + cleaned["narrative_text"].str.lower()
    ).str.replace(r"\s+", " ", regex=True).str.strip()

    return cleaned


def filter_records(df: pd.DataFrame, keywords: Iterable[str], years: Iterable[int]) -> pd.DataFrame:
    """Filter records to target years and rows containing at least one keyword.

    Some source connectors, notably TGA DAEN, apply the keyword search before
    record retrieval by selecting matching devices. Individual adverse-event
    rows may not repeat the original device keyword, so those rows can opt in
    with ``source_query_match=True`` after the source has already constrained
    the result set.
    """
    year_values = list(years)
    source_type = df.get("source_type", pd.Series("", index=df.index)).fillna("").astype(str)
    device_registry = source_type == "regulatory_device_database"
    if year_values:
        filtered = df[df["year"].isin(year_values) | device_registry].copy()
    else:
        filtered = df.copy()
    filtered["keyword_hit"] = filtered["combined_text"].apply(lambda text: contains_any_keyword(text, keywords))
    if "source_query_match" in filtered.columns:
        source_match = filtered["source_query_match"].fillna(False).astype(bool)
        filtered.loc[source_match, "keyword_hit"] = True
    return filtered[filtered["keyword_hit"]].copy()


def deduplicate_records(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate by source/event_id when present, otherwise by source/date/text hash."""
    if df.empty:
        return df.copy()

    deduped = df.copy()
    deduped["event_id"] = deduped["event_id"].fillna("").astype(str).str.strip()

    with_id = deduped[deduped["event_id"] != ""].drop_duplicates(subset=["source", "event_id"], keep="first")
    no_id = deduped[deduped["event_id"] == ""].copy()

    if not no_id.empty:
        no_id["tmp_hash"] = (
            no_id["source"].astype(str)
            + "|"
            + no_id["event_date"].astype(str)
            + "|"
            + no_id["combined_text"].str.slice(0, 500)
        ).apply(hash)
        no_id = no_id.drop_duplicates(subset=["tmp_hash"], keep="first").drop(columns=["tmp_hash"])

    return pd.concat([with_id, no_id], ignore_index=True)


def summarize_by_year_category(df: pd.DataFrame) -> pd.DataFrame:
    """Count unique incidents by event year and classifier category."""
    if df.empty:
        return pd.DataFrame(columns=["year", "category", "incident_count"])
    return (
        df.groupby(["year", "category"], as_index=False)
        .agg(incident_count=("event_id", _incident_count))
        .sort_values(["year", "incident_count"], ascending=[True, False])
    )


def summarize_by_source_category(df: pd.DataFrame) -> pd.DataFrame:
    """Count unique incidents by source and classifier category."""
    if df.empty:
        return pd.DataFrame(columns=["source", "category", "incident_count"])
    return (
        df.groupby(["source", "category"], as_index=False)
        .agg(incident_count=("event_id", _incident_count))
        .sort_values(["source", "incident_count"], ascending=[True, False])
    )


def _incident_count(event_ids: pd.Series) -> int:
    """Count unique non-empty event IDs, or rows when IDs are unavailable."""
    normalized = event_ids.fillna("").astype(str)
    non_empty = normalized[normalized != ""]
    return int(non_empty.nunique()) if not non_empty.empty else int(len(event_ids))


def parse_args() -> argparse.Namespace:
    """Parse command-line options."""
    parser = argparse.ArgumentParser(description="Run the trocar incident data pipeline.")
    parser.add_argument("--keywords", nargs="*", default=DEFAULT_KEYWORDS, help="Search keywords.")
    parser.add_argument("--years", nargs="*", type=int, default=DEFAULT_TARGET_YEARS, help="Target event years.")
    parser.add_argument("--output-dir", default=str(EXPORTS_DIR), help="Directory for CSV and Excel exports.")
    parser.add_argument("--sources", nargs="*", default=DEFAULT_SELECTED_SOURCES, help="Selected source ids.")
    return parser.parse_args()


def main() -> None:
    """Run the pipeline from the command line."""
    args = parse_args()
    run_pipeline(
        keywords=args.keywords,
        years=args.years,
        output_dir=args.output_dir,
        selected_sources=args.sources,
    )


if __name__ == "__main__":
    main()
