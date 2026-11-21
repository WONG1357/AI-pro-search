"""Command-line entrypoint and orchestration for the trocar incident pipeline."""

from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from pipeline.classifier import classify_custom_incident_details, classify_incident_details
from pipeline.config import DEFAULT_KEYWORDS, DEFAULT_TARGET_YEARS, EXPORTS_DIR
from pipeline.export import export_pipeline_results
from pipeline.progress import ProgressReporter
from pipeline.sources import DEFAULT_SELECTED_SOURCES, SOURCE_REGISTRY, ensure_unified_columns
from pipeline.utils import contains_any_keyword, date_or_year_in_range, format_fda_date, parse_year_from_date, parse_date_value


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
    start_date: object | None = None,
    end_date: object | None = None,
    classifier_profile: str = "Trocar",
    **kwargs: Any,
) -> dict[str, Any]:
    """Run ingestion, cleaning, filtering, deduplication, classification, and export.

    Source coverage is retained separately from record counts. A failed source
    is never interpreted as a verified zero-result search.
    """
    keywords = list(dict.fromkeys(str(term).strip() for term in keywords if str(term).strip()))
    if not keywords:
        raise ValueError("At least one non-empty device keyword is required")
    years = sorted(set(int(year) for year in years))
    if any(year < 1900 or year > 9999 for year in years):
        raise ValueError("Years must be between 1900 and 9999")
    for label, value in [("start_date", start_date), ("end_date", end_date)]:
        if value is not None and parse_date_value(value) is None:
            raise ValueError(f"{label} must be a complete, valid date")
    if start_date is not None and end_date is not None and parse_date_value(start_date) > parse_date_value(end_date):
        raise ValueError("start_date must be on or before end_date")
    selected_sources = list(dict.fromkeys(DEFAULT_SELECTED_SOURCES if selected_sources is None else selected_sources))
    if not selected_sources:
        raise ValueError("At least one source must be selected")
    source_options = deepcopy(source_options or {})
    global_options = source_options.pop("_global", {})
    progress_reporter = kwargs.pop("progress_reporter", None) or ProgressReporter(selected_sources)
    request_timeout = global_options.get("request_timeout")
    max_pages = global_options.get("max_pages")
    max_records = global_options.get("max_records")
    debug = bool(global_options.get("debug", False))

    if mhra_csv_path is not None:
        print("[WARN] mhra_csv_path is deprecated; the selected online source connectors are used.")
    if kwargs:
        print(f"[WARN] Ignoring unsupported run_pipeline kwargs: {sorted(kwargs.keys())}")

    source_frames: list[pd.DataFrame] = []
    source_errors: list[dict[str, str]] = []
    source_warnings: list[dict[str, str]] = []
    source_status: list[dict[str, Any]] = []
    query = {"keywords": keywords, "years": years, "selected_sources": selected_sources,
             "start_date": str(start_date) if start_date is not None else None,
             "end_date": str(end_date) if end_date is not None else None,
             "classifier_profile": classifier_profile, "components": components or [],
             "accident_terms": accident_terms or []}

    for source_id in selected_sources:
        connector_cls = SOURCE_REGISTRY.get(source_id)
        if connector_cls is None:
            source_errors.append({"source_name": source_id, "error_message": "Unknown source id"})
            progress_reporter.fail_source(source_id, "Unknown source id")
            source_status.append({"source_name": source_id, "success": False, "completeness": "failed", "retrieved_count": 0, "total_available": None, "error_message": "Unknown source id"})
            continue

        progress_reporter.start_source(source_id, message="Source queued")
        options = dict(request_timeout=request_timeout, max_pages=max_pages, max_records=max_records, debug=debug)
        options.update(source_options.get(source_id, {}))
        # Reserved query fields cannot be replaced by source tuning options.
        options.update(keywords=keywords, years=years, components=None,
                       accident_terms=None, progress_reporter=progress_reporter,
                       start_date=start_date, end_date=end_date)
        try:
            result = connector_cls().fetch(**options)
        except Exception as exc:
            message = f"{type(exc).__name__}: source execution failed"
            progress_reporter.fail_source(source_id, message)
            source_errors.append({"source_name": source_id, "error_message": message})
            source_status.append({"source_name": source_id, "success": False, "completeness": "failed", "retrieved_count": 0, "total_available": None, "error_message": message})
            continue

        records = ensure_unified_columns(result.records)
        completeness = getattr(result, "completeness", "unverified")
        if completeness not in {"complete", "partial", "failed", "unverified"}:
            completeness = "unverified"
        if not result.success:
            completeness = "partial" if len(records) else "failed"
        source_status.append({
            "source_name": source_id, "success": bool(result.success),
            "completeness": completeness,
            "retrieved_count": getattr(result, "retrieved_count", None) if getattr(result, "retrieved_count", None) is not None else len(records),
            "returned_count": len(records),
            "total_available": getattr(result, "total_available", None),
            "effective_start": result.records.attrs.get("effective_start"),
            "effective_end": result.records.attrs.get("effective_end"),
            "error_message": result.error_message or "",
        })

        if result.warnings:
            source_warnings.extend(
                {"source_name": result.source_name, "warning_message": warning}
                for warning in result.warnings
            )
        if not result.success:
            source_errors.append(
                {
                    "source_name": result.source_name,
                    "error_message": result.error_message or "Unknown source error",
                }
            )
        if completeness == "failed":
            progress_reporter.fail_source(source_id, result.error_message or "Source failed")
        else:
            progress_reporter.complete_source(source_id, records_fetched=len(records), completeness=completeness)
        if not records.empty:
            source_frames.append(records)

    df_all = pd.concat(source_frames, ignore_index=True) if source_frames else ensure_unified_columns(pd.DataFrame())

    empty_results = {
        "all_raw": df_all,
        "filtered": pd.DataFrame(),
        "summary_year_category": pd.DataFrame(),
        "summary_source_category": pd.DataFrame(),
        "source_errors": pd.DataFrame(source_errors),
        "source_warnings": pd.DataFrame(source_warnings),
        "source_status": pd.DataFrame(source_status),
        "query": query,
        "progress": progress_reporter.get_snapshot(),
    }
    if df_all.empty:
        print("[WARN] No records retrieved.")
        if export_results:
            empty_results["export_files"] = export_pipeline_results(empty_results, output_dir, file_prefix=export_file_prefix)
        return empty_results

    df_all = clean_records(df_all)
    df_filtered = filter_records(df_all, keywords, years, start_date=start_date, end_date=end_date)
    df_filtered = deduplicate_records(df_filtered)
    for status in source_status:
        status["matched_count"] = int(df_filtered["source"].eq(status["source_name"]).sum())

    print(f"[INFO] Classifying {len(df_filtered):,} filtered records...")
    classification = classify_records(
        df_filtered,
        classifier_profile=classifier_profile,
        components=components,
        accident_terms=accident_terms,
    )
    df_filtered["category"] = classification.apply(lambda result: result.category)
    df_filtered["category_confidence"] = classification.apply(lambda result: result.confidence)
    df_filtered["category_reason"] = classification.apply(lambda result: result.reason)
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
        "source_status": pd.DataFrame(source_status),
        "query": query,
        "progress": progress_reporter.get_snapshot(),
    }

    if export_results:
        files = export_pipeline_results(results, output_dir, file_prefix=export_file_prefix)
        results["export_files"] = files
        print(f"[INFO] Pipeline finished with coverage status: {results['progress'].overall_status}.")
        print(f"Raw records:      {len(df_all):,}")
        print(f"Filtered records: {len(df_filtered):,}")
        print(f"Saved to folder:  {Path(output_dir).resolve()}")
        print(f"Excel report:     {files['excel'].resolve()}")

    return results


def classify_records(
    df: pd.DataFrame,
    classifier_profile: str = "Trocar",
    components: list[str] | None = None,
    accident_terms: list[str] | None = None,
) -> pd.Series:
    """Classify records using the selected device profile."""
    if str(classifier_profile).strip().lower() == "custom":
        return df["classification_text"].apply(
            lambda text: classify_custom_incident_details(
                text,
                components=components,
                accident_terms=accident_terms,
            )
        )
    return df["classification_text"].apply(classify_incident_details)


def clean_records(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize text and date columns and prepare searchable combined text."""
    cleaned = df.copy()

    for column in [
        "source",
        "source_type",
        "event_id",
        "report_number",
        "event_date",
        "report_date",
        "search_date",
        "product_name",
        "manufacturer",
        "narrative_text",
        "brand_names",
        "generic_name",
        "device_model",
        "product_code",
        "fda_match_field",
        "fda_match_category",
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
        "category_confidence",
        "category_reason",
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
        "fda_match_field",
        "fda_match_category",
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
        "category_confidence",
        "category_reason",
    ]:
        cleaned[column] = cleaned[column].fillna("").astype(str)

    for date_column in ["date_of_event", "date_received", "report_date", "event_date", "search_date"]:
        if date_column not in cleaned.columns:
            cleaned[date_column] = ""
        cleaned[date_column] = cleaned[date_column].fillna("").astype(str).apply(format_fda_date)

    missing_report_date = cleaned["report_date"].fillna("").astype(str).str.strip() == ""
    cleaned.loc[missing_report_date, "report_date"] = cleaned.loc[missing_report_date, "date_received"]

    missing_event_date = cleaned["event_date"].fillna("").astype(str).str.strip() == ""
    cleaned.loc[missing_event_date, "event_date"] = cleaned.loc[missing_event_date, "date_of_event"]

    cleaned["year"] = pd.to_numeric(cleaned["event_date"].apply(parse_year_from_date), errors="coerce").astype("Int64")
    cleaned["received_year"] = pd.to_numeric(
        cleaned["date_received"].apply(parse_year_from_date),
        errors="coerce",
    ).astype("Int64")
    source = cleaned.get("source", pd.Series("", index=cleaned.index)).fillna("").astype(str)
    report_date_sources = source.isin(["FDA_MAUDE", "TGA_DAEN"])
    cleaned["search_date"] = cleaned["event_date"].where(~report_date_sources, cleaned["report_date"])
    cleaned["search_date_basis"] = "event_date"
    cleaned.loc[report_date_sources, "search_date_basis"] = "report_date"
    missing_search_date = cleaned["search_date"].fillna("").astype(str).str.strip() == ""
    cleaned.loc[missing_search_date, "search_date"] = cleaned.loc[missing_search_date, "report_date"]
    cleaned.loc[missing_search_date, "search_date_basis"] = "report_date_fallback"
    cleaned.loc[cleaned["search_date"].eq(""), "search_date_basis"] = "unknown"
    cleaned["search_year"] = pd.to_numeric(
        cleaned["search_date"].apply(parse_year_from_date),
        errors="coerce",
    ).astype("Int64")

    search_fields = ["product_name", "brand_names", "generic_name", "device_model",
                     "manufacturer", "product_code", "device_problem_text",
                     "patient_problem_text", "narrative_text"]
    cleaned["combined_text"] = cleaned[search_fields].agg(" ".join, axis=1).str.lower().str.replace(r"\s+", " ", regex=True).str.strip()
    classification_fields = ["product_name", "brand_names", "generic_name", "device_model",
                             "event_type", "device_problem_text", "patient_problem_text",
                             "patient_outcome", "source_category_name", "narrative_text"]
    # Preserve field boundaries so 'No patient injury' cannot negate a separate
    # device-problem field describing a fracture.
    cleaned["classification_text"] = cleaned[classification_fields].apply(
        lambda row: ". ".join(value for value in row if value.strip()), axis=1
    ).str.lower().str.replace(r"\s+", " ", regex=True).str.strip()

    return cleaned


def filter_records(
    df: pd.DataFrame,
    keywords: Iterable[str],
    years: Iterable[int],
    start_date: object | None = None,
    end_date: object | None = None,
) -> pd.DataFrame:
    """Filter records to target years, date range, and keyword matches.

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
        search_year_match = df.get("search_year", df["year"]).isin(year_values)
        filtered = df[search_year_match | device_registry].copy()
    else:
        filtered = df.copy()
    if start_date is not None or end_date is not None:
        filtered_source_type = filtered.get("source_type", pd.Series("", index=filtered.index)).fillna("").astype(str)
        filtered_device_registry = filtered_source_type == "regulatory_device_database"
        date_basis = filtered.get("search_date", filtered["event_date"])
        date_mask = date_basis.apply(lambda value: date_or_year_in_range(value, start_date, end_date))
        filtered = filtered[date_mask | filtered_device_registry].copy()
    filtered["keyword_hit"] = filtered["combined_text"].apply(lambda text: contains_any_keyword(text, keywords))
    if "source_query_match" in filtered.columns:
        source_match = filtered["source_query_match"].apply(
            lambda value: value is True or isinstance(value, (int, float)) and value == 1
            or isinstance(value, str) and value.strip().lower() in {"true", "1"}
        )
        filtered.loc[source_match, "keyword_hit"] = True
    return filtered[filtered["keyword_hit"]].copy()


def deduplicate_records(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate by source/event_id when present, otherwise by source/date/text hash."""
    if df.empty:
        return df.copy()

    deduped = df.copy()
    deduped["event_id"] = deduped["event_id"].fillna("").astype(str).str.strip()
    deduped["record_uid"] = deduped.apply(_record_uid, axis=1)
    deduped["duplicate_count"] = deduped.groupby("record_uid")["record_uid"].transform("size") - 1
    return deduped.drop_duplicates(subset=["record_uid"], keep="first").reset_index(drop=True)


def _record_uid(row: pd.Series) -> str:
    source = str(row.get("source") or "")
    event_id = str(row.get("event_id") or "").strip()
    if event_id:
        identity = [source, "event_id", event_id]
    else:
        fields = ["report_number", "event_date", "report_date", "product_name", "manufacturer",
                  "brand_names", "generic_name", "device_model", "combined_text"]
        identity = [source, "content"] + [str(row.get(field) or "") for field in fields]
    return hashlib.sha256(json.dumps(identity, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


def _summary_records(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    # IDs are unique only inside a source; missing IDs still represent records.
    source = working.get("source", pd.Series("", index=working.index)).fillna("").astype(str)
    event_id = working.get("event_id", pd.Series("", index=working.index)).fillna("").astype(str).str.strip()
    working["_count_identity"] = [
        json.dumps([s, identifier]) if identifier else f"missing:{position}"
        for position, (s, identifier) in enumerate(zip(source, event_id))
    ]
    return working


def summarize_by_year_category(df: pd.DataFrame) -> pd.DataFrame:
    """Count unique incidents by selected search year and classifier category."""
    if df.empty:
        return pd.DataFrame(columns=["year", "category", "incident_count"])
    year_column = "search_year" if "search_year" in df.columns else "year"
    return (
        _summary_records(df).groupby([year_column, "category"], as_index=False, dropna=False)
        .agg(incident_count=("_count_identity", "nunique"))
        .rename(columns={year_column: "year"})
        .sort_values(["year", "incident_count"], ascending=[True, False])
    )


def summarize_by_source_category(df: pd.DataFrame) -> pd.DataFrame:
    """Count unique incidents by source and classifier category."""
    if df.empty:
        return pd.DataFrame(columns=["source", "category", "incident_count"])
    return (
        _summary_records(df).groupby(["source", "category"], as_index=False, dropna=False)
        .agg(incident_count=("_count_identity", "nunique"))
        .sort_values(["source", "incident_count"], ascending=[True, False])
    )


def _incident_count(event_ids: pd.Series) -> int:
    """Count unique non-empty event IDs, or rows when IDs are unavailable."""
    normalized = event_ids.fillna("").astype(str)
    non_empty = normalized[normalized != ""]
    return int(non_empty.nunique()) + int((normalized == "").sum())


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
