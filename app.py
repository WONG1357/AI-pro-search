"""Streamlit dashboard for the medical device incident search platform."""

from __future__ import annotations

import re
import tempfile
from datetime import date
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd
import streamlit as st

from cloud_agent.evidence import direct_record_url, official_url
from cloud_agent.schema import SOURCE_INFO
from pipeline.config import DEFAULT_KEYWORDS, EXPORTS_DIR
from pipeline.export import spreadsheet_safe_frame
from pipeline.pipeline import run_pipeline
from pipeline.progress import PipelineProgress, ProgressReporter, format_seconds
from pipeline.sources import DEFAULT_SELECTED_SOURCES, SOURCE_DISPLAY_NAMES, SOURCE_REGISTRY
PAGE_TITLE = "Medical Device Incident Search Platform"
APP_STATE_VERSION = "submitted-search-v1"
DEFAULT_TROCAR_KEYWORDS = ", ".join(DEFAULT_KEYWORDS)
DEFAULT_SEARCH_START_DATE = date(2024, 1, 1)
DEFAULT_SEARCH_END_DATE = date.today()
DEFAULT_REQUEST_TIMEOUT = 60
SEARCH_SCOPE_FIELDS = (
    "device_name", "device_profile", "keywords", "components", "accident_terms",
    "start_date", "end_date", "selected_sources", "request_timeout", "max_pages", "max_records",
)
FDA_MATCH_CATEGORIES = ["Model number matches", "Product code matches", "Brand name matches", "Generic name matches", "Narrative matches", "Other matches"]
PRIMARY_COLUMNS = [
    "source",
    "event_id",
    "report_number",
    "event_date",
    "year",
    "product_name",
    "brand_names",
    "category",
    "event_link",
]
FIELD_LABELS = {
    "source": "Source",
    "event_id": "Event ID",
    "report_number": "Report number",
    "date_of_event": "Event date",
    "date_received": "Date received",
    "report_date": "Report date",
    "event_date": "Event date",
    "year": "Year",
    "received_year": "Report year",
    "search_date": "Date used for filtering",
    "search_date_basis": "Search date basis",
    "search_year": "Search year",
    "product_name": "Product name",
    "brand_names": "Brand / generic names",
    "generic_name": "Generic name",
    "manufacturer": "Manufacturer",
    "device_model": "Device model",
    "product_code": "Product code",
    "fda_match_category": "FDA match category",
    "fda_match_field": "FDA match field",
    "event_type": "Event type",
    "patient_outcome": "Patient outcome",
    "device_problem_text": "Device problem",
    "patient_problem_text": "Patient problem",
    "source_category_name": "Source category",
    "category": "Category",
    "category_confidence": "Category confidence",
    "category_reason": "Category reason",
    "event_link": "Source link",
    "device_link": "Notice document",
    "raw_link": "Raw source",
}


def main() -> None:
    """Render the dashboard and coordinate user-triggered pipeline runs."""
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    reset_stale_session_state()
    st.title(PAGE_TITLE)

    sidebar_state = render_sidebar()
    if sidebar_state["run_search"]:
        execute_search(sidebar_state)

    results = st.session_state.get("results")
    if not results:
        st.info("Set your search options in the sidebar, then click Run Search.")
        return

    scope = st.session_state["submitted_scope"]
    st.subheader(f"Results for {scope['device_name']}")
    if any(sidebar_state[field] != scope[field] for field in SEARCH_SCOPE_FIELDS):
        st.warning("Search inputs have changed. These results and downloads are from the previous search. Select Run Search to apply the new inputs.")
    render_submitted_scope(scope)
    render_dashboard(results)


def reset_stale_session_state() -> None:
    """Clear old in-memory results after source connector/layout changes."""
    if st.session_state.get("app_state_version") == APP_STATE_VERSION:
        return
    st.session_state["app_state_version"] = APP_STATE_VERSION
    for key in ["results", "submitted_scope", "export_prefix"]:
        st.session_state.pop(key, None)


def render_sidebar() -> dict[str, object]:
    """Render search controls and return normalized sidebar state."""
    with st.sidebar:
        st.header("Search")
        device_profile = st.selectbox("Device profile", ["Trocar", "Custom"])

        if device_profile == "Trocar":
            device_name = "Trocar"
            keyword_text = st.text_input("Keywords", value=DEFAULT_TROCAR_KEYWORDS)
            component_text = ""
            accident_text = ""
        else:
            device_name = st.text_input("Device name", value="").strip() or "Custom device"
            keyword_text = st.text_area("Device keywords", value="", help="Enter device names, model numbers, or product codes, separated by commas or new lines.")
            component_text = st.text_area("Components to categorize", value="", help="Optional. These terms label retrieved records.")
            accident_text = st.text_area("Reported issues to categorize", value="", help="Optional. These terms label retrieved records.")

        st.subheader("Sources")
        selected_sources = []
        for source_id, display_name in SOURCE_DISPLAY_NAMES.items():
            default_value = source_id in DEFAULT_SELECTED_SOURCES
            if st.checkbox(display_name, value=default_value, key=f"source_{source_id}"):
                selected_sources.append(source_id)
        st.caption("Select each regulator you want to search.")
        st.subheader("Date range")
        search_date_range = st.date_input(
            "Search window",
            value=(DEFAULT_SEARCH_START_DATE, DEFAULT_SEARCH_END_DATE),
            key="search_date_range",
            help="Dates refer to the report, notice, or event date used by each source. The results show the date used for filtering.",
        )
        search_start_date, search_end_date = normalize_search_date_range(search_date_range)
        with st.expander("Request limits"):
            request_timeout = st.number_input("Request timeout (seconds)", min_value=5, max_value=120, value=20)
            max_pages = st.number_input("Maximum pages per source query", min_value=1, max_value=100, value=20)
            max_records = st.number_input("Maximum records per source", min_value=1, max_value=10000, value=2000)
            st.caption("A reached limit is reported as partial coverage.")
        run_search = st.button("Run Search", type="primary", use_container_width=True)

    keywords = parse_keyword_text(keyword_text)
    components = parse_keyword_text(component_text)
    accident_terms = parse_keyword_text(accident_text)
    # Component and accident terms describe classification, not an alternative
    # device identity. OR-ing 'injury' into retrieval admitted unrelated devices.
    search_terms = keywords
    return {
        "device_profile": device_profile,
        "device_name": device_name,
        "keywords": search_terms,
        "base_keywords": keywords,
        "components": components,
        "accident_terms": accident_terms,
        "start_date": search_start_date,
        "end_date": search_end_date,
        "selected_sources": selected_sources,
        "request_timeout": request_timeout,
        "max_pages": max_pages,
        "max_records": max_records,
        "debug_logs": False,
        "run_search": run_search,
    }


def normalize_search_date_range(value: object) -> tuple[date | None, date | None]:
    """Normalize Streamlit date range input while a user is mid-edit."""
    if isinstance(value, (tuple, list)):
        if len(value) >= 2:
            return value[0], value[1]
        if len(value) == 1:
            return value[0], value[0]
        return None, None
    if isinstance(value, date):
        return value, value
    return None, None


def execute_search(search_state: dict[str, object]) -> None:
    """Run the pipeline and save result dataframes plus export paths in session state."""
    keywords = search_state["keywords"]
    start_date = search_state["start_date"]
    end_date = search_state["end_date"]

    if not keywords:
        st.error("Enter at least one device keyword. Component and accident terms are used for classification.")
        return
    if start_date is None or end_date is None:
        st.error("Select a valid date range.")
        return
    if start_date > end_date:
        st.error("The start date must be on or before the end date.")
        return
    if not search_state["selected_sources"]:
        st.error("Select at least one data source.")
        return

    device_slug = slugify(str(search_state["device_name"]))
    export_prefix = f"{device_slug}_incident_report_{date.today().isoformat()}"
    source_options: dict[str, dict[str, object]] = {}
    if "TGA_DAEN" in search_state["selected_sources"]:
        source_options["TGA_DAEN"] = {}
    source_options["_global"] = {
        "request_timeout": search_state.get("request_timeout"),
        "max_pages": search_state.get("max_pages"),
        "max_records": search_state.get("max_records"),
        "debug": search_state.get("debug_logs"),
    }

    progress_box = st.container()
    overall_bar = progress_box.progress(0.0)
    summary_slot = progress_box.empty()
    table_slot = progress_box.empty()
    logs_slot = progress_box.empty()

    def render_progress(snapshot: PipelineProgress) -> None:
        overall_pct = snapshot.overall_percent_complete or 0.0
        overall_bar.progress(min(1.0, max(0.0, overall_pct / 100.0)))
        summary_slot.markdown(
            f"**Current source:** {snapshot.current_source or 'None'}  \n"
            f"**Overall:** {overall_pct:.1f}%  \n"
            f"**Elapsed:** {format_seconds(snapshot.elapsed_seconds)}  \n"
            f"**ETA:** {format_seconds(snapshot.eta_seconds)}  \n"
            f"**Status:** {snapshot.overall_status}"
        )
        table_slot.dataframe(progress_table(snapshot), use_container_width=True, hide_index=True)
        with logs_slot.expander("Progress logs", expanded=False):
            st.text("\n".join(snapshot.logs[-20:]))

    reporter = ProgressReporter(list(search_state["selected_sources"]), on_update=render_progress)

    try:
        with st.spinner("Searching adverse event records..."):
            results = run_pipeline(
                keywords=keywords,
                years=years_from_date_range(start_date, end_date),
                start_date=start_date,
                end_date=end_date,
                output_dir=EXPORTS_DIR,
                export_results=True,
                export_file_prefix=export_prefix,
                selected_sources=list(search_state["selected_sources"]),
                source_options=source_options,
                progress_reporter=reporter,
                classifier_profile=str(search_state.get("device_profile") or "Trocar"),
                components=list(search_state.get("components") or []),
                accident_terms=list(search_state.get("accident_terms") or []),
            )
    except Exception:
        message = "The search could not finish. Try again or narrow the date range or sources."
        if st.session_state.get("results") is not None:
            message += " The previous search and its downloads are still available below."
        st.error(message)
        return

    st.session_state["results"] = results
    st.session_state["submitted_scope"] = {field: search_state[field] for field in SEARCH_SCOPE_FIELDS}
    st.session_state["export_prefix"] = export_prefix


def render_submitted_scope(scope: dict[str, object]) -> None:
    """Keep the dates, device terms, and selected regulators beside the saved results."""
    st.text(f"Requested dates: {scope['start_date']} to {scope['end_date']}")
    st.text(f"Device terms: {', '.join(scope['keywords'])}")
    sources = ", ".join(SOURCE_DISPLAY_NAMES.get(source, source) for source in scope["selected_sources"])
    st.text(f"Selected sources: {sources}")


def years_from_date_range(start_date: date, end_date: date) -> list[int]:
    """Return the inclusive set of years covered by a date range."""
    return list(range(start_date.year, end_date.year + 1))


def progress_table(snapshot: PipelineProgress) -> pd.DataFrame:
    """Convert a progress snapshot to a display table."""
    rows = []
    for source in snapshot.sources.values():
        rows.append(
            {
                "source_name": source.source_name,
                "status": source.status,
                "stage": source.stage,
                "percent_complete": source.percent_complete,
                "records_fetched": source.records_fetched,
                "elapsed_time": format_seconds(source.elapsed_seconds),
                "eta": format_seconds(source.eta_seconds),
                "latest_message": source.message,
            }
        )
    return pd.DataFrame(rows)



def render_dashboard(results: dict[str, pd.DataFrame]) -> None:
    """Render source-aware dashboard pages."""
    statuses = results.get("source_status", pd.DataFrame())
    coverage = statuses.get("completeness", pd.Series(dtype=str))
    complete = not coverage.empty and coverage.eq("complete").all()
    render_source_messages(results)
    filtered = results.get("filtered", pd.DataFrame())
    all_raw = results.get("all_raw", pd.DataFrame())
    if filtered.empty:
        if complete:
            st.info("No records matched the selected sources, device terms, and date filters. Database reports alone cannot rule out events.")
        elif not coverage.empty and coverage.eq("failed").all():
            st.warning("No matches could be confirmed. All selected sources failed. Review the source errors and try again.")
        else:
            st.warning("No matches could be confirmed. Some selected sources could not be fully checked. Review the source coverage before drawing conclusions.")
    elif not complete:
        st.warning("Some selected sources could not be fully checked. The records below cover only the data retrieved.")
    render_evidence_download(results.get("export_files", {}))
    if filtered.empty and all_raw.empty:
        with st.expander("Exports & raw data"):
            render_debug_raw_data(results)
        return

    overview_tab, source_tab, detail_tab, audit_tab, debug_tab = st.tabs(
        ["Overview", "Source Results", "Record Detail", "Record Count Audit", "Exports & Raw Data"]
    )
    with overview_tab:
        render_overview(filtered, all_raw)
    with source_tab:
        render_source_results(filtered, statuses)
    with detail_tab:
        render_record_detail_page(filtered)
    with audit_tab:
        render_record_count_audit(results)
    with debug_tab:
        render_debug_raw_data(results)


def render_source_messages(results: dict[str, pd.DataFrame]) -> None:
    """Show connector warnings and errors without blocking the dashboard."""
    warnings = results.get("source_warnings", pd.DataFrame())
    errors = results.get("source_errors", pd.DataFrame())
    statuses = results.get("source_status", pd.DataFrame())
    if not statuses.empty:
        st.caption("Source coverage for this search")
        st.dataframe(statuses, use_container_width=True, hide_index=True, column_config={
            "source_name": "Source", "completeness": "Coverage", "effective_start": "Effective start",
            "effective_end": "Effective end", "retrieved_count": "Retrieved", "returned_count": "Returned",
            "matched_count": "Matches after filtering", "total_available": "Total reported by source",
            "error_message": "Retrieval issue",
        })
    audit, warnings = split_source_audit_messages(warnings)
    if warnings.empty and errors.empty and audit.empty:
        return

    with st.expander("Source status messages", expanded=False):
        if not audit.empty:
            st.info("Source count audit messages.")
            st.dataframe(audit, use_container_width=True, hide_index=True)
        if not warnings.empty:
            st.warning("Some sources returned warnings.")
            st.dataframe(warnings, use_container_width=True, hide_index=True)
        if not errors.empty:
            st.error("Some sources failed.")
            st.dataframe(errors, use_container_width=True, hide_index=True)


def render_overview(filtered: pd.DataFrame, all_raw: pd.DataFrame) -> None:
    """Render high-level cross-source counts."""
    st.caption("Counts describe retrieved records. A report or safety notice does not establish causation, incidence, or a unique patient.")
    kinds = filtered.get("source", pd.Series(dtype=str)).map({source: info[2] for source, info in SOURCE_INFO.items()})
    st.caption(f"Included after filtering: {kinds.eq('adverse_event_report').sum():,} adverse-event reports and {kinds.eq('safety_notice').sum():,} safety notices.")
    col_total, col_raw, col_sources = st.columns(3)
    col_total.metric("Filtered records", f"{len(filtered):,}")
    col_raw.metric("Raw records", f"{len(all_raw):,}")
    col_sources.metric("Sources with records", f"{filtered['source'].nunique() if 'source' in filtered.columns and not filtered.empty else 0:,}")

    col_source, col_year, col_category = st.columns(3)
    with col_source:
        st.caption("Record count by source")
        st.dataframe(count_table(filtered, "source"), use_container_width=True, hide_index=True)
    with col_year:
        st.caption("Record count by selected date year")
        st.dataframe(count_table(filtered, display_year_column(filtered)), use_container_width=True, hide_index=True)
    with col_category:
        st.caption("Record count by category")
        st.dataframe(count_table(filtered, "category"), use_container_width=True, hide_index=True)

    if not filtered.empty and "category" in filtered.columns:
        st.subheader("Top categories")
        top_categories = count_table(filtered, "category").head(10)
        if not top_categories.empty:
            st.bar_chart(top_categories.set_index("category"))


def count_table(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Return a simple count table that is safe for empty/missing columns."""
    if df.empty or column not in df.columns:
        return pd.DataFrame(columns=[column, "record_count"])
    values = df[column].astype("object").where(df[column].notna(), "Unknown").astype(str).replace("", "Unknown")
    counts = (
        values
        .value_counts(dropna=False)
        .rename_axis(column)
        .reset_index(name="record_count")
    )
    return counts


def render_source_results(filtered: pd.DataFrame, statuses: pd.DataFrame) -> None:
    """Render one results tab per source."""
    coverage = dict(zip(statuses.get("source_name", []), statuses.get("completeness", [])))
    source_order = [source for source in source_result_order(filtered) if source in coverage]
    if not source_order:
        st.info("No source coverage is available for this search.")
        return
    tabs = st.tabs([SOURCE_DISPLAY_NAMES.get(source, source) for source in source_order])
    for source_id, tab in zip(source_order, tabs):
        with tab:
            source_df = filtered[filtered.get("source", pd.Series(dtype=str)).astype(str) == source_id].copy() if not filtered.empty else pd.DataFrame()
            render_source_result_tab(source_id, source_df, coverage[source_id])


def render_source_result_tab(source_id: str, df: pd.DataFrame, coverage: str) -> None:
    """Render source-specific metrics, filters, table, and selector."""
    display_name = SOURCE_DISPLAY_NAMES.get(source_id, source_id)
    st.subheader(display_name)
    if df.empty:
        if coverage == "complete":
            st.info("No records matched this search within the completed source query.")
        else:
            st.warning(f"No matches could be confirmed. This source has {coverage} coverage.")
        return

    metric_cols = st.columns(4)
    metric_cols[0].metric("Records", f"{len(df):,}")
    year_column = display_year_column(df)
    metric_cols[1].metric("Search years", f"{df[year_column].nunique() if year_column in df.columns else 0:,}")
    metric_cols[2].metric("Categories", f"{df['category'].nunique() if 'category' in df.columns else 0:,}")
    metric_cols[3].metric("With links", f"{count_rows_with_links(df):,}")

    if source_id == "FDA_MAUDE":
        render_fda_source_result_tab(source_id, df)
        return

    visible = apply_source_filters(source_id, df)
    columns = source_table_columns(source_id, visible)
    st.caption(f"{len(visible):,} records shown")
    st.dataframe(
        visible[columns].copy() if columns else visible,
        use_container_width=True,
        hide_index=True,
        column_config=link_column_config(visible),
    )

    if visible.empty:
        st.info("No records match the selected filters.")
        return

    labels = [format_record_label(index, row) for index, row in visible.reset_index(drop=True).iterrows()]
    selected_label = st.selectbox("Select a record", labels, key=f"source_selector_{source_id}")
    selected = visible.reset_index(drop=True).iloc[labels.index(selected_label)]
    render_source_detail(source_id, selected, visible, scope=f"source_results_{source_id}")


def render_fda_source_result_tab(source_id: str, df: pd.DataFrame) -> None:
    """Render FDA MAUDE records grouped by openFDA match category."""
    order = FDA_MATCH_CATEGORIES
    categories = [category for category in order if (df.get("fda_match_category", pd.Series(dtype=str)).fillna("").astype(str) == category).any()]
    extra_categories = [
        category
        for category in sorted(df.get("fda_match_category", pd.Series(dtype=str)).fillna("Other matches").astype(str).unique())
        if category and category not in categories
    ]
    categories.extend(extra_categories)
    if not categories:
        categories = ["Other matches"]

    tabs = st.tabs([f"{category} ({count_fda_category(df, category):,})" for category in categories])
    for category, tab in zip(categories, tabs):
        with tab:
            category_df = df[df.get("fda_match_category", pd.Series("", index=df.index)).fillna("Other matches").astype(str) == category].copy()
            if category_df.empty:
                st.info(f"No FDA MAUDE {category.lower()} records are available.")
                continue
            visible = apply_source_filters(f"{source_id}_{slugify(category)}", category_df)
            columns = source_table_columns(source_id, visible)
            st.caption(f"{len(visible):,} records shown")
            st.dataframe(
                visible[columns].copy() if columns else visible,
                use_container_width=True,
                hide_index=True,
                column_config=link_column_config(visible),
            )
            if visible.empty:
                st.info("No records match the selected filters.")
                continue
            labels = [format_record_label(index, row) for index, row in visible.reset_index(drop=True).iterrows()]
            selected_label = st.selectbox("Select a record", labels, key=f"source_selector_{source_id}_{slugify(category)}")
            selected = visible.reset_index(drop=True).iloc[labels.index(selected_label)]
            render_source_detail(source_id, selected, visible, scope=f"source_results_{source_id}_{slugify(category)}")


def count_fda_category(df: pd.DataFrame, category: str) -> int:
    if df.empty or "fda_match_category" not in df.columns:
        return 0
    return int((df["fda_match_category"].fillna("Other matches").astype(str) == category).sum())


def apply_source_filters(source_id: str, df: pd.DataFrame) -> pd.DataFrame:
    """Apply source-specific filters."""
    filtered = df.copy()
    columns = st.columns(3)
    with columns[0]:
        category_values = sorted(filtered.get("category", pd.Series(dtype=str)).dropna().astype(str).unique())
        selected_categories = st.multiselect("Category", category_values, default=category_values, key=f"filter_category_{source_id}")
    with columns[1]:
        if source_id == "HEALTH_CANADA_MDI":
            field = "event_type"
            label = "Hazard severity"
        elif source_id == "TGA_DAEN":
            field = "generic_name"
            label = "GMDN term"
        elif source_id in {"SWISSMEDIC_FSCA", "MHRA_FSCA", "BFARM_RECALLS"}:
            field = "event_type"
            label = "Recall status"
        else:
            field = "manufacturer"
            label = "Manufacturer"
        values = filter_values(filtered, field)
        selected_values = st.multiselect(label, values, default=[], key=f"filter_field_{source_id}")
    with columns[2]:
        query = st.text_input("Text search", key=f"filter_text_{source_id}")

    if "category" in filtered.columns:
        filtered = filtered[filtered["category"].isin(selected_categories)]
    if selected_values and field in filtered.columns:
        filtered = filtered[filtered[field].fillna("").astype(str).isin(selected_values)]
    if query.strip():
        text = (
            filtered.get("product_name", pd.Series("", index=filtered.index)).fillna("").astype(str)
            + " "
            + filtered.get("manufacturer", pd.Series("", index=filtered.index)).fillna("").astype(str)
            + " "
            + filtered.get("narrative_text", pd.Series("", index=filtered.index)).fillna("").astype(str)
        )
        filtered = filtered[text.str.contains(re.escape(query.strip()), case=False, na=False)]
    return filtered


def source_result_order(filtered: pd.DataFrame) -> list[str]:
    """Return registry sources plus any unexpected source ids in result data."""
    order = list(SOURCE_REGISTRY.keys())
    if filtered.empty or "source" not in filtered.columns:
        return order
    extras = [
        source
        for source in sorted(filtered["source"].dropna().astype(str).unique())
        if source not in order
    ]
    return order + extras


def filter_values(df: pd.DataFrame, field: str) -> list[str]:
    """Return all filter values without silently capping the displayed records."""
    if df.empty or field not in df.columns:
        return []
    return sorted(df[field].fillna("").astype(str).unique())


def source_table_columns(source_id: str, df: pd.DataFrame) -> list[str]:
    """Return source-specific display columns present in the dataframe."""
    mapping = {
        "FDA_MAUDE": ["fda_match_category", "event_id", "report_number", "report_date", "event_date", "search_year", "product_name", "brand_names", "manufacturer", "event_type", "category", "category_confidence", "event_link"],
        "TGA_DAEN": ["report_number", "report_date", "event_date", "search_year", "product_name", "manufacturer", "generic_name", "event_type", "category", "category_confidence"],
        "HEALTH_CANADA_MDI": ["event_id", "date_received", "search_year", "product_name", "generic_name", "manufacturer", "event_type", "device_problem_text", "patient_outcome", "category", "category_confidence"],
        "SWISSMEDIC_FSCA": ["event_id", "search_year", "manufacturer", "product_name", "generic_name", "device_model", "event_type", "device_problem_text", "category", "category_confidence", "device_link"],
        "MHRA_FSCA": ["event_id", "search_year", "manufacturer", "product_name", "device_model", "event_type", "device_problem_text", "category", "category_confidence", "event_link"],
        "BFARM_RECALLS": ["event_id", "search_year", "manufacturer", "product_name", "event_type", "device_problem_text", "category", "category_confidence", "event_link"],
    }
    preferred = mapping.get(source_id, PRIMARY_COLUMNS) + ["search_date", "search_date_basis"]
    return [column for column in preferred if column in df.columns and column_has_values(df[column])]


def column_has_values(values: pd.Series) -> bool:
    """Return whether a dataframe column has displayable content."""
    return bool(values.astype("string").fillna("").str.strip().ne("").any())


def display_year_column(df: pd.DataFrame) -> str:
    """Use the same date basis users selected for dashboard year counts."""
    if "search_year" in df.columns and column_has_values(df["search_year"]):
        return "search_year"
    return "year"


def render_record_detail_page(filtered: pd.DataFrame) -> None:
    """Render source-specific detail view for any selected record."""
    if filtered.empty:
        st.info("No records are available for detail review.")
        return
    sources = sorted(filtered["source"].dropna().astype(str).unique())
    selected_source = st.selectbox("Source", sources, key="detail_source")
    source_df = filtered[filtered["source"].astype(str) == selected_source].reset_index(drop=True)
    if selected_source == "FDA_MAUDE":
        source_df = filter_fda_detail_category(source_df)
        if source_df.empty:
            st.info("No FDA MAUDE records are available for the selected match category.")
            return
    labels = [format_record_label(index, row) for index, row in source_df.iterrows()]
    selected_label = st.selectbox("Record", labels, key="detail_record")
    selected = source_df.iloc[labels.index(selected_label)]
    render_source_detail(selected_source, selected, source_df, scope=f"record_detail_{selected_source}")


def filter_fda_detail_category(source_df: pd.DataFrame) -> pd.DataFrame:
    categories = [
        category
        for category in FDA_MATCH_CATEGORIES
        if (source_df.get("fda_match_category", pd.Series(dtype=str)).fillna("").astype(str) == category).any()
    ]
    if not categories:
        return source_df
    selected_category = st.selectbox("FDA match category", categories, key="detail_fda_match_category")
    return source_df[source_df["fda_match_category"].fillna("").astype(str) == selected_category].reset_index(drop=True)


def render_source_detail(source_id: str, record: pd.Series, context_df: pd.DataFrame | None = None, scope: str = "detail") -> None:
    """Dispatch to a source-specific detail renderer."""
    if source_id == "HEALTH_CANADA_MDI":
        render_health_canada_mdi_detail(record, context_df, scope=scope)
        return
    renderers = {
        "FDA_MAUDE": render_fda_maude_detail,
        "TGA_DAEN": render_tga_daen_detail,
        "SWISSMEDIC_FSCA": render_swissmedic_fsca_detail,
        "MHRA_FSCA": render_mhra_fsca_detail,
        "BFARM_RECALLS": render_bfarm_recall_detail,
    }
    renderers.get(source_id, render_generic_detail)(record)


def render_fda_maude_detail(record: pd.Series) -> None:
    render_detail_fields(record, ["fda_match_category", "event_id", "report_number", "report_date", "event_date", "search_date", "search_date_basis", "product_name", "brand_names", "manufacturer", "event_type", "patient_outcome", "device_problem_text", "patient_problem_text", "category", "category_confidence", "category_reason"])


def render_tga_daen_detail(record: pd.Series) -> None:
    render_detail_fields(
        record,
        ["report_number", "report_date", "event_date", "search_date_basis", "product_name", "manufacturer", "generic_name", "product_code", "event_type", "category", "category_confidence", "category_reason"],
        missing_link_message="No direct TGA DAEN case link is available for this record.",
        render_link=False,
    )


def render_health_canada_mdi_detail(record: pd.Series, context_df: pd.DataFrame | None = None, scope: str = "detail") -> None:
    render_detail_fields(record, ["event_id", "date_received", "search_date", "search_date_basis", "product_name", "generic_name", "manufacturer", "event_type", "device_problem_text", "patient_outcome", "product_code", "category"], render_link=False)
    locator = official_url(record.get("raw_link"), "HEALTH_CANADA_MDI") or official_url(record.get("event_link"), "HEALTH_CANADA_MDI")
    if locator:
        st.link_button("Open Health Canada database (locator)", str(locator))
    render_json_section("Health Canada-specific fields", record.get("source_specific"))
    render_health_canada_downloads(record, context_df, scope=scope)


def render_swissmedic_fsca_detail(record: pd.Series) -> None:
    render_detail_fields(
        record,
        ["event_id", "search_date", "search_date_basis", "manufacturer", "product_name", "generic_name", "device_model", "event_type", "device_problem_text", "source_category_name", "category"],
        missing_link_message="No direct recall document link available for this record.",
        render_link=False,
    )
    render_json_section("Swissmedic-specific fields", record.get("source_specific"))
    render_swissmedic_actions(record)


def render_mhra_fsca_detail(record: pd.Series) -> None:
    render_detail_fields(
        record,
        ["event_id", "search_date", "search_date_basis", "manufacturer", "product_name", "device_model", "event_type", "device_problem_text", "source_category_name", "category", "event_link", "raw_link"],
        missing_link_message="No direct field safety notice link available for this record.",
    )
    render_json_section("MHRA-specific fields", record.get("source_specific"))


def render_bfarm_recall_detail(record: pd.Series) -> None:
    render_detail_fields(
        record,
        ["event_id", "search_date", "search_date_basis", "manufacturer", "product_name", "event_type", "device_problem_text", "source_category_name", "category", "event_link"],
        missing_link_message="No direct BfArM recall link available for this record.",
    )
    render_json_section("BfArM-specific fields", record.get("source_specific"))


def render_generic_detail(record: pd.Series) -> None:
    render_detail_fields(record, ["source", "event_id", "event_date", "product_name", "manufacturer", "category", "event_link"])


def render_detail_fields(
    record: pd.Series,
    fields: list[str],
    missing_link_message: str = "No direct incident link available for this record.",
    render_link: bool = True,
) -> None:
    """Render a compact details section with graceful missing-link handling."""
    rows = []
    for field in fields:
        value = record.get(field, "")
        if value not in (None, ""):
            rows.append({"field": FIELD_LABELS.get(field, field), "value": value})
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    st.subheader("Narrative / description")
    st.write(record.get("narrative_text") or "No narrative text available.")
    if render_link:
        source = str(record.get("source", ""))
        link = direct_record_url(record, source)
        locator = official_url(record.get("raw_link"), source) or official_url(record.get("event_link"), source)
        if link:
            st.link_button("Open source record", link)
        elif locator:
            st.link_button("Open source listing (locator)", locator)
        else:
            st.info(missing_link_message)
    render_json_section("Raw record", record.get("raw_record"))


def render_swissmedic_actions(record: pd.Series) -> None:
    """Render Swissmedic search-result and document actions."""
    source_link = str(record.get("event_link") or record.get("raw_link") or "").strip()
    download_link = str(record.get("device_link") or "").strip()
    if not download_link:
        source_specific = record.get("source_specific")
        if isinstance(source_specific, str) and source_specific.strip():
            try:
                parsed = json.loads(source_specific)
                source_link = source_link or str(parsed.get("search_link") or "").strip()
                download_link = str(parsed.get("download_link") or "").strip()
                documents = parsed.get("documents") or []
                if not download_link and documents and isinstance(documents, list):
                    download_link = str(documents[0].get("download_url") or "").strip()
            except Exception:
                download_link = ""
    if not source_link and not download_link:
        return
    col_source, col_download = st.columns(2)
    with col_source:
        if source_link:
            st.link_button("Open Swissmedic listing (locator)", source_link, use_container_width=True)
    with col_download:
        if download_link:
            st.link_button("Download Swissmedic file", download_link, use_container_width=True)


def render_health_canada_downloads(record: pd.Series, context_df: pd.DataFrame | None = None, scope: str = "detail") -> None:
    """Render Health Canada-specific action buttons."""
    export_df = context_df.copy() if context_df is not None and not context_df.empty else pd.DataFrame([record.to_dict()])
    export_columns = [
        "event_id",
        "date_received",
        "event_date",
        "product_name",
        "generic_name",
        "manufacturer",
        "event_type",
        "device_problem_text",
        "patient_outcome",
        "product_code",
        "event_link",
    ]
    export_columns = [column for column in export_columns if column in export_df.columns]
    csv_filename = f"health_canada_mdi_filtered_{date.today().isoformat()}.csv"
    csv_payload = spreadsheet_safe_frame(export_df[export_columns]).to_csv(index=False).encode("utf-8-sig")
    record_id = slugify(str(record.get("event_id") or record.get("report_number") or "selected"))
    button_key = f"download_health_canada_csv_{scope}_{record_id}_{len(export_df)}"
    st.download_button(
        "Download Health Canada CSV",
        data=csv_payload,
        file_name=csv_filename,
        mime="text/csv",
        use_container_width=True,
        key=button_key,
    )


def render_json_section(title: str, value: object) -> None:
    if value in (None, ""):
        return
    with st.expander(title, expanded=False):
        try:
            parsed = json.loads(value) if isinstance(value, str) else value
            st.json(parsed)
        except Exception:
            st.code(str(value))


def render_record_count_audit(results: dict[str, pd.DataFrame]) -> None:
    """Render source count audit tables."""
    all_raw = results.get("all_raw", pd.DataFrame())
    filtered = results.get("filtered", pd.DataFrame())
    rows = []
    source_ids = sorted(set(all_raw.get("source", pd.Series(dtype=str)).dropna().astype(str)) | set(filtered.get("source", pd.Series(dtype=str)).dropna().astype(str)))
    for source_id in source_ids:
        raw_count = int((all_raw.get("source", pd.Series(dtype=str)).astype(str) == source_id).sum()) if not all_raw.empty else 0
        filtered_count = int((filtered.get("source", pd.Series(dtype=str)).astype(str) == source_id).sum()) if not filtered.empty else 0
        rows.append({"source": source_id, "raw_records": raw_count, "filtered_records": filtered_count, "duplicates_or_filtered_out": raw_count - filtered_count})
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    warnings = results.get("source_warnings", pd.DataFrame())
    audit_messages, warnings = split_source_audit_messages(warnings)
    if not audit_messages.empty:
        st.subheader("Connector count messages")
        st.dataframe(audit_messages, use_container_width=True, hide_index=True)
    errors = results.get("source_errors", pd.DataFrame())
    if not warnings.empty:
        st.subheader("Source warnings")
        st.dataframe(warnings, use_container_width=True, hide_index=True)
    if not errors.empty:
        st.subheader("Source errors")
        st.dataframe(errors, use_container_width=True, hide_index=True)


def evidence_archive(files: tuple[Path, ...]) -> bytes:
    """Package the original export bytes using the manifest's file names."""
    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        for file in files:
            archive.write(file, arcname=file.name)
    return buffer.getvalue()


def render_evidence_download(export_files: dict[str, Path]) -> None:
    manifest = export_files.get("manifest")
    paths = tuple(Path(file) for file in export_files.values())
    if manifest is None or not all(file.is_file() for file in paths):
        st.warning("The evidence package is unavailable. Run the search again to create a new export.")
        return
    run_id = Path(manifest).parent.name
    st.caption(f"Evidence run ID {run_id}")
    st.download_button(
        "Download evidence package (ZIP)", data=lambda: evidence_archive(paths),
        file_name=f"incident_evidence_{run_id}.zip", mime="application/zip", on_click="ignore",
        help="Includes Excel, CSV, raw and filtered JSONL, the search scope, and the evidence manifest.",
    )


def render_debug_raw_data(results: dict[str, pd.DataFrame]) -> None:
    """Render full exports and a preview of the underlying tables."""
    prefix = st.session_state.get("export_prefix", f"medical_device_incident_report_{date.today().isoformat()}")
    export_files = results.get("export_files", {})
    for key, label, mime in [("excel", "Download Excel report", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"), ("manifest", "Download evidence manifest", "application/json"), ("raw_jsonl", "Download complete raw records", "application/x-ndjson"), ("filtered_jsonl", "Download complete filtered records", "application/x-ndjson")]:
        file = export_files.get(key)
        if file is not None and Path(file).is_file():
            path = Path(file)
            st.download_button(label, data=path.read_bytes, file_name=path.name, mime=mime, key=f"artifact_{key}", on_click="ignore")
    for key in ["all_raw", "filtered", "summary_year_category", "summary_source_category", "source_status", "source_warnings", "source_errors"]:
        df = results.get(key, pd.DataFrame())
        label = key.replace("_", " ").capitalize()
        st.subheader(label)
        st.caption(f"{len(df):,} rows")
        file = export_files.get(key)
        path = Path(file) if file is not None else None
        st.download_button(
            f"Download {label.lower()} CSV",
            data=path.read_bytes if path is not None and path.is_file() else spreadsheet_safe_frame(df).to_csv(index=False).encode("utf-8-sig"),
            file_name=path.name if path is not None else f"{prefix}_{key}.csv",
            mime="text/csv",
            use_container_width=True,
            key=f"download_debug_{key}",
            on_click="ignore",
        )
        st.dataframe(df.head(100), use_container_width=True, hide_index=True)


def split_source_audit_messages(warnings: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Separate normal connector count/debug messages from actionable warnings."""
    if warnings.empty or "warning_message" not in warnings.columns:
        return pd.DataFrame(), warnings
    audit_pattern = re.compile(
        r"^(device_search keyword=.* matches=\d+|selected_devices_count=\d+|"
        r"expected_report_count=\d+|print_report_url=.*|content_type=.*|"
        r"pdf_text_length=\d+|max_pages=\d+|"
        r"health_canada_mdi_raw_records=\d+|health_canada_mdi_filtered_records=\d+|"
        r"eudamed_records=\d+|swissmedic_fsca_records=\d+|mhra_fsca_records=\d+|"
        r"bfarm_records=\d+)$"
    )
    messages = warnings["warning_message"].fillna("").astype(str)
    audit_mask = messages.str.match(audit_pattern)
    return warnings[audit_mask].copy(), warnings[~audit_mask].copy()


def count_rows_with_links(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    event_links = df.get("event_link", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
    device_links = df.get("device_link", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
    raw_links = df.get("raw_link", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
    return int(((event_links != "") | (device_links != "") | (raw_links != "")).sum())


def link_column_config(df: pd.DataFrame) -> dict[str, object]:
    config: dict[str, object] = {}
    for column, label in FIELD_LABELS.items():
        if column in df.columns:
            config[column] = st.column_config.TextColumn(label)
    if "event_link" in df.columns:
        config["event_link"] = st.column_config.LinkColumn(FIELD_LABELS["event_link"])
    if "device_link" in df.columns:
        config["device_link"] = st.column_config.LinkColumn(FIELD_LABELS["device_link"])
    if "raw_link" in df.columns:
        config["raw_link"] = st.column_config.LinkColumn(FIELD_LABELS["raw_link"])
    return config


def parse_keyword_text(text: str) -> list[str]:
    """Split comma/newline-separated keyword text into unique non-empty terms."""
    terms = re.split(r"[,\n]+", text)
    cleaned = []
    seen = set()
    for term in terms:
        value = term.strip()
        key = value.lower()
        if value and key not in seen:
            cleaned.append(value)
            seen.add(key)
    return cleaned


def merge_unique_terms(*groups: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for term in group:
            key = term.strip().lower()
            if key and key not in seen:
                merged.append(term.strip())
                seen.add(key)
    return merged


def slugify(value: str) -> str:
    """Convert a device name into a lowercase filename-safe slug."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower()).strip("_")
    return slug or "medical_device"


def format_record_label(index: int, row: pd.Series) -> str:
    """Build a compact record label for the narrative selector."""
    event_id = row.get("event_id") or "no-id"
    category = row.get("category") or "Uncategorized"
    source = row.get("source") or ""
    display_date = row.get("report_date") if source in {"FDA_MAUDE", "TGA_DAEN"} else row.get("event_date")
    display_date = display_date or row.get("event_date") or "no-date"
    return f"{index + 1}. {display_date} | {event_id} | {category}"


def save_uploaded_file(uploaded_file: object | None) -> str | None:
    """Persist an uploaded file to a temporary path for connector consumption."""
    if uploaded_file is None:
        return None
    suffix = Path(uploaded_file.name).suffix or ".csv"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
        temp_file.write(uploaded_file.getvalue())
        return temp_file.name


if __name__ == "__main__":
    main()
