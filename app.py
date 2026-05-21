"""Streamlit dashboard for the medical device incident search platform."""

from __future__ import annotations

import re
import tempfile
from datetime import date
import json
from pathlib import Path

import pandas as pd
import streamlit as st

from pipeline.config import EXPORTS_DIR
from pipeline.pipeline import run_pipeline
from pipeline.progress import PipelineProgress, ProgressReporter, format_seconds
from pipeline.sources import DEFAULT_SELECTED_SOURCES, SOURCE_DISPLAY_NAMES
from pipeline.sources.tga_daen import build_tga_date_range


PAGE_TITLE = "Medical Device Incident Search Platform"
APP_STATE_VERSION = "2026-05-21-no-record-limit-v1"
DEFAULT_TROCAR_KEYWORDS = "Xcel, Versaport, VersaOne, Kii, Apple Trocar, Lina Port, Trocar, leak, fixation, puncture, death, injury, infection, blade, pyramidal tip"
YEAR_OPTIONS = list(range(2020, 2027))
DEFAULT_YEARS = [2024, 2025, 2026]
DEFAULT_REQUEST_TIMEOUT = 60
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


def main() -> None:
    """Render the dashboard and coordinate user-triggered pipeline runs."""
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    reset_stale_session_state()
    st.title(PAGE_TITLE)

    sidebar_state = render_sidebar()
    st.subheader(f"Current search: {sidebar_state['device_name']}")

    if sidebar_state["run_search"]:
        execute_search(sidebar_state)

    results = st.session_state.get("results")
    if not results:
        st.info("Set your search options in the sidebar, then click Run Search.")
        return

    render_dashboard(results)


def reset_stale_session_state() -> None:
    """Clear old in-memory results after source connector/layout changes."""
    if st.session_state.get("app_state_version") == APP_STATE_VERSION:
        return
    st.session_state["app_state_version"] = APP_STATE_VERSION
    for key in ["results", "device_name", "export_prefix"]:
        st.session_state.pop(key, None)


def render_sidebar() -> dict[str, object]:
    """Render search controls and return normalized sidebar state."""
    with st.sidebar:
        st.header("Search")
        device_profile = st.selectbox("Device profile", ["Trocar", "Custom"])

        if device_profile == "Trocar":
            device_name = "Trocar"
            keyword_text = st.text_input("Keywords", value=DEFAULT_TROCAR_KEYWORDS)
        else:
            device_name = st.text_input("Device name", value="").strip() or "Custom device"
            keyword_text = st.text_area("Keywords", value="")
            components = st.text_area("Components", value="")
            accident_terms = st.text_area("Accident terms", value="")
            keyword_text = ", ".join([keyword_text, components, accident_terms])

        years = st.multiselect("Years", YEAR_OPTIONS, default=DEFAULT_YEARS)
        st.subheader("Sources")
        selected_sources = []
        for source_id, display_name in SOURCE_DISPLAY_NAMES.items():
            default_value = source_id in DEFAULT_SELECTED_SOURCES
            if st.checkbox(display_name, value=default_value, key=f"source_{source_id}"):
                selected_sources.append(source_id)
        tga_csv_file = None
        tga_start_date = None
        tga_end_date = None
        if "TGA_DAEN" in selected_sources:
            default_start_text, default_end_text = build_tga_date_range(years or DEFAULT_YEARS)
            default_start = date.fromisoformat(default_start_text)
            default_end = date.fromisoformat(default_end_text)
            st.caption("TGA DAEN date range")
            tga_start_date = st.date_input("From date", value=default_start, key="tga_start_date")
            tga_end_date = st.date_input("To date", value=default_end, key="tga_end_date")
            with st.expander("Advanced / fallback import", expanded=False):
                st.caption("Optional manual CSV import if direct access needs debugging.")
                tga_csv_file = st.file_uploader("TGA DAEN CSV", type=["csv"])
        with st.expander("Advanced settings", expanded=False):
            request_timeout = st.number_input("Request timeout (sec)", min_value=5, max_value=600, value=DEFAULT_REQUEST_TIMEOUT)
            max_pages_input = st.number_input("Max pages per source (0 = no limit)", min_value=0, max_value=5000, value=0)
            debug_logs = st.checkbox("Enable debug logs", value=False)
        run_search = st.button("Run Search", type="primary", use_container_width=True)

    keywords = parse_keyword_text(keyword_text)
    return {
        "device_name": device_name,
        "keywords": keywords,
        "years": years,
        "selected_sources": selected_sources,
        "tga_csv_file": tga_csv_file,
        "tga_start_date": tga_start_date,
        "tga_end_date": tga_end_date,
        "request_timeout": request_timeout,
        "max_pages": int(max_pages_input) if max_pages_input else None,
        "debug_logs": debug_logs,
        "run_search": run_search,
    }


def execute_search(search_state: dict[str, object]) -> None:
    """Run the pipeline and save result dataframes plus export paths in session state."""
    keywords = search_state["keywords"]
    years = search_state["years"]

    if not keywords:
        st.error("Enter at least one keyword, component, or accident term.")
        return
    if not years:
        st.error("Select at least one year.")
        return
    if not search_state["selected_sources"]:
        st.error("Select at least one data source.")
        return

    device_slug = slugify(str(search_state["device_name"]))
    export_prefix = f"{device_slug}_incident_report_{date.today().isoformat()}"
    source_options: dict[str, dict[str, object]] = {}
    if search_state.get("tga_csv_file") is not None:
        source_options["TGA_DAEN"] = {"csv_path": save_uploaded_file(search_state["tga_csv_file"])}
    elif "TGA_DAEN" in search_state["selected_sources"]:
        source_options["TGA_DAEN"] = {
            "start_date": search_state.get("tga_start_date"),
            "end_date": search_state.get("tga_end_date"),
        }
    source_options["_global"] = {
        "request_timeout": search_state.get("request_timeout"),
        "max_pages": search_state.get("max_pages"),
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
                years=years,
                output_dir=EXPORTS_DIR,
                export_results=True,
                export_file_prefix=export_prefix,
                selected_sources=list(search_state["selected_sources"]),
                source_options=source_options,
                progress_reporter=reporter,
            )
    except Exception as exc:
        st.session_state.pop("results", None)
        st.error(f"Search failed: {exc}")
        return

    st.session_state["results"] = results
    st.session_state["device_name"] = search_state["device_name"]
    st.session_state["export_prefix"] = export_prefix
    st.success("Search completed.")


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
    render_source_messages(results)
    filtered = results.get("filtered", pd.DataFrame())
    all_raw = results.get("all_raw", pd.DataFrame())
    if filtered.empty and all_raw.empty:
        st.warning("No matching records were found for this search.")
        return

    overview_tab, source_tab, detail_tab, audit_tab, debug_tab = st.tabs(
        ["Overview", "Source Results", "Record Detail", "Record Count Audit", "Debug / Raw Data"]
    )
    with overview_tab:
        render_overview(filtered, all_raw)
    with source_tab:
        render_source_results(filtered)
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
    col_total, col_raw, col_sources = st.columns(3)
    col_total.metric("Filtered records", f"{len(filtered):,}")
    col_raw.metric("Raw records", f"{len(all_raw):,}")
    col_sources.metric("Sources with records", f"{filtered['source'].nunique() if 'source' in filtered.columns and not filtered.empty else 0:,}")

    col_source, col_year, col_category = st.columns(3)
    with col_source:
        st.caption("Record count by source")
        st.dataframe(count_table(filtered, "source"), use_container_width=True, hide_index=True)
    with col_year:
        st.caption("Record count by year")
        st.dataframe(count_table(filtered, "year"), use_container_width=True, hide_index=True)
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


def render_source_results(filtered: pd.DataFrame) -> None:
    """Render one results tab per source."""
    source_order = ["FDA_MAUDE", "TGA_DAEN", "HEALTH_CANADA_MDI", "SWISSMEDIC_FSCA", "MHRA_FSCA", "BFARM_RECALLS"]
    tabs = st.tabs([SOURCE_DISPLAY_NAMES.get(source, source) for source in source_order])
    for source_id, tab in zip(source_order, tabs):
        with tab:
            source_df = filtered[filtered.get("source", pd.Series(dtype=str)).astype(str) == source_id].copy() if not filtered.empty else pd.DataFrame()
            render_source_result_tab(source_id, source_df)


def render_source_result_tab(source_id: str, df: pd.DataFrame) -> None:
    """Render source-specific metrics, filters, table, and selector."""
    display_name = SOURCE_DISPLAY_NAMES.get(source_id, source_id)
    st.subheader(display_name)
    if df.empty:
        st.info(f"No {display_name} records are available for this search.")
        return

    metric_cols = st.columns(4)
    metric_cols[0].metric("Records", f"{len(df):,}")
    metric_cols[1].metric("Years", f"{df['year'].nunique() if 'year' in df.columns else 0:,}")
    metric_cols[2].metric("Categories", f"{df['category'].nunique() if 'category' in df.columns else 0:,}")
    metric_cols[3].metric("With links", f"{count_rows_with_links(df):,}")

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
    render_source_detail(source_id, selected)


def apply_source_filters(source_id: str, df: pd.DataFrame) -> pd.DataFrame:
    """Apply source-specific filters."""
    filtered = df.copy()
    columns = st.columns(4)
    with columns[0]:
        years = sorted([int(year) for year in pd.to_numeric(filtered.get("year"), errors="coerce").dropna().unique()]) if "year" in filtered else []
        selected_years = st.multiselect("Year", years, default=years, key=f"filter_year_{source_id}")
    with columns[1]:
        category_values = sorted(filtered.get("category", pd.Series(dtype=str)).dropna().astype(str).unique())
        selected_categories = st.multiselect("Category", category_values, default=category_values, key=f"filter_category_{source_id}")
    with columns[2]:
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
        values = sorted(filtered.get(field, pd.Series(dtype=str)).fillna("").astype(str).unique())[:200]
        selected_values = st.multiselect(label, values, default=values, key=f"filter_field_{source_id}")
    with columns[3]:
        query = st.text_input("Text search", key=f"filter_text_{source_id}")

    if selected_years and "year" in filtered.columns:
        filtered = filtered[filtered["year"].isin(selected_years)]
    if selected_categories and "category" in filtered.columns:
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


def source_table_columns(source_id: str, df: pd.DataFrame) -> list[str]:
    """Return source-specific display columns present in the dataframe."""
    mapping = {
        "FDA_MAUDE": ["event_id", "report_number", "event_date", "year", "product_name", "brand_names", "manufacturer", "event_type", "category", "event_link"],
        "TGA_DAEN": ["report_number", "event_date", "year", "product_name", "manufacturer", "generic_name", "event_type", "category", "event_link"],
        "HEALTH_CANADA_MDI": ["event_id", "date_received", "year", "product_name", "generic_name", "manufacturer", "event_type", "device_problem_text", "patient_outcome", "category"],
        "SWISSMEDIC_FSCA": ["event_id", "event_date", "year", "manufacturer", "product_name", "generic_name", "device_model", "event_type", "device_problem_text", "category", "event_link"],
        "MHRA_FSCA": ["event_id", "event_date", "year", "manufacturer", "product_name", "device_model", "event_type", "device_problem_text", "category", "event_link"],
        "BFARM_RECALLS": ["event_id", "event_date", "year", "manufacturer", "product_name", "event_type", "device_problem_text", "category", "event_link"],
    }
    preferred = mapping.get(source_id, PRIMARY_COLUMNS)
    columns = [column for column in preferred if column in df.columns]
    extras = [column for column in ["source_specific", "raw_record"] if column in df.columns and column not in columns]
    return columns + extras


def render_record_detail_page(filtered: pd.DataFrame) -> None:
    """Render source-specific detail view for any selected record."""
    if filtered.empty:
        st.info("No records are available for detail review.")
        return
    sources = sorted(filtered["source"].dropna().astype(str).unique())
    selected_source = st.selectbox("Source", sources, key="detail_source")
    source_df = filtered[filtered["source"].astype(str) == selected_source].reset_index(drop=True)
    labels = [format_record_label(index, row) for index, row in source_df.iterrows()]
    selected_label = st.selectbox("Record", labels, key="detail_record")
    selected = source_df.iloc[labels.index(selected_label)]
    render_source_detail(selected_source, selected)


def render_source_detail(source_id: str, record: pd.Series) -> None:
    """Dispatch to a source-specific detail renderer."""
    renderers = {
        "FDA_MAUDE": render_fda_maude_detail,
        "TGA_DAEN": render_tga_daen_detail,
        "HEALTH_CANADA_MDI": render_health_canada_mdi_detail,
        "SWISSMEDIC_FSCA": render_swissmedic_fsca_detail,
        "MHRA_FSCA": render_mhra_fsca_detail,
        "BFARM_RECALLS": render_bfarm_recall_detail,
    }
    renderers.get(source_id, render_generic_detail)(record)


def render_fda_maude_detail(record: pd.Series) -> None:
    render_detail_fields(record, ["event_id", "report_number", "event_date", "product_name", "brand_names", "manufacturer", "event_type", "patient_outcome", "device_problem_text", "patient_problem_text", "category"])


def render_tga_daen_detail(record: pd.Series) -> None:
    render_detail_fields(record, ["report_number", "event_date", "product_name", "manufacturer", "generic_name", "product_code", "event_type", "category"])


def render_health_canada_mdi_detail(record: pd.Series) -> None:
    render_detail_fields(record, ["event_id", "date_received", "product_name", "generic_name", "manufacturer", "event_type", "device_problem_text", "patient_outcome", "product_code", "category"])
    render_json_section("Health Canada-specific fields", record.get("source_specific"))


def render_swissmedic_fsca_detail(record: pd.Series) -> None:
    render_detail_fields(
        record,
        ["event_id", "event_date", "manufacturer", "product_name", "generic_name", "device_model", "event_type", "device_problem_text", "source_category_name", "category", "event_link"],
        missing_link_message="No direct recall document link available for this record.",
    )
    render_json_section("Swissmedic-specific fields", record.get("source_specific"))


def render_mhra_fsca_detail(record: pd.Series) -> None:
    render_detail_fields(
        record,
        ["event_id", "event_date", "manufacturer", "product_name", "device_model", "event_type", "device_problem_text", "source_category_name", "category", "event_link", "raw_link"],
        missing_link_message="No direct field safety notice link available for this record.",
    )
    render_json_section("MHRA-specific fields", record.get("source_specific"))


def render_bfarm_recall_detail(record: pd.Series) -> None:
    render_detail_fields(
        record,
        ["event_id", "event_date", "manufacturer", "product_name", "event_type", "device_problem_text", "source_category_name", "category", "event_link"],
        missing_link_message="No direct BfArM recall link available for this record.",
    )
    render_json_section("BfArM-specific fields", record.get("source_specific"))


def render_generic_detail(record: pd.Series) -> None:
    render_detail_fields(record, ["source", "event_id", "event_date", "product_name", "manufacturer", "category", "event_link"])


def render_detail_fields(record: pd.Series, fields: list[str], missing_link_message: str = "No direct incident link available for this record.") -> None:
    """Render a compact details section with graceful missing-link handling."""
    rows = []
    for field in fields:
        value = record.get(field, "")
        if value not in (None, ""):
            rows.append({"field": field, "value": value})
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    st.subheader("Narrative / description")
    st.write(record.get("narrative_text") or "No narrative text available.")
    link = record.get("event_link") or record.get("device_link") or record.get("raw_link")
    if link and str(link).strip():
        st.link_button("Open source record", str(link))
    else:
        st.info(missing_link_message)
    render_json_section("Raw record", record.get("raw_record"))


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


def render_debug_raw_data(results: dict[str, pd.DataFrame]) -> None:
    """Render debug and raw data downloads."""
    prefix = st.session_state.get("export_prefix", f"medical_device_incident_report_{date.today().isoformat()}")
    for key in ["all_raw", "filtered", "summary_year_category", "summary_source_category", "source_warnings", "source_errors"]:
        df = results.get(key, pd.DataFrame())
        st.subheader(key)
        st.caption(f"{len(df):,} rows")
        st.download_button(
            f"Download {key} CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name=f"{prefix}_{key}.csv",
            mime="text/csv",
            use_container_width=True,
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
    if "event_link" in df.columns:
        config["event_link"] = st.column_config.LinkColumn("event_link")
    if "device_link" in df.columns:
        config["device_link"] = st.column_config.LinkColumn("device_link")
    if "raw_link" in df.columns:
        config["raw_link"] = st.column_config.LinkColumn("raw_link")
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


def slugify(value: str) -> str:
    """Convert a device name into a lowercase filename-safe slug."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower()).strip("_")
    return slug or "medical_device"


def format_record_label(index: int, row: pd.Series) -> str:
    """Build a compact record label for the narrative selector."""
    event_id = row.get("event_id") or "no-id"
    category = row.get("category") or "Uncategorized"
    event_date = row.get("event_date") or "no-date"
    return f"{index + 1}. {event_date} | {event_id} | {category}"


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
