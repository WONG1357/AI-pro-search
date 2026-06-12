# Medical Device Incident Search Platform

Python project for searching, normalizing, classifying, and exporting FDA MAUDE adverse event records related to medical devices.

Supported sources:

- FDA MAUDE 
- TGA DAEN 
- Health Canada MDI 
- Swissmedic FSCA medical device recall publications
- UK MHRA field safety notices 
- BfArM medical device recall / manufacturer action notices

## Setup

```bash
cd trocar_incident_platform
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Set `OPENFDA_API_KEY` in `.env` or your shell if you have an openFDA API key. The pipeline also works without a key, subject to openFDA rate limits.

```bash
export OPENFDA_API_KEY="your_key_here"
```

## Run

```bash
python -m pipeline.pipeline
```

Optional arguments:

```bash
python -m pipeline.pipeline --keywords Xcel Versaport VersaOne Kii "Apple Trocar" "Lina Port" Trocar leak fixation puncture death injury infection blade "pyramidal tip" --years 2024 2025 2026
```

Exports are written to `data/exports/`:

- `all_raw_records.csv`
- `filtered_classified_records.csv`
- `summary_year_category.csv`
- `summary_source_category.csv`
- `incident_report.xlsx`

## Tests

```bash
pytest
```

## Structure

- `pipeline/config.py`: shared settings, paths, unified columns, environment-backed API key.
- `pipeline/utils.py`: text normalization, date parsing, keyword matching.
- `pipeline/classifier.py`: existing rule-based incident classifier.
- `pipeline/sources_openfda.py`: FDA MAUDE openFDA fetch and normalization.
- `pipeline/sources/tga_daen.py`: TGA DAEN direct connector, print-report parser, and optional CSV fallback loader.
- `pipeline/sources/health_canada_mdi.py`: Health Canada MDI direct connector using the public DataTables endpoint.
- `pipeline/sources/swissmedic.py`: Swissmedic FSCA direct recall connector.
- `pipeline/sources/mhra_fsca.py`: UK MHRA field safety notice connector.
- `pipeline/sources/bfarm.py`: BfArM medical device recall connector.
- `pipeline/export.py`: CSV and Excel report writing.
- `pipeline/pipeline.py`: orchestration and CLI entrypoint.
- `app.py`: Streamlit dashboard with source-specific result tabs, detail renderers, count audit, and raw/debug downloads.

## Streamlit dashboard

Run the local dashboard with:

```bash
streamlit run app.py
```

The app keeps a minimal unified schema for cross-source analytics, but it does not force every source into the same visible table. `Source Results` shows one tab per source with source-specific columns, filters, metrics, and record detail rendering. Source-specific payloads are retained in `source_specific` and `raw_record`, and can be inspected or downloaded from the debug views.

Some records do not provide a direct incident URL. In those cases the detail view shows `No direct incident link available for this record.`

## Health Canada MDI data source

Health Canada MDI is accessed directly online through the Medical Device Incidents public search infrastructure:

- Results page: `https://hpr-rps.hres.ca/mdi_results.php`
- Data endpoint: `https://hpr-rps.hres.ca/mdi_dep/serverSideProcessing.php`

The connector submits one OR-style keyword search at a time, pages through DataTables JSON results, deduplicates by Health Canada incident ID, filters by selected years, normalizes key fields into the shared schema, and preserves the native incident JSON in `raw_record`.

Health Canada-specific values such as `hazard_severity_code_e`, `mandatory_rt`, `device_detail`, `company_detail`, and `problem_detail` are stored in `source_specific`.

## TGA DAEN data source

TGA DAEN is accessed directly online. The connector mirrors DAEN's multi-step browser flow:

1. Search medical-device names.
2. Select matching device identifiers.
3. Submit selected devices with a date range.
4. Open the list/print version of the report.
5. Parse adverse event report rows from HTML, text, or PDF output.

The selected device list is treated as search metadata only. Final records must come from the detailed report log with report numbers, dates, trade names, model/reference values, event descriptions, and outcomes. Manual CSV import remains available only under the Streamlit advanced fallback/debug section.

DAEN excludes recent reports. The app caps the default end date using DAEN's visible rule: first Thursday of the current month minus three months. You can override the TGA date range in the sidebar.

Endpoint settings can be configured with:

- `TGA_DAEN_BASE_URL`
- `TGA_DAEN_DEVICE_SEARCH_ENDPOINT`
- `TGA_DAEN_REPORT_SEARCH_ENDPOINT`
- `TGA_DAEN_TIMEOUT`
- `TGA_DAEN_PAGE_SIZE`
- `TGA_DAEN_USER_AGENT`
- `TGA_DAEN_DEBUG=1`

If the endpoint changes, enable debug logs in the Streamlit advanced settings or set `TGA_DAEN_DEBUG=1` to inspect request URLs, status codes, response snippets, selected device counts, and report page progress.

## Progress tracking and ETA

Searches report live progress for every selected source. The dashboard shows current source, stage, records fetched, elapsed time, ETA, source status, and recent logs.

Percent complete is exact when a connector knows total steps. For sources with unknown pagination, elapsed time and records fetched are shown while ETA remains unavailable. Overall progress gives each selected source equal weight.

Advanced settings in the sidebar include request timeout, max pages per source, max records per source, and debug logs. These limits are especially useful for TGA DAEN to avoid long-running searches.
