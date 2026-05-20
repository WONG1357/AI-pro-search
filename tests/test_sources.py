import pandas as pd
import requests

from pipeline.config import UNIFIED_COLUMNS
from pipeline.sources import SOURCE_REGISTRY
from pipeline.sources.base import ensure_unified_columns
from pipeline.sources.bfarm import (
    BfarmClientConfig,
    build_bfarm_search_payload,
    build_bfarm_year_facets,
    extract_bfarm_period_links,
    fetch_bfarm_recalls_direct,
    normalize_bfarm_record,
    parse_bfarm_results,
)
from pipeline.sources.health_canada_mdi import (
    HealthCanadaMdiClientConfig,
    build_health_canada_mdi_payload,
    fetch_health_canada_mdi_direct,
    normalize_health_canada_mdi_record,
    parse_health_canada_mdi_response,
)
from pipeline.sources.mhra_fsca import (
    build_mhra_search_params,
    normalize_mhra_fsca_record,
    parse_mhra_notice_page,
    parse_mhra_search_results,
)
from pipeline.sources.swissmedic import (
    SwissmedicFscaClientConfig,
    build_swissmedic_payload,
    fetch_swissmedic_fsca_direct,
    normalize_swissmedic_record,
    parse_swissmedic_response,
)
from pipeline.sources.tga_daen import (
    TgaDaenClientConfig,
    TgaDaenConnector,
    build_tga_daen_query,
    build_tga_date_range,
    fetch_tga_daen_direct,
    fetch_tga_reports_for_devices,
    fetch_tga_print_report,
    find_tga_list_of_reports_url,
    find_tga_print_version_url,
    load_tga_daen_csv,
    normalize_tga_daen_report,
    normalize_tga_daen_record,
    parse_tga_print_report_html,
    parse_tga_print_report_text,
    parse_tga_search_result_counts,
    parse_tga_daen_response,
    extract_tga_device_options,
)


def test_ensure_unified_columns_adds_missing_columns() -> None:
    df = ensure_unified_columns(__import__("pandas").DataFrame({"source": ["FDA_MAUDE"]}))
    for column in UNIFIED_COLUMNS:
        assert column in df.columns


def test_source_registry_contains_expected_sources() -> None:
    for source_id in ["FDA_MAUDE", "TGA_DAEN", "HEALTH_CANADA_MDI", "SWISSMEDIC_FSCA", "MHRA_FSCA", "BFARM_RECALLS"]:
        assert source_id in SOURCE_REGISTRY


def sample_swissmedic_raw() -> dict:
    return {
        "publikationsDatum": "2024-03-05",
        "swissmedicRef": "Vk_20240304_29",
        "hersteller": "Covidien llc",
        "status": "FIRST",
        "statusDatum": "2024-03-05",
        "begruendung": "Field safety corrective action",
        "devices": [
            {
                "handelsname": "Auto Suture Structural Balloon Trocar",
                "sn": None,
                "lot": "all",
                "swVersion": None,
                "model": "OMS-T10SB",
                "beschreibungKlasse": "MD: Laparoscopic multi-instrument access port, single-use",
            }
        ],
        "freigeschaltet": True,
        "documents": [
            {"title": "DE-1", "language": "DE", "version": "1"},
            {"title": "EN-1", "language": "EN", "version": "1"},
        ],
    }


def test_swissmedic_payload_builder() -> None:
    payload = build_swissmedic_payload("trocar", "2024-01-01", "2026-05-20")
    assert payload == {
        "queryTerm": "trocar",
        "fromDate": "2024-01-01",
        "toDate": "2026-05-20",
        "onlyUpdates": False,
    }


def test_swissmedic_response_parser() -> None:
    parsed = parse_swissmedic_response(
        {"content": [sample_swissmedic_raw()], "totalElements": 1, "totalPages": 1, "number": 0, "last": True}
    )
    assert parsed["total_elements"] == 1
    assert parsed["total_pages"] == 1
    assert parsed["content"][0]["swissmedicRef"] == "Vk_20240304_29"


def test_swissmedic_normalization_maps_recall_fields() -> None:
    normalized = normalize_swissmedic_record(sample_swissmedic_raw(), SwissmedicFscaClientConfig(base_url="https://example.test/mep"))
    assert normalized["source"] == "SWISSMEDIC_FSCA"
    assert normalized["source_type"] == "regulatory_recall"
    assert normalized["event_id"] == "Vk_20240304_29"
    assert normalized["event_date"] == "2024-03-05"
    assert normalized["manufacturer"] == "Covidien llc"
    assert normalized["product_name"] == "Auto Suture Structural Balloon Trocar"
    assert normalized["device_model"] == "OMS-T10SB"
    assert normalized["event_type"] == "FIRST"
    assert normalized["event_link"] == "https://example.test/mep/api/publications/Vk_20240304_29/documents/1"


def test_swissmedic_direct_fetch_success_with_mock_http() -> None:
    session = FakeTgaSession(
        [
            make_response(
                200,
                b'{"content":[{"publikationsDatum":"2024-03-05","swissmedicRef":"Vk_20240304_29","hersteller":"Covidien llc","status":"FIRST","statusDatum":"2024-03-05","begruendung":null,"devices":[{"handelsname":"Auto Suture Structural Balloon Trocar","model":"OMS-T10SB","beschreibungKlasse":"MD: Laparoscopic multi-instrument access port, single-use"}],"freigeschaltet":true,"documents":[{"title":"DE-1","language":"DE","version":"1"}]}],"totalElements":1,"totalPages":1,"number":0,"last":true}',
            )
        ]
    )
    config = SwissmedicFscaClientConfig(base_url="https://example.test/mep", search_endpoint="/api/publications/search")
    df, warnings = fetch_swissmedic_fsca_direct(["trocar"], [2024], client_config=config, session=session)
    assert len(df) == 1
    assert df.iloc[0]["source"] == "SWISSMEDIC_FSCA"
    assert session.calls[0]["json"]["queryTerm"] == "trocar"
    assert warnings


def test_swissmedic_repeated_page_warning_is_scoped_per_keyword() -> None:
    session = FakeTgaSession(
        [
            make_response(200, b'{"content":[{"publikationsDatum":"2024-03-05","swissmedicRef":"A1","hersteller":"X","status":"FIRST","devices":[],"documents":[]}],"totalElements":1,"totalPages":1,"number":0,"last":true}'),
            make_response(200, b'{"content":[{"publikationsDatum":"2024-03-05","swissmedicRef":"A1","hersteller":"X","status":"FIRST","devices":[],"documents":[]}],"totalElements":1,"totalPages":1,"number":0,"last":true}'),
        ]
    )
    config = SwissmedicFscaClientConfig(base_url="https://example.test/mep", search_endpoint="/api/publications/search")
    df, warnings = fetch_swissmedic_fsca_direct(["trocar", "kii"], [2024], client_config=config, session=session)
    assert len(df) == 1
    assert warnings


def test_mhra_search_params() -> None:
    assert build_mhra_search_params("trocar", page=2) == {
        "alert_type": "field-safety-notices",
        "keywords": "trocar",
        "page": "2",
    }


def test_mhra_search_results_parser() -> None:
    html = """
    <ul><li class="gem-c-document-list__item">
      <a href="/drug-device-alerts/field-safety-notices-4-to-8-march-2024">Field Safety Notices: 4 to 8 March 2024</a>
      <p>Alert type: Field safety notice Issued: 11 March 2024</p>
    </li></ul>
    """
    parsed = parse_mhra_search_results(html, "https://www.gov.uk")
    assert parsed[0]["title"] == "Field Safety Notices: 4 to 8 March 2024"
    assert parsed[0]["url"] == "https://www.gov.uk/drug-device-alerts/field-safety-notices-4-to-8-march-2024"


def test_mhra_notice_page_parser_and_normalization() -> None:
    html = """
    <html><body>
    <h1>Field Safety Notices: 4 to 8 March 2024</h1>
    <main>
      <div class="gem-c-govspeak">
        <h2>Covidien: Auto Suture Structural Balloon & Blunt Tip Trocar</h2>
        <p>05 March 2024</p>
        <p>MD: Laparoscopic multi-instrument access port, single-use</p>
        <p>Model: OMS-T10SB</p>
        <p>MHRA reference: <a href="https://mhra-gov.filecamp.com/s/d/example">28050499</a></p>
      </div>
    </main>
    </body></html>
    """
    raw = parse_mhra_notice_page(html, "https://www.gov.uk/drug-device-alerts/test", keyword="trocar")[0]
    normalized = normalize_mhra_fsca_record(raw)
    assert normalized["source"] == "MHRA_FSCA"
    assert normalized["source_type"] == "regulatory_recall"
    assert normalized["manufacturer"] == "Covidien"
    assert "Trocar" in normalized["product_name"]
    assert normalized["device_model"] == "OMS-T10SB"
    assert normalized["event_link"] == "https://mhra-gov.filecamp.com/s/d/example"


def test_mhra_partial_date_is_preserved() -> None:
    html = """
    <html><body>
    <div class="gem-c-govspeak">
        <h2>Example Manufacturer: Example Device</h2>
        <p>Issued: March 2024</p>
        <p>Model: ABC-123</p>
    </div>
    </body></html>
    """
    raw = parse_mhra_notice_page(html, "https://www.gov.uk/drug-device-alerts/test", keyword="example")[0]
    normalized = normalize_mhra_fsca_record(raw)
    assert normalized["event_date"] == "March 2024"


def test_bfarm_parser_and_normalization() -> None:
    html = """
    <html><body>
      <section id="results">
        <li class="l-teaser-list__item">
          <div class="c-icon-teaser">
            <a href="/SharedDocs/Kundeninfos/EN/13/2023/06720-23_kundeninfo_en.pdf?__blob=publicationFile">
              <span class="c-icon-teaser__headline">Urgent Field Safety Notice for MANI Trocar Kit by MANI, INC.</span>
              <span class="c-icon-teaser__meta">PDF, 530KB</span>
            </a>
            <span class="c-icon-teaser__date"><span class="aural">Date:</span> 02. May 2023</span>
            <span class="c-icon-teaser__topic"><span class="aural">Topics:</span> Medical devices</span>
            <span class="c-icon-teaser__category"><span class="aural">Type:</span> Customer information</span>
            <p class="c-icon-teaser__reference">Product group ophthalmological technology<br/>Reference 06720/23</p>
          </div>
        </li>
      </section>
    </body></html>
    """
    parsed = parse_bfarm_results(html, "https://www.bfarm.de")
    assert parsed[0]["url"] == "https://www.bfarm.de/SharedDocs/Kundeninfos/EN/13/2023/06720-23_kundeninfo_en.pdf?__blob=publicationFile"
    assert parsed[0]["reference"] == "06720/23"
    normalized = normalize_bfarm_record(parsed[0])
    assert normalized["source"] == "BFARM_RECALLS"
    assert normalized["event_id"] == "06720/23"
    assert normalized["event_date"] == "2023-05-02"
    assert normalized["event_link"] == "https://www.bfarm.de/SharedDocs/Kundeninfos/EN/13/2023/06720-23_kundeninfo_en.pdf?__blob=publicationFile"


def test_bfarm_search_payload() -> None:
    payload = build_bfarm_search_payload("trocar", year_facet="lastyear")
    assert payload["templateQueryString"] == "trocar"
    assert payload["dateOfIssue_dt"] == "lastyear"
    assert payload["sortOrder"] == "dateOfIssue_dt desc"


def test_bfarm_year_facets_for_current_range() -> None:
    assert build_bfarm_year_facets([2024, 2025, 2026]) == ["thisyear", "lastyear", "penultimateyear"]


def test_bfarm_period_link_parser() -> None:
    html = """
    <a href="SiteGlobals/Forms/Suche/EN/Expertensuche_Formular.html?dateOfIssue_dt=lastyear&templateQueryString=trocar&submit=Send">2025 (2)</a>
    <a href="SiteGlobals/Forms/Suche/EN/Expertensuche_Formular.html?dateOfIssue_dt=penultimateyear&templateQueryString=trocar&submit=Send">2024 (1)</a>
    """
    links = extract_bfarm_period_links(html, "https://www.bfarm.de/SiteGlobals/Forms/Suche/EN/Expertensuche_Formular.html")
    assert [link["year"] for link in links] == [2025, 2024]


def test_bfarm_direct_fetch_mock_http() -> None:
    session = FakeTgaSession([
        make_response(200, b"""<html><body>
        <a href='SiteGlobals/Forms/Suche/EN/Expertensuche_Formular.html?dateOfIssue_dt=penultimateyear&templateQueryString=trocar&submit=Send'>2024 (1)</a>
        </body></html>""", "text/html"),
        make_response(200, b"<html><body><section id='results'><li class='l-teaser-list__item'><a href='/node/123'>Example Trocar Recall</a><span class='c-icon-teaser__date'>Date: 2024-05-01</span><span class='c-icon-teaser__topic'>Topics: Medical devices</span><span class='c-icon-teaser__category'>Type: Customer information</span><p>Reference 12345/24</p></li></section></body></html>", "text/html"),
    ])
    config = BfarmClientConfig(base_url="https://www.bfarm.de", search_path="/SiteGlobals/Forms/Suche/EN/Expertensuche_Formular.html")
    df, warnings = fetch_bfarm_recalls_direct(["trocar"], [2024], client_config=config, session=session)
    assert len(df) == 1
    assert session.calls[0]["params"]["templateQueryString"] == "trocar"
    assert "dateOfIssue_dt" not in session.calls[0]["params"]
    assert "dateOfIssue_dt=penultimateyear" in session.calls[1]["url"]
    assert warnings


def test_health_canada_payload_uses_keyword_and_pagination() -> None:
    payload = build_health_canada_mdi_payload("trocar", start=100, length=50)

    assert payload["term_search"] == "trocar"
    assert payload["start"] == "100"
    assert payload["length"] == "50"
    assert payload["columns[0][data]"] == "incident.incident_id"


def test_health_canada_response_parser_json() -> None:
    response = make_response(
        200,
        b'{"draw":1,"recordsTotal":1,"recordsFiltered":1,"data":[{"incident":{"incident_id":997411}}]}',
    )

    parsed = parse_health_canada_mdi_response(response)

    assert parsed["recordsFiltered"] == 1
    assert parsed["data"][0]["incident"]["incident_id"] == 997411


def test_health_canada_normalization_preserves_source_specific_fields() -> None:
    normalized = normalize_health_canada_mdi_record(
        {
            "incident": {
                "incident_id": 997411,
                "trade_name": ["ENDOPATH XCEL"],
                "company_name": ["Ethicon"],
                "receipt_date": "2024-08-14",
                "device_desc_e": ["Cannula, trocar"],
                "hazard_severity_code_e": "II",
                "problem_detail": [
                    {"desc_e": "A0401 - Break", "code_type_e": "Medical Device Problem"},
                    {"desc_e": "E2008 - Foreign Body In Patient", "code_type_e": "Health Effect"},
                ],
                "device_detail": [{"pref_name_code": "87"}],
            }
        }
    )

    assert normalized["source"] == "HEALTH_CANADA_MDI"
    assert normalized["event_id"] == "997411"
    assert normalized["event_date"] == "2024-08-14"
    assert normalized["product_name"] == "ENDOPATH XCEL"
    assert normalized["manufacturer"] == "Ethicon"
    assert normalized["device_problem_text"] == "A0401 - Break"
    assert normalized["patient_outcome"] == "E2008 - Foreign Body In Patient"
    assert normalized["source_specific"]
    assert normalized["raw_record"]


def test_health_canada_direct_fetch_success_with_mock_http() -> None:
    session = FakeTgaSession(
        [
            make_response(
                200,
                b'{"draw":1,"recordsTotal":1,"recordsFiltered":1,"data":[{"incident":{"incident_id":997411,"trade_name":["ENDOPATH XCEL"],"company_name":["Ethicon"],"receipt_date":"2024-08-14","device_desc_e":["Cannula, trocar"],"hazard_severity_code_e":"II","problem_detail":[{"desc_e":"A0401 - Break","code_type_e":"Medical Device Problem"}]}}]}',
            )
        ]
    )
    config = HealthCanadaMdiClientConfig(
        server_side_endpoint="https://example.test/mdi",
        page_size=100,
    )

    df, warnings = fetch_health_canada_mdi_direct(
        ["trocar"],
        [2024],
        client_config=config,
        session=session,
        max_pages=1,
    )

    assert len(df) == 1
    assert df.iloc[0]["source"] == "HEALTH_CANADA_MDI"
    assert df.iloc[0]["event_id"] == "997411"
    assert session.calls[0]["data"]["term_search"] == "trocar"
    assert warnings


def test_tga_daen_csv_loader_maps_flexible_columns(tmp_path) -> None:
    sample = pd.DataFrame(
        [
            {
                "case_number": "TGA-1",
                "notification_date": "2025-02-03",
                "device": "Trocar access system",
                "sponsor": "Example Sponsor",
                "report_type": "Injury",
                "description": "Trocar seal leaked during surgery.",
                "url": "https://example.test/tga-1",
            }
        ]
    )
    csv_path = tmp_path / "tga.csv"
    sample.to_csv(csv_path, index=False)

    df = load_tga_daen_csv(csv_path)

    assert list(df["source"]) == ["TGA_DAEN"]
    assert list(df["source_type"]) == ["regulatory_adverse_event"]
    assert list(df["country"]) == ["Australia"]
    assert list(df["event_id"]) == ["TGA-1"]
    assert list(df["product_name"]) == ["Trocar access system"]
    assert list(df["manufacturer"]) == ["Example Sponsor"]
    assert list(df["event_type"]) == ["Injury"]
    assert list(df["year"]) == [2025]


def test_tga_daen_connector_filters_csv_by_keyword_and_year(tmp_path) -> None:
    sample = pd.DataFrame(
        [
            {
                "reference": "TGA-1",
                "date": "2025-01-01",
                "product": "Trocar",
                "company": "Example",
                "summary": "Trocar obturator broke.",
            },
            {
                "reference": "TGA-2",
                "date": "2024-01-01",
                "product": "Catheter",
                "company": "Example",
                "summary": "Catheter issue.",
            },
        ]
    )
    csv_path = tmp_path / "tga.csv"
    sample.to_csv(csv_path, index=False)

    result = TgaDaenConnector().fetch(["trocar"], [2025], csv_path=csv_path)

    assert result.success is True
    assert len(result.records) == 1
    assert result.records.iloc[0]["event_id"] == "TGA-1"


def test_tga_query_builder_uses_user_terms_and_years() -> None:
    config = TgaDaenClientConfig(
        search_page_url="https://example.test/search",
        device_search_endpoint="https://example.test/search/devices",
        report_search_endpoint="https://example.test/search/reports",
        page_size=25,
    )

    query = build_tga_daen_query(["catheter"], [2024, 2025], components=["balloon"], client_config=config)

    assert query["url"] == "https://example.test/search/devices"
    assert query["json"]["prefix"] == "catheter OR balloon"
    assert query["method"] == "POST"


def test_tga_date_range_builder_caps_end_date() -> None:
    start, end = build_tga_date_range([2024, 2025, 2026], explicit_end_date="2026-02-07")
    assert start == "2024-01-01"
    assert end == "2026-02-07"


def test_tga_search_result_counts_parser() -> None:
    parsed = parse_tga_search_result_counts(
        "145 medical devices selected between 01/01/2024 - 07/02/2026.\nNumber of reports: 207"
    )
    assert parsed["selected_devices_count"] == 145
    assert parsed["expected_report_count"] == 207
    assert parsed["search_start_date"] == "2024-01-01"
    assert parsed["search_end_date"] == "2026-02-07"


def test_tga_response_parser_json() -> None:
    response = requests.Response()
    response.status_code = 200
    response.url = "https://example.test/search"
    response.headers["Content-Type"] = "application/json"
    response._content = (
        b'{"records":[{"case_number":"TGA-1","date":"2025-03-04",'
        b'"device":"Trocar","description":"Trocar seal leaked"}]}'
    )

    parsed = parse_tga_daen_response(response)

    records = parsed["records"]
    assert len(records) == 1
    assert records.iloc[0]["source"] == "TGA_DAEN"
    assert records.iloc[0]["event_id"] == "TGA-1"
    assert records.iloc[0]["year"] == 2025


def test_tga_response_parser_html_table() -> None:
    response = requests.Response()
    response.status_code = 200
    response.url = "https://example.test/search"
    response.headers["Content-Type"] = "text/html"
    response._content = (
        b"<html><body><table>"
        b"<tr><th>case_number</th><th>date</th><th>device</th><th>summary</th></tr>"
        b"<tr><td>TGA-9</td><td>2025-04-05</td><td>Trocar</td><td>Trocar issue</td></tr>"
        b"</table></body></html>"
    )

    parsed = parse_tga_daen_response(response)
    records = parsed["records"]

    assert len(records) == 1
    assert records.iloc[0]["event_id"] == "TGA-9"
    assert records.iloc[0]["product_name"] == "Trocar"


def test_tga_device_option_parser_json() -> None:
    response = requests.Response()
    response.status_code = 200
    response.url = "https://example.test/search"
    response.headers["Content-Type"] = "application/json"
    response._content = (
        b'{"d":[{"Key":"24971,AU38254","HashKey":"3#24971,AU38254",'
        b'"ProductName":"Abdominal trocar","Manufacturer":"Applied Medical Resources",'
        b'"DisplayName":"Applied Medical Resources - Abdominal trocar","GMDNTerm":"Abdominal trocar"}]}'
    )

    parsed = extract_tga_device_options(response)
    assert len(parsed) == 1
    assert parsed[0]["DisplayName"] == "Applied Medical Resources - Abdominal trocar"


def test_tga_print_report_text_parser_rows() -> None:
    text = """
    Report #: 95253
    Date: 29/02/2024
    Trade Name: Laparoscopic Port and Trocar
    Model/Ref: Auto Suture 5.5mm
    Event Description: Small piece of plastic dislodged and was found inside the patient. Retrieved.
    Outcome: Mechanical Problem
    """
    parsed = parse_tga_print_report_text(text)
    assert parsed[0]["report_number"] == "95253"
    assert parsed[0]["report_date"] == "2024-02-29"
    assert parsed[0]["trade_name"] == "Laparoscopic Port and Trocar"
    assert parsed[0]["model_ref"] == "Auto Suture 5.5mm"
    assert "plastic dislodged" in parsed[0]["event_description"]
    assert parsed[0]["outcome"] == "Mechanical Problem"


def test_tga_print_report_html_parser_rows() -> None:
    html = """
    <html><body><table>
    <tr><th>Report #</th><th>Date</th><th>Trade Name</th><th>Model/Ref</th><th>Event Description</th><th>Outcome</th></tr>
    <tr><td>99093</td><td>14/08/2024</td><td>Kii FIOS ADVFIX</td><td>CFF03</td><td>Bowel perforated upon entry with the device.</td><td>Injury</td></tr>
    </table></body></html>
    """
    parsed = parse_tga_print_report_html(html)
    assert parsed[0]["report_number"] == "99093"
    assert parsed[0]["report_date"] == "2024-08-14"
    assert parsed[0]["trade_name"] == "Kii FIOS ADVFIX"
    assert parsed[0]["model_ref"] == "CFF03"
    assert "Bowel perforated" in parsed[0]["event_description"]
    assert parsed[0]["outcome"] == "Injury"


def test_tga_normalization_maps_unified_fields() -> None:
    normalized = normalize_tga_daen_record(
        {
            "report_id": "AUS-123",
            "event_date": "20250102",
            "manufacturer": "Maker",
            "product": "Trocar",
            "model": "M1",
            "code": "P1",
            "outcome": "Injury",
            "device_problem": "Leak",
            "patient_problem": "Pain",
            "summary": "Trocar leak reported",
            "link": "https://example.test/record",
        }
    )

    assert normalized["source"] == "TGA_DAEN"
    assert normalized["source_type"] == "regulatory_adverse_event"
    assert normalized["country"] == "Australia"
    assert normalized["device_model"] == "M1"
    assert normalized["product_code"] == "P1"
    assert normalized["record_hash"]


def test_tga_report_normalization_maps_unified_fields() -> None:
    normalized = normalize_tga_daen_report(
        {
            "report_number": "126402",
            "date": "21/01/2026",
            "trade_name": "OPT Bladeless Stability",
            "model_ref": "2B5LT",
            "event_description": "Bowel perforation.",
            "outcome": "Injury",
        },
        {"raw_link": "https://example.test/report"},
    )
    assert normalized["source"] == "TGA_DAEN"
    assert normalized["event_id"] == "126402"
    assert normalized["date_of_event"] == "2026-01-21"
    assert normalized["product_name"] == "OPT Bladeless Stability"
    assert normalized["record_hash"]


def test_tga_find_urls_from_result_page() -> None:
    html = """
    <html><body>
      <a href="/reports/list">List of reports</a>
      <a href="/reports/print">Print version of this report</a>
    </body></html>
    """
    assert find_tga_list_of_reports_url(html, "https://apps.tga.gov.au/prod/DEVICES/daen-report.aspx") == "https://apps.tga.gov.au/reports/list"
    assert find_tga_print_version_url(html, "https://apps.tga.gov.au/prod/DEVICES/daen-report.aspx") == "https://apps.tga.gov.au/reports/print"


class FakeTgaSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        response = self.responses.pop(0)
        if params and "page" in params:
            response.url = f"{url}?page={params.get('page')}"
        else:
            response.url = url
        return response

    def post(self, url, params=None, json=None, data=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "json": json, "data": data, "headers": headers, "timeout": timeout})
        response = self.responses.pop(0)
        response.url = url
        return response


def make_response(status_code: int, content: bytes, content_type: str = "application/json") -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.headers["Content-Type"] = content_type
    response._content = content
    return response


def test_tga_print_report_fetch_returns_bytes() -> None:
    session = FakeTgaSession([make_response(200, b"%PDF-1.4 report", "application/pdf")])
    content, content_type, final_url = fetch_tga_print_report(
        "https://example.test/report",
        session=session,
        debug=False,
        client_config=TgaDaenClientConfig(report_search_endpoint="https://example.test/report"),
    )
    assert content.startswith(b"%PDF")
    assert "pdf" in content_type
    assert final_url == "https://example.test/report"


def test_tga_direct_fetch_success_with_mock_http() -> None:
    session = FakeTgaSession(
        [
            make_response(200, b"<html>entry</html>", "text/html"),
            make_response(200, b'{"d":null}'),
            make_response(
                200,
                b'{"d":[{"Key":"27201","HashKey":"0#27201","ProductName":"Trocar Triport","Manufacturer":"Advanced Surgical Concepts","DisplayName":"Advanced Surgical Concepts - Trocar Triport","GMDNTerm":"Laparoscopic multi-instrument access port"}]}',
            ),
            make_response(
                200,
                b"<html><body><a href='/reports/list'>List of reports</a><a href='/reports/print'>Print version of this report</a></body></html>",
                "text/html",
            ),
            make_response(
                200,
                b"""<html><body><table>
                <tr><th>Report #</th><th>Date</th><th>Trade Name</th><th>Model/Ref</th><th>Event Description</th><th>Outcome</th></tr>
                <tr><td>95253</td><td>29/02/2024</td><td>Laparoscopic Port and Trocar</td><td>Auto Suture 5.5mm</td><td>Small piece of plastic dislodged and was found inside the patient. Retrieved.</td><td>Mechanical Problem</td></tr>
                </table></body></html>""",
                "text/html",
            ),
        ]
    )
    config = TgaDaenClientConfig(
        search_page_url="https://example.test/tga",
        device_search_endpoint="https://example.test/tga/devices",
        report_search_endpoint="https://example.test/tga/reports",
        page_size=50,
    )

    result = fetch_tga_daen_direct(["trocar"], [2025], start_date="2024-01-01", end_date="2026-02-07", client_config=config, session=session)

    assert len(result["records"]) == 1
    assert result["records"].iloc[0]["event_id"] == "95253"
    assert bool(result["records"].iloc[0]["source_query_match"]) is True
    assert session.calls[2]["json"]["prefix"] == "trocar"


def test_tga_direct_fetch_failed_request() -> None:
    session = FakeTgaSession([
        make_response(200, b"<html>entry</html>", "text/html"),
        make_response(200, b'{"d":null}'),
        make_response(500, b"server error", "text/plain"),
    ])
    config = TgaDaenClientConfig(
        search_page_url="https://example.test/tga",
        device_search_endpoint="https://example.test/tga/devices",
        report_search_endpoint="https://example.test/tga/reports",
        page_size=50,
    )

    result = TgaDaenConnector(client_config=config).fetch(["trocar"], [2025], session=session)

    assert result.success is False
    assert "TGA DAEN direct fetch failed" in result.error_message
    assert result.warnings


def test_tga_report_fetch_for_devices() -> None:
    session = FakeTgaSession(
        [
            make_response(200, b"<html>entry</html>", "text/html"),
            make_response(200, b'{"d":null}'),
            make_response(200, b"<html><body><a href='/reports/print'>Print version of this report</a></body></html>", "text/html"),
            make_response(200, b"<html><body><table><tr><th>Report #</th><th>Date</th><th>Trade Name</th><th>Model/Ref</th><th>Event Description</th><th>Outcome</th></tr><tr><td>93155</td><td>10/04/2024</td><td>OMS-T10SB Trocar Balloon</td><td>OMS-T10SB</td><td>Valve seal completely came off as mesh was being introduced.</td><td>No injury</td></tr></table></body></html>", "text/html"),
        ]
    )
    config = TgaDaenClientConfig(report_search_endpoint="https://example.test/report", page_size=50)
    df, warnings = fetch_tga_reports_for_devices(["3#24971,AU38254"], "2024-01-01", "2026-02-07", session=session, client_config=config)

    assert len(df) == 1
    assert warnings


def test_tga_detects_report_page_urls() -> None:
    html = "<a href='/reports/list'>List of reports</a><a href='/reports/print'>Print version of this report</a>"
    assert find_tga_list_of_reports_url(html, "https://apps.tga.gov.au/prod/DEVICES/daen-report.aspx")
    assert find_tga_print_version_url(html, "https://apps.tga.gov.au/prod/DEVICES/daen-report.aspx")
