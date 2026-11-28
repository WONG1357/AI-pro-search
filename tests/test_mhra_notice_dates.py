"""Regression cases for notice and publication dates on MHRA weekly lists."""

import json
from unittest.mock import Mock

import pytest
import requests

from pipeline.pipeline import clean_records, filter_records
from pipeline.sources.mhra_fsca import fetch_mhra_fsca_direct


WEEKLY_URL = "/drug-device-alerts/field-safety-notices-fsns-from-1-to-5-january-2024"
WEEKLY_ISSUED = "<dl><dt>Issued:</dt><dd>9 January 2024</dd></dl>"


def _response(html):
    response = requests.Response()
    response.status_code = 200
    response.encoding = "utf-8"
    response._content = html.encode()
    return response


def _fetch(item_date, start, end, page_metadata=WEEKLY_ISSUED):
    listing = f'<li class="gem-c-document-list__item"><a href="{WEEKLY_URL}">Weekly notices</a> Issued: 9 January 2024</li>'
    detail = f"""<main>{page_metadata}<div class="gem-c-govspeak">
      <h3>Manufacturer: Example device</h3><p>{item_date}</p>
      <p>Model: Example</p></div></main>"""
    session = Mock(spec=requests.Session)
    session.get.side_effect = [_response(listing), _response(detail)]
    years = list(range(int(start[:4]), int(end[:4]) + 1))
    records, warnings = fetch_mhra_fsca_direct(["Example"], years, start, end, session=session)
    assert records.attrs["completeness"] == "unverified"
    assert any("individual notice date" in warning for warning in warnings)
    return records if records.empty else filter_records(clean_records(records), ["Example"], years, start, end)


def test_individual_notice_in_earlier_year_survives_the_date_filter():
    records = _fetch("20 November 2023", "2023-11-20", "2023-11-20")
    assert len(records) == 1
    row = records.iloc[0]
    assert row.search_date == "2023-11-20"
    assert row.search_date_basis == "manufacturer_notice_date"
    assert row.report_date == "2024-01-09"
    assert row.date_of_event == ""
    assert json.loads(row.raw_record)["weekly_issued_date"] == row.report_date


def test_partial_notice_date_is_not_replaced_by_day_of_weekly_issue():
    row = _fetch("January 2024", "2024-01-01", "2024-01-31", page_metadata="").iloc[0]
    assert row.search_date == "January 2024"
    assert row.report_date == "2024-01-09"
    assert row.search_date_basis == "manufacturer_notice_date"
    assert _fetch("January 2024", "2024-01-09", "2024-01-09").empty


@pytest.mark.parametrize("metadata", [WEEKLY_ISSUED, ""])
@pytest.mark.parametrize("prose", ["Implementation deadline: 31 January 2024", "January 2024 production batch"])
def test_notice_without_an_individual_date_uses_identified_publication_fallback(metadata, prose):
    row = _fetch(prose, "2024-01-09", "2024-01-09", page_metadata=metadata).iloc[0]
    assert row.search_date == row.report_date == "2024-01-09"
    assert row.search_date_basis == "weekly_publication_date"
    assert json.loads(row.raw_record)["notice_date"] == ""


def test_date_with_trailing_fsn_reference_on_the_same_line():
    row = _fetch("23 February 2024 FSN-24006", "2024-02-23", "2024-02-23").iloc[0]
    assert row.search_date == "2024-02-23"
    assert row.search_date_basis == "manufacturer_notice_date"
