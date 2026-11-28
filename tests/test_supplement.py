"""Mocked public-document retrieval and evidence boundary regressions."""

from io import BytesIO
import hashlib

from pypdf import PdfWriter
from pypdf.generic import DictionaryObject, NameObject, DecodedStreamObject
import pytest
import requests

from cloud_agent.supplement import fetch_official_document


URL = "https://www.bfarm.de/SharedDocs/Kundeninfos/DE/06/2025/notice.pdf?__blob=publicationFile"


class Response:
    def __init__(self, body=b"", *, status=200, headers=None, chunks=None):
        self.body, self.status_code = body, status
        self.headers = headers or {}
        self.chunks, self.closed, self.streamed = chunks, False, False

    def iter_content(self, chunk_size):
        self.streamed = True
        yield from self.chunks if self.chunks is not None else [self.body]

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def close(self):
        self.closed = True


class Session:
    def __init__(self, *responses):
        self.responses, self.calls, self.closed = list(responses), [], False

    def get(self, url, **kwargs):
        assert kwargs["allow_redirects"] is False and kwargs["stream"] is True
        assert kwargs["timeout"] == (15, 15)
        self.calls.append(url)
        return self.responses.pop(0)

    def close(self):
        self.closed = True


def pdf_bytes(*pages):
    writer = PdfWriter()
    for text in pages:
        page = writer.add_blank_page(width=200, height=200)
        font = DictionaryObject({NameObject("/Type"): NameObject("/Font"), NameObject("/Subtype"): NameObject("/Type1"), NameObject("/BaseFont"): NameObject("/Helvetica")})
        page[NameObject("/Resources")] = DictionaryObject({NameObject("/Font"): DictionaryObject({NameObject("/F1"): writer._add_object(font)})})
        stream = DecodedStreamObject()
        stream.set_data(f"BT /F1 12 Tf 10 100 Td ({text}) Tj ET".encode())
        page[NameObject("/Contents")] = writer._add_object(stream)
    buffer = BytesIO()
    writer.write(buffer)
    return buffer.getvalue()


def test_official_pdf_has_hashed_supplementary_evidence_without_invented_facts():
    body = pdf_bytes("Safety notice for a trocar, 5 January 2025, reference 123.")
    response = Response(body, headers={"Content-Type": "application/pdf"})
    session = Session(response)
    evidence, raw, extension = fetch_official_document("BFARM_RECALLS", URL, session=session)
    assert raw == body and extension == ".pdf" and response.closed and not session.closed
    assert evidence["evidence_id"] == "WEB-BFARM_RECALLS-" + hashlib.sha256(body).hexdigest()[:16]
    assert evidence["raw_sha256"] == hashlib.sha256(body).hexdigest()
    assert evidence["record_kind"] == "supplemental_official_document"
    assert evidence["coverage"] == "unverified" and evidence["extraction_truncated"] is False
    for field in ("native_id", "event_date", "report_date", "search_date", "product_name", "patient_outcome", "device_problem"):
        assert evidence[field] == ""
    assert "trocar" in evidence["narrative"]
    assert evidence["raw_record"]["extracted_text"] == evidence["narrative"]
    assert evidence["direct_record_url"] == URL


def test_pdf_page_limit_is_explicit_and_retains_the_complete_original_bytes():
    body = pdf_bytes("First notice page", "Second notice page")
    evidence, raw, extension = fetch_official_document("BFARM_RECALLS", URL, session=Session(Response(body)), max_pages=1)
    assert raw == body and extension == ".pdf"
    assert evidence["extraction_truncated"] is True
    assert evidence["raw_record"]["pages_total"] == 2 and evidence["raw_record"]["pages_extracted"] == 1
    assert "First notice page" in evidence["narrative"] and "Second notice page" not in evidence["narrative"]


def test_allowed_redirect_and_html_extraction_preserve_source_provenance():
    redirect = Response(status=302, headers={"Location": "https://bfarm.de/notice.html"})
    body = b"<html><head><title>Official safety notice</title><script>do_not_quote()</script></head><body><p>Manufacturers recalled a device.</p></body></html>"
    final = Response(body, headers={"Content-Type": "text/html; charset=utf-8"})
    session = Session(redirect, final)
    evidence, raw, extension = fetch_official_document("BFARM_RECALLS", URL, session=session)
    assert raw == body and extension == ".html" and redirect.closed and final.closed
    assert evidence["direct_record_url"] == URL
    assert evidence["raw_record"]["final_url"] == "https://bfarm.de/notice.html"
    assert evidence["narrative"] == "Manufacturers recalled a device."


@pytest.mark.parametrize("url", [
    "file:///etc/passwd", "https://127.0.0.1/notice.pdf", "https://localhost/notice.pdf",
    "https://www.bfarm.de.evil.test/notice.pdf", "https://www.bfarm.de@evil.test/notice.pdf",
    "https://user:secret@www.bfarm.de/notice.pdf", "https://@www.bfarm.de/notice.pdf",
    "https://www.bfarm.de:8888/notice.pdf", "https://www.fda.gov/notice.pdf",
    "https://www.bfarm.de/notice.pdf\n", "https://www.bfarm.de\\@evil.test/notice.pdf",
])
def test_nonofficial_private_credentialled_or_malformed_urls_never_send_a_request(url):
    session = Session()
    with pytest.raises(ValueError):
        fetch_official_document("BFARM_RECALLS", url, session=session)
    assert session.calls == []


def test_unlisted_redirect_is_not_followed():
    response = Response(status=302, headers={"Location": "http://169.254.169.254/latest/meta-data/"})
    session = Session(response)
    with pytest.raises(ValueError, match="allowlist"):
        fetch_official_document("BFARM_RECALLS", URL, session=session)
    assert session.calls == [URL] and response.closed


def test_redirect_count_is_bounded():
    responses = [Response(status=302, headers={"Location": f"/redirect-{i}"}) for i in range(4)]
    session = Session(*responses)
    with pytest.raises(ValueError, match="three redirects"):
        fetch_official_document("BFARM_RECALLS", URL, session=session)
    assert len(session.calls) == 4 and all(response.closed for response in responses)


@pytest.mark.parametrize("headers,chunks,streamed", [
    ({"Content-Length": "8"}, [b"12345678"], False),
    ({}, [b"1234", b"5678"], True),
    ({"Content-Length": "1"}, [b"1234", b"5678"], True),
])
def test_download_budget_is_enforced_before_parsing_even_without_truthful_length(headers, chunks, streamed):
    response = Response(headers=headers, chunks=chunks)
    with pytest.raises(ValueError, match="byte budget"):
        fetch_official_document("BFARM_RECALLS", URL, session=Session(response), max_bytes=5)
    assert response.closed and response.streamed is streamed


@pytest.mark.parametrize("body,content_type", [
    (b'<html><title>Just a moment...</title><body>Checking your browser</body></html>', "text/html"),
    (b'<html><title>404 Not Found</title><body>Page not found</body></html>', "text/html"),
    (b'<html><body>Access denied</body></html>', "text/html"),
    (b'{"error":"unexpected source response"}', "application/json"),
    (b'{"error":"unexpected source response"}', "text/html"),
    (b'', "application/pdf"),
    (b'%PDF-1.7 invalid PDF', "application/pdf"),
    (pdf_bytes(""), "application/pdf"),
])
def test_errors_challenges_empty_documents_and_unrecognized_responses_are_not_evidence(body, content_type):
    response = Response(body, headers={"Content-Type": content_type})
    with pytest.raises(ValueError):
        fetch_official_document("BFARM_RECALLS", URL, session=Session(response))
    assert response.closed


@pytest.mark.parametrize("status", [206, 403, 404, 500])
def test_failed_or_partial_http_response_is_not_document_evidence(status):
    response = Response(b"error", status=status)
    with pytest.raises((ValueError, requests.HTTPError)):
        fetch_official_document("BFARM_RECALLS", URL, session=Session(response))
    assert response.closed


def test_owned_session_is_closed(monkeypatch):
    session = Session(Response(pdf_bytes("Public safety notice")))
    monkeypatch.setattr("cloud_agent.supplement.requests.Session", lambda: session)
    fetch_official_document("BFARM_RECALLS", URL)
    assert session.closed


@pytest.mark.parametrize("options", [{"max_bytes": 0}, {"max_pages": 0}, {"max_pages": 21}, {"timeout": float("inf")}, {"timeout": -1}])
def test_invalid_budgets_fail_before_network(options):
    session = Session()
    with pytest.raises(ValueError):
        fetch_official_document("BFARM_RECALLS", URL, session=session, **options)
    assert session.calls == []


def test_unknown_source_fails_before_network():
    session = Session()
    with pytest.raises(ValueError, match="Unknown"):
        fetch_official_document("INVENTED", URL, session=session)
    assert session.calls == []
