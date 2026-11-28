"""Fetch explicit official documents; discovery never proves database coverage."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from io import BytesIO
import math
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup
from pypdf import PdfReader
import requests

from cloud_agent.schema import SOURCE_DOMAINS, SOURCE_INFO


def _checked_url(source: str, url: str) -> str:
    try:
        parsed = urlsplit(url)
        valid = (isinstance(url, str) and parsed.scheme in ("http", "https")
                 and parsed.hostname in SOURCE_DOMAINS[source]
                 and parsed.username is None and parsed.password is None
                 and parsed.port in (None, 80 if parsed.scheme == "http" else 443)
                 and not any(ord(char) <= 32 or char == "\\" for char in url))
    except (ValueError, TypeError, KeyError):
        valid = False
    if not valid:
        raise ValueError("Document URL must be an explicit public URL on this source's official allowlist")
    return url


def _extract(body: bytes, content_type: str, max_pages: int) -> tuple[str, str, dict]:
    if body.startswith(b"%PDF-"):
        try:
            reader = PdfReader(BytesIO(body))
            if reader.is_encrypted:
                raise ValueError("Encrypted PDF documents are not supported")
            total = len(reader.pages)
            text = "\n\n".join(reader.pages[i].extract_text() or "" for i in range(min(total, max_pages)))
        except Exception as exc:
            raise ValueError("Official PDF could not be parsed as readable evidence") from exc
        return text.strip(), ".pdf", {"pages_total": total, "pages_extracted": min(total, max_pages), "extraction_truncated": total > max_pages}
    if content_type not in ("text/html", "application/xhtml+xml"):
        raise ValueError("Official document must contain a readable PDF or HTML page")
    soup = BeautifulSoup(body, "html.parser")
    if not soup.find(["html", "body", "article", "main"]):
        raise ValueError("Response is not a recognized HTML document")
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    for element in soup(["script", "style", "noscript", "template"]):
        element.decompose()
    text = (soup.body or soup).get_text("\n", strip=True)
    inspected = (title + "\n" + text[:1000]).casefold()
    blocked = ("access denied", "just a moment", "checking your browser", "verify you are human",
               "page not found", "404 not found", "403 forbidden", "service unavailable",
               "enable javascript and cookies to continue", "attention required! | cloudflare")
    if any(marker in inspected for marker in blocked):
        raise ValueError("Official source returned an error or access challenge, not document evidence")
    return text, ".html", {"title": title, "extraction_truncated": False}


def fetch_official_document(source: str, url: str, *, session=None, timeout=15,
                            max_bytes=10 * 1024 * 1024, max_pages=20) -> tuple[dict, bytes, str]:
    """Return evidence, downloaded bytes and extension (.pdf/.html), without writing files.

    This supplementary document may be outside the requested date/device scope.
    Its dates/identifiers are unknown until reviewed; it never upgrades coverage.
    """
    if source not in SOURCE_DOMAINS:
        raise ValueError("Unknown official source identifier")
    if type(max_bytes) is not int or max_bytes < 1 or type(max_pages) is not int or not 1 <= max_pages <= 20:
        raise ValueError("max_bytes must be positive and max_pages must be from 1 to 20")
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be a positive finite number")
    current = _checked_url(source, url)
    client = session if session is not None else requests.Session()
    try:
        for redirects in range(4):
            response = client.get(current, timeout=(timeout, timeout), stream=True, allow_redirects=False)
            try:
                if response.status_code in (301, 302, 303, 307, 308):
                    if redirects == 3 or not response.headers.get("Location"):
                        raise ValueError("Official document exceeded three redirects or supplied no redirect location")
                    current = _checked_url(source, urljoin(current, response.headers["Location"]))
                    continue
                response.raise_for_status()
                if response.status_code != 200:
                    raise ValueError("Official document response was not a complete HTTP 200 response")
                length = response.headers.get("Content-Length")
                if length is not None and int(length) > max_bytes:
                    raise ValueError("Official document exceeds the download byte budget")
                output = bytearray()
                for chunk in response.iter_content(chunk_size=65536):
                    if len(output) + len(chunk) > max_bytes:
                        raise ValueError("Official document exceeds the download byte budget")
                    output.extend(chunk)
                body = bytes(output)
                content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
                break
            finally:
                response.close()
    finally:
        if session is None:
            client.close()
    text, extension, metadata = _extract(body, content_type, max_pages)
    if not text.strip():
        raise ValueError("Official document contains no extractable text; manual review or OCR is required")
    digest = hashlib.sha256(body).hexdigest()
    metadata.update({"extracted_text": text, "content_type": content_type, "final_url": current,
                     "date_status": "unknown", "coverage_note": "Supplementary official-document discovery; database coverage and query relevance are unverified."})
    evidence = {
        "evidence_id": f"WEB-{source}-{digest[:16]}", "source": source, "source_name": SOURCE_INFO[source][0],
        "record_kind": "supplemental_official_document", "native_id": "", "product_name": "", "manufacturer": "",
        "event_date": "", "report_date": "", "search_date": "", "search_date_basis": "unknown",
        "direct_record_url": url, "source_locator_url": url, "source_portal": SOURCE_INFO[source][1],
        "retrieved_at": datetime.now(timezone.utc).isoformat(), "raw_sha256": digest, "narrative": text,
        "patient_outcome": "", "device_problem": "", "category": "", "category_reason": "",
        "classification_method": "unclassified_supplemental_document", "raw_record": metadata,
        "coverage": "unverified", "extraction_truncated": metadata["extraction_truncated"],
    }
    return evidence, body, extension
