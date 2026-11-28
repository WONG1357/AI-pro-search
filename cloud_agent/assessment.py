"""Validate an interpretation against the full text of its cited evidence."""

import json
import re

from cloud_agent.schema import SOURCE_INFO


ASSESSMENT_FORMAT_VERSION = 2

_RUN_STATUS = {
    "complete_for_requested_queries": "Complete within the selected queries.",
    "incomplete": "Coverage is incomplete or unverified.",
    "failed": "All selected sources failed. The search could not determine whether matching reports exist.",
}


def _text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _md(value: object) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ").replace("<", "&lt;").replace(">", "&gt;")


def _review_header(overview: dict) -> list[str]:
    request = overview["request"]
    status = overview["status"]
    structured_status = overview.get("structured_retrieval_status", status)
    lines = [
        "# Agent assessment", "",
        "**Human review required.** Evidence IDs and excerpts were verified against this run. "
        "Check that each excerpt supports the claim before relying on the interpretation.", "",
        f"**Run status** — {_RUN_STATUS[status]}", "",
        f"**Requested dates** — {request['start_date']} through {request['end_date']} (inclusive)",
    ]
    if structured_status != status:
        lines += ["", f"**Structured search** — {_RUN_STATUS[structured_status]}"]
    if overview.get("supplemental_documents"):
        lines += ["", f"**Supplemental documents** — {overview['supplemental_documents']} (coverage unverified). "
                  "These do not count as structured search results."]
    lines += ["", "| Source | Coverage | Exported records | Effective dates |",
              "| --- | --- | ---: | --- |"]
    for source in overview["source_status"]:
        name = SOURCE_INFO[source["source_name"]][0]
        start = source.get("effective_start")
        end = source.get("effective_end")
        dates = f"{start} through {end}" if start and end else "Not reported"
        lines.append(f"| {_md(name)} | {_md(source['completeness'])} | {source['exported_count']} | {_md(dates)} |")
    return [*lines, ""]


def render_assessment(assessment: dict, evidence: dict, *, overview: dict | None = None) -> str:
    if not isinstance(assessment, dict) or set(assessment) - {"claims", "limitations", "next_steps"}:
        raise ValueError("Assessment supports only claims, limitations, and next_steps")
    claims = assessment.get("claims", [])
    if not isinstance(claims, list) or len(claims) > 30:
        raise ValueError("claims must be a list of at most 30 items")
    lines = (
        _review_header(overview) if overview is not None else
        ["# Agent assessment", "", "References and excerpts below were checked against this run. This check establishes provenance, not that the interpretation is correct.", ""]
    )
    if overview is not None and claims:
        lines += ["## Claims and source excerpts", ""]
    for claim in claims:
        if not isinstance(claim, dict) or set(claim) != {"statement", "support"}:
            raise ValueError("Each claim requires exactly statement and support")
        statement, supports = claim["statement"], claim["support"]
        if not isinstance(statement, str) or not statement.strip() or len(statement) > 4000:
            raise ValueError("Invalid claim statement")
        if not isinstance(supports, list) or not supports:
            raise ValueError("Every factual claim needs evidence support")
        refs = []
        for support in supports:
            if not isinstance(support, dict) or set(support) != {"evidence_id", "quote"}:
                raise ValueError("Support requires evidence_id and quote")
            row = evidence.get(support["evidence_id"])
            quote = support["quote"]
            if row is None or not isinstance(quote, str) or len(quote.strip()) < 8:
                raise ValueError("Unknown evidence reference or missing substantive excerpt")
            haystack = "\n".join([row["narrative"], row["patient_outcome"], row["device_problem"], _text(row["raw_record"])])
            if quote not in haystack:
                raise ValueError("Supporting excerpt does not occur in the cited evidence")
            refs.append((support["evidence_id"], quote))
        if overview is None:
            ids = ", ".join(f"`{identifier}`" for identifier, _ in refs)
            lines += [f"- {_md(statement)} ({ids})"]
            continue
        lines += [f"- {_md(statement)}"]
        for identifier, quote in refs:
            # A longer fence keeps literal Markdown from being rendered as formatted content.
            fence = "~" * (max((len(run) for run in re.findall(r"~{3,}", quote)), default=2) + 1)
            lines += ["", f"  Excerpt from `{identifier}`", f"  {fence}text",
                      *["  " + line for line in quote.split("\n")], f"  {fence}"]
    if overview is not None and not claims:
        lines += ["No evidence-backed claims were provided in this assessment.", ""]
    for key, heading in (("limitations", "Limitations"), ("next_steps", "Suggested follow-up")):
        items = assessment.get(key, [])
        if not isinstance(items, list) or any(not isinstance(v, str) or len(v) > 4000 for v in items) or len(items) > 30:
            raise ValueError(f"Invalid {key}")
        if items:
            lines += ["", "## " + heading, ""] + ["- " + _md(item) for item in items]
    return "\n".join(lines) + "\n"
