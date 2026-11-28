"""Validate an interpretation against the full text of its cited evidence."""

import json


def _text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _md(value: object) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ").replace("<", "&lt;").replace(">", "&gt;")


def render_assessment(assessment: dict, evidence: dict) -> str:
    if not isinstance(assessment, dict) or set(assessment) - {"claims", "limitations", "next_steps"}:
        raise ValueError("Assessment supports only claims, limitations, and next_steps")
    claims = assessment.get("claims", [])
    if not isinstance(claims, list) or len(claims) > 30:
        raise ValueError("claims must be a list of at most 30 items")
    lines = ["# Agent assessment", "", "References and excerpts below were checked against this run. This check establishes provenance, not that the interpretation is correct.", ""]
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
            refs.append(f"`{support['evidence_id']}`")
        lines += [f"- {_md(statement)} ({', '.join(refs)})"]
    for key, heading in (("limitations", "Limitations"), ("next_steps", "Suggested follow-up")):
        items = assessment.get(key, [])
        if not isinstance(items, list) or any(not isinstance(v, str) or len(v) > 4000 for v in items) or len(items) > 30:
            raise ValueError(f"Invalid {key}")
        if items:
            lines += ["", "## " + heading, ""] + ["- " + _md(item) for item in items]
    return "\n".join(lines) + "\n"
