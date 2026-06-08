"""Rule-based incident classifier for trocar-related adverse event narratives."""

from __future__ import annotations

import re
from dataclasses import dataclass

from pipeline.utils import normalize_text


@dataclass(frozen=True)
class ClassificationResult:
    """Incident category plus lightweight explainability metadata."""

    category: str
    confidence: str
    reason: str


NEGATION_PATTERNS = [
    r"\b(no|not|without|denies|denied)\b.{0,40}\b(leak|leakage|break|broken|broke|fracture|fractured|crack|cracked|rupture|separat|detach|dislodg|failure|failed|malfunction)\w*\b",
    r"\b(leak|leakage|break|broken|broke|fracture|fractured|crack|cracked|rupture|separat|detach|dislodg|failure|failed|malfunction)\w*\b.{0,40}\b(not observed|not found|not reported|was not|were not|did not occur)\b",
]

PACKAGING_CONTEXT = r"\b(packaging|package|pouch|box|carton|label|labeling|tray|outer box)\b"


CATEGORY_RULES: list[dict[str, object]] = [
    {
        "category": "seal breaking",
        "high": [
            r"\b(trocar\s+)?seal\b.{0,80}\b(broke|broken|break|failure|failed|fails|detached|dislodged|tear|torn|rupture|ruptured|damage|damaged|lost)\b",
            r"\b(duckbill|membrane|gasket)\b.{0,80}\b(seal|valve)?\b.{0,80}\b(broke|broken|tear|torn|rupture|detached|dislodged|failed|damaged)\b",
            r"\blost\b.{0,40}\bseal\b",
        ],
        "medium": [
            r"\bseal\b.{0,80}\b(leak|leaks|leaked|leakage)\b",
            r"\b(trocar|port)\b.{0,80}\bseal\b.{0,80}\bproblem\b",
        ],
    },
    {
        "category": "Loosen or broken valve",
        "high": [
            r"\bvalve\b.{0,80}\b(loose|loosened|dislodged|displaced|detached|broken|broke|cracked|crack|failure|failed|damaged|fell|fall)\b",
            r"\b(loose|broken|cracked|dislodged|detached|displaced)\b.{0,80}\bvalve\b",
            r"\bduckbill\b.{0,80}\bvalve\b.{0,80}\b(loose|loosened|dislodged|displaced|detached|broken|broke|cracked|failed|fell|fall)\b",
        ],
        "medium": [
            r"\bvalve\b.{0,80}\b(issue|problem|malfunction|would not close|did not close)\b",
        ],
    },
    {
        "category": "Balloon issue",
        "high": [
            r"\bballoon\b.{0,100}\b(failed to inflate|would not inflate|did not inflate|deflat|rupture|ruptured|leak|leaked|leakage|detach|detached|burst|crack|cracked)\w*",
            r"\binflation port\b.{0,100}\b(crack|cracked|broken|broke|leak|leaked|leakage|failure|failed|damaged)\w*",
            r"\bretention balloon\b.{0,100}\b(issue|problem|failure|failed|leak|rupture|detached)\b",
        ],
        "medium": [
            r"\bballoon\b.{0,100}\b(issue|problem|malfunction|would not stay inflated|inflation)\b",
        ],
    },
    {
        "category": "Broken Obturator tip",
        "high": [
            r"\bobturator\b.{0,80}\btip\b.{0,80}\b(broke|broken|break|fracture|fractured|snapped|detached|separated|cracked)\b",
            r"\btip\b.{0,80}\b(broke|broken|break|fracture|fractured|snapped|detached|separated|cracked)\b.{0,80}\bobturator\b",
        ],
        "medium": [
            r"\bobturator\b.{0,80}\b(broke|broken|break|fracture|fractured|snapped|cracked|damaged|malfunction)\b",
        ],
    },
    {
        "category": "Broken cannula",
        "high": [
            r"\bcannula\b.{0,80}\b(broke|broken|break|crack|cracked|fracture|fractured|split|snapped|separated)\b",
            r"\bshaft\b.{0,80}\b(fracture|fractured|broken|broke|cracked|crack|split|snapped)\b",
            r"\bsleeve\b.{0,80}\b(broke|broken|break|cracked|crack|fractured|fracture|split|snapped)\b",
        ],
        "medium": [
            r"\bcannula\b.{0,80}\b(damaged|defect|malfunction|issue|problem)\b",
            r"\bsleeve\b.{0,80}\b(damaged|defect|malfunction|issue|problem)\b",
        ],
    },
    {
        "category": "separation of devices",
        "high": [
            r"\bseparat(ed|ion)\b.{0,100}\b(device|component|part|tip|shaft|cannula|obturator|valve|spring|piece)\b",
            r"\b(device|component|part|tip|shaft|cannula|obturator|valve|spring|piece)\b.{0,100}\bseparat(ed|ion)\b",
            r"\b(detach|detached|disengaged|dislodged)\b.{0,100}\b(device|component|part|tip|shaft|cannula|obturator|valve|spring|piece)\b",
            r"\b(device|component|part|tip|shaft|cannula|obturator|valve|spring|piece)\b.{0,100}\b(detach|detached|disengaged|dislodged|fell off|came off)\b",
        ],
        "medium": [
            r"\b(piece|fragment|component|part)\b.{0,80}\b(loose|missing|came loose)\b",
        ],
    },
    {
        "category": "Trocar Leakage",
        "high": [
            r"\btrocar\b.{0,100}\b(leak|leaks|leaked|leakage|gas leak|co2 leak|insufflation leak|loss of pneumoperitoneum)\b",
            r"\bport\b.{0,100}\b(leak|leaks|leaked|leakage|gas leak|co2 leak|insufflation leak)\b",
            r"\binsufflation\b.{0,100}\b(leak|leaks|leaked|leakage|loss)\b",
            r"\bco2\b.{0,100}\b(leak|leaks|leaked|leakage|loss)\b",
            r"\bloss of pneumoperitoneum\b",
        ],
        "medium": [
            r"\bloss of visibility\b.{0,100}\b(leak|co2|insufflation|gas)\b",
            r"\bair leak\b|\bgas escaped\b",
        ],
    },
    {
        "category": "Difficulty in removing the obturator",
        "high": [
            r"\bdifficulty\b.{0,80}\b(remov|withdraw|release)\w*\b.{0,80}\bobturator\b",
            r"\bobturator\b.{0,100}\b(stuck|jammed|unable to remove|unable to withdraw|hard to remove|difficult to remove|would not remove|could not remove|would not release|would not click out|click out)\b",
            r"\bobturator\b.{0,100}\b(cannot|could not|unable|failed)\b.{0,80}\b(remove|withdraw|release)\b",
        ],
        "medium": [
            r"\b(removal|withdrawal)\b.{0,80}\b(difficult|difficulty|stuck|jammed)\b",
        ],
    },
    {
        "category": "Small flake was found during surgery",
        "high": [
            r"\bflake\b.{0,80}\b(found|noted|observed|seen|identified|discovered)\b.{0,80}\b(surgery|procedure|operation|intraoperative|intraoperatively)\b",
            r"\b(small particle|particle|fragment|shaving|shavings|debris)\b.{0,80}\b(found|noted|observed|seen|identified|discovered)\b.{0,80}\b(surgery|procedure|operation|intraoperative|intraoperatively)\b",
            r"\b(fragment|particle|flake|debris|shaving|shavings)\b.{0,100}\bintraoperative\b",
            r"\bintraoperative\b.{0,100}\b(fragment|particle|flake|debris|shaving|shavings)\b",
        ],
        "medium": [
            r"\bforeign material\b.{0,80}\b(found|observed|identified)\b",
        ],
    },
    {
        "category": "incorrect sterilization",
        "high": [
            r"\bincorrect sterilization\b",
            r"\bsteriliz\w+\b.{0,100}\b(incorrect|inadequate|insufficient|failed|failure|deviation|non-sterile|non sterile|not sterile)\b",
            r"\b(non[- ]sterile|not sterile)\b",
            r"\bsterility\b.{0,100}\b(breach|failure|failed|compromised|concern)\b",
            r"\bsterile barrier\b.{0,100}\b(breach|compromised|failed|failure|damaged)\b",
        ],
        "medium": [
            r"\bcontamination\b.{0,80}\b(sterile|sterility|device|product)\b",
        ],
    },
    {
        "category": "High level of endotoxin on product",
        "high": [
            r"\bendotoxin\b.{0,80}\b(high|elevated|above limit|exceed|exceeded|exceeds|out of spec|oos|contamination|contaminated)\b",
            r"\bhigh level of endotoxin\b",
            r"\bpyrogen\b.{0,80}\b(contamination|contaminated|high|elevated|above limit|exceeded|out of spec|oos)\b",
        ],
        "medium": [
            r"\bendotoxin\b|\bpyrogen\b",
        ],
    },
    {
        "category": "Pyramidal tip trocar causes vascular lacerations",
        "high": [
            r"\bpyramidal\b.{0,80}\b(tip|trocar)\b.{0,120}\b(vascular|vessel|artery|vein|aorta|iliac)\b.{0,80}\b(laceration|lacerations|injury|injuries|tear|perforation|bleeding)\b",
            r"\b(vascular|vessel|artery|vein|aorta|iliac)\b.{0,80}\b(laceration|lacerations|injury|injuries|tear|perforation|bleeding)\b.{0,120}\bpyramidal\b",
            r"\btrocar\b.{0,120}\bvascular\b.{0,80}\b(laceration|lacerations|injury|injuries)\b",
        ],
        "medium": [
            r"\btrocar\b.{0,120}\b(laceration|perforation|bleeding)\b",
        ],
    },
    {
        "category": "Retractable shield trocar design cause serious injuries and deaths",
        "high": [
            r"\bretractable shield\b.{0,120}\b(trocar|device|design)\b.{0,120}\b(injury|injuries|death|deaths|fatal|fatality|serious)\b",
            r"\bshielded trocar\b.{0,120}\b(injury|injuries|death|deaths|fatal|fatality|serious)\b",
            r"\bretractable\b.{0,80}\bshield\b.{0,120}\b(injury|injuries|death|deaths|fatal|fatality|serious)\b",
        ],
        "medium": [
            r"\bshield\b.{0,80}\btrocar\b.{0,80}\binjury\b",
        ],
    },
]

OTHER_RULES: list[dict[str, object]] = [
    {
        "category": "Unrelated / insufficient detail",
        "patterns": [
            r"\b(no product problem|no device problem|no reported product malfunction|no reported device malfunction|unknown|insufficient information|unable to determine|routine use)\b",
        ],
    },
    {
        "category": "Sterility / packaging issue",
        "patterns": [
            r"\b(packaging|package|pouch|box|carton|tray|label|labeling)\b.{0,100}\b(damaged|open|opened|tear|torn|hole|breach|compromised|incorrect|missing|defect)\b",
            r"\b(expired|expiration|shelf life|lot number|labeling)\b.{0,80}\b(issue|incorrect|missing|wrong|error)\b",
        ],
    },
    {
        "category": "Unclassified patient injury",
        "patterns": [
            r"\b(death|died|fatal|injury|injured|bleeding|hemorrhage|perforation|laceration|infection|pain|burn|foreign body)\b",
        ],
    },
    {
        "category": "Unclassified mechanical issue",
        "patterns": [
            r"\b(malfunction|failure|failed|defect|defective|damage|damaged|broke|broken|break|crack|cracked|fracture|leak|leaked|leakage|detach|detached|separate|separated)\w*\b",
        ],
    },
]

ALLOWED_CATEGORIES = [str(rule["category"]) for rule in CATEGORY_RULES] + [str(rule["category"]) for rule in OTHER_RULES] + ["Other"]


def classify_incident(text: object | None) -> str:
    """Classify incident text using deterministic category regexes in priority order."""
    return classify_incident_details(text).category


def classify_incident_details(text: object | None) -> ClassificationResult:
    """Classify incident text and return category confidence metadata."""
    normalized = normalize_text(text)
    if not normalized:
        return ClassificationResult("Unrelated / insufficient detail", "Low", "No classifiable text")

    negated = _matches_any(normalized, NEGATION_PATTERNS)
    packaging_only = bool(re.search(PACKAGING_CONTEXT, normalized)) and not _has_device_malfunction_context(normalized)

    for rule in CATEGORY_RULES:
        category = str(rule["category"])
        if negated and category not in {"incorrect sterilization", "High level of endotoxin on product"}:
            continue
        high_match = _first_match(normalized, rule.get("high", []))
        if high_match and not (packaging_only and category not in {"incorrect sterilization", "High level of endotoxin on product"}):
            return ClassificationResult(category, "High", high_match)
        medium_match = _first_match(normalized, rule.get("medium", []))
        if medium_match and not packaging_only:
            return ClassificationResult(category, "Medium", medium_match)

    for rule in OTHER_RULES:
        if negated and str(rule["category"]) == "Unclassified mechanical issue":
            continue
        match = _first_match(normalized, rule.get("patterns", []))
        if match:
            return ClassificationResult(str(rule["category"]), "Low", match)

    return ClassificationResult("Other", "Low", "No category rule matched")


def classify_custom_incident_details(
    text: object | None,
    components: list[str] | None = None,
    accident_terms: list[str] | None = None,
) -> ClassificationResult:
    """Classify custom-device records from user-provided components and accident terms."""
    normalized = normalize_text(text)
    if not normalized:
        return ClassificationResult("Unrelated / insufficient detail", "Low", "No classifiable text")

    component_matches = _matched_terms(normalized, components or [])
    accident_matches = _matched_terms(normalized, accident_terms or [])
    inferred_accident = _infer_accident_group(normalized)
    negated = _matches_any(normalized, NEGATION_PATTERNS)

    if component_matches and accident_matches and not negated:
        component = _display_term(component_matches[0])
        accident = _display_term(accident_matches[0])
        return ClassificationResult(f"{component} - {accident} issue", "High", f"Matched component '{component}' and accident term '{accident}'")

    if component_matches and inferred_accident and not negated:
        component = _display_term(component_matches[0])
        return ClassificationResult(f"{component} - {inferred_accident}", "Medium", f"Matched component '{component}' with inferred accident '{inferred_accident}'")

    if accident_matches and not negated:
        accident = _display_term(accident_matches[0])
        return ClassificationResult(f"{accident} issue", "Medium", f"Matched accident term '{accident}'")

    if component_matches:
        component = _display_term(component_matches[0])
        return ClassificationResult(f"{component} - unspecified issue", "Low", f"Matched component '{component}' without a specific accident term")

    fallback = classify_incident_details(normalized)
    if fallback.category.startswith("Unclassified") or fallback.category in {"Sterility / packaging issue", "Unrelated / insufficient detail"}:
        return fallback
    if fallback.category != "Other":
        return ClassificationResult(fallback.category, "Low", f"Generic incident rule: {fallback.reason}")
    return fallback


def _first_match(text: str, patterns: object) -> str | None:
    for pattern in patterns if isinstance(patterns, list) else []:
        match = re.search(str(pattern), text)
        if match:
            return match.group(0)[:160]
    return None


def _matches_any(text: str, patterns: list[str]) -> bool:
    return any(re.search(pattern, text) for pattern in patterns)


def _has_device_malfunction_context(text: str) -> bool:
    device_terms = r"\b(device|trocar|cannula|obturator|seal|valve|balloon|sleeve|shaft|tip|port)\b"
    malfunction_terms = r"\b(malfunction|failure|failed|defect|damage|damaged|broke|broken|fracture|crack|leak|detach|separate|rupture)\w*\b"
    return bool(re.search(device_terms, text) and re.search(malfunction_terms, text))


def _matched_terms(text: str, terms: list[str]) -> list[str]:
    matches: list[str] = []
    seen: set[str] = set()
    for term in terms:
        normalized = normalize_text(term)
        if not normalized or normalized in seen:
            continue
        if re.search(_term_pattern(normalized), text):
            matches.append(normalized)
            seen.add(normalized)
    return matches


def _term_pattern(term: str) -> str:
    escaped = re.escape(term).replace(r"\ ", r"\s+")
    return rf"\b{escaped}\b"


def _display_term(term: str) -> str:
    return " ".join(word.capitalize() for word in normalize_text(term).split())


def _infer_accident_group(text: str) -> str | None:
    groups = [
        ("crack/break issue", r"\b(broke|broken|break|crack|cracked|fracture|fractured|split|snapped)\w*\b"),
        ("leakage issue", r"\b(leak|leaks|leaked|leakage|gas leak|fluid leak)\b"),
        ("separation/detachment issue", r"\b(separat|detach|detached|dislodged|fell off|came off)\w*\b"),
        ("inflation/deflation issue", r"\b(inflat|deflat|rupture|ruptured|burst)\w*\b"),
        ("removal/deployment difficulty", r"\b(stuck|jammed|difficult|difficulty|unable|could not|would not)\b.{0,80}\b(remove|withdraw|deploy|release|insert)\w*\b"),
        ("sterility/packaging issue", r"\b(steril|sterility|non-sterile|packaging|package|pouch|label|expired|expiration)\w*\b"),
        ("patient injury", r"\b(injury|injured|death|fatal|bleeding|hemorrhage|perforation|laceration|infection|pain|burn|foreign body)\b"),
        ("mechanical malfunction", r"\b(malfunction|failure|failed|defect|defective|damage|damaged)\w*\b"),
    ]
    for label, pattern in groups:
        if re.search(pattern, text):
            return label
    return None
