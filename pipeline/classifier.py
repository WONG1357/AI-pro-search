"""Rule-based incident classifier for trocar-related adverse event narratives."""

from __future__ import annotations

import re

from pipeline.utils import normalize_text


CATEGORY_PATTERNS: dict[str, list[str]] = {
    "Broken Obturator tip": [
        r"\bobturator\b.{0,80}\btip\b.{0,80}\b(broke|broken|break|fracture|fractured|snapped|detached|separated|cracked)\b",
        r"\btip\b.{0,80}\b(broke|broken|break|fracture|fractured|snapped|detached|separated|cracked)\b.{0,80}\bobturator\b",
        r"\bobturator\b.{0,80}\b(broke|broken|break|fracture|fractured|snapped|cracked)\b",
    ],
    "seal breaking": [
        r"\bseal\b.{0,80}\b(broke|broken|break|failure|failed|fails|detached|dislodged|tear|torn|rupture|ruptured|damage|damaged|lost)\b",
        r"\bseal\b.{0,80}\b(leak|leaks|leaked|leakage)\b",
        r"\blost\b.{0,40}\bseal\b",
    ],
    "Broken cannula": [
        r"\bcannula\b.{0,80}\b(broke|broken|break|crack|cracked|fracture|fractured|split|snapped|separated)\b",
        r"\bshaft\b.{0,80}\b(fracture|fractured|broken|broke|cracked|crack|split|snapped)\b",
        r"\bsleeve\b.{0,80}\b(broke|broken|break|cracked|crack|fractured|fracture|split|snapped)\b",
    ],
    "Retractable shield trocar design cause serious injuries and deaths": [
        r"\bretractable shield\b.{0,120}\b(trocar|device|design)\b.{0,120}\b(injury|injuries|death|deaths|fatal|fatality|serious)\b",
        r"\bshielded trocar\b.{0,120}\b(injury|injuries|death|deaths|fatal|fatality|serious)\b",
        r"\bretractable\b.{0,80}\bshield\b.{0,120}\b(injury|injuries|death|deaths|fatal|fatality|serious)\b",
    ],
    "Pyramidal tip trocar causes vascular lacerations": [
        r"\bpyramidal\b.{0,80}\b(tip|trocar)\b.{0,120}\b(vascular|vessel|artery|vein|aorta|iliac)\b.{0,80}\b(laceration|lacerations|injury|injuries|tear|perforation|bleeding)\b",
        r"\b(vascular|vessel|artery|vein|aorta|iliac)\b.{0,80}\b(laceration|lacerations|injury|injuries|tear|perforation|bleeding)\b.{0,120}\bpyramidal\b",
        r"\btrocar\b.{0,120}\bvascular\b.{0,80}\b(laceration|lacerations|injury|injuries)\b",
    ],
    "Balloon issue": [
        r"\bballoon\b.{0,100}\b(failed to inflate|would not inflate|did not inflate|failure|failed|deflat|inflate|rupture|ruptured|leak|leaked|leakage|detach|detached|burst|crack|cracked)\w*",
        r"\binflation port\b.{0,100}\b(crack|cracked|broken|broke|leak|leaked|leakage|failure|failed|damaged)\w*",
        r"\bretention balloon\b.{0,100}\b(issue|problem|failure|failed|leak|rupture|detached)\b",
    ],
    "separation of devices": [
        r"\bseparat(ed|ion)\b.{0,100}\b(device|component|part|tip|shaft|cannula|obturator|valve|spring|piece)\b",
        r"\b(device|component|part|tip|shaft|cannula|obturator|valve|spring|piece)\b.{0,100}\bseparat(ed|ion)\b",
        r"\b(detach|detached|disengaged|dislodged)\b.{0,100}\b(device|component|part|tip|shaft|cannula|obturator|valve|spring|piece)\b",
        r"\b(device|component|part|tip|shaft|cannula|obturator|valve|spring|piece)\b.{0,100}\b(detach|detached|disengaged|dislodged|fell off|came off)\b",
    ],
    "Small flake was found during surgery": [
        r"\bflake\b.{0,80}\b(found|noted|observed|seen|identified|discovered)\b.{0,80}\b(surgery|procedure|operation|intraoperative|intraoperatively)\b",
        r"\b(small particle|particle|fragment|shaving|shavings|debris)\b.{0,80}\b(found|noted|observed|seen|identified|discovered)\b.{0,80}\b(surgery|procedure|operation|intraoperative|intraoperatively)\b",
        r"\b(fragment|particle|flake|debris|shaving|shavings)\b.{0,100}\bintraoperative\b",
        r"\bintraoperative\b.{0,100}\b(fragment|particle|flake|debris|shaving|shavings)\b",
    ],
    "High level of endotoxin on product": [
        r"\bendotoxin\b.{0,80}\b(high|elevated|above limit|exceed|exceeded|exceeds|out of spec|oos|contamination|contaminated)\b",
        r"\bhigh level of endotoxin\b",
        r"\bpyrogen\b.{0,80}\b(contamination|contaminated|high|elevated|above limit|exceeded|out of spec|oos)\b",
    ],
    "Difficulty in removing the obturator": [
        r"\bdifficulty\b.{0,80}\b(remov|withdraw|release)\w*\b.{0,80}\bobturator\b",
        r"\bobturator\b.{0,100}\b(stuck|jammed|unable to remove|unable to withdraw|hard to remove|difficult to remove|would not remove|could not remove|would not release|would not click out|click out)\b",
        r"\bobturator\b.{0,100}\b(cannot|could not|unable|failed)\b.{0,80}\b(remove|withdraw|release)\b",
    ],
    "incorrect sterilization": [
        r"\bincorrect sterilization\b",
        r"\bsteriliz\w+\b.{0,100}\b(incorrect|inadequate|insufficient|failed|failure|deviation|non-sterile|non sterile|not sterile)\b",
        r"\b(non[- ]sterile|not sterile)\b",
        r"\bsterility\b.{0,100}\b(breach|failure|failed|compromised|concern)\b",
        r"\bsterile barrier\b.{0,100}\b(breach|compromised|failed|failure|damaged)\b",
    ],
    "Loosen or broken valve": [
        r"\bvalve\b.{0,80}\b(loose|loosened|dislodged|displaced|detached|broken|broke|cracked|crack|failure|failed|damaged|fell|fall)\b",
        r"\b(loose|broken|cracked|dislodged|detached|displaced)\b.{0,80}\bvalve\b",
        r"\bduckbill\b.{0,80}\bvalve\b.{0,80}\b(loose|loosened|dislodged|displaced|detached|broken|broke|cracked|failed|fell|fall)\b",
    ],
    "Trocar Leakage": [
        r"\btrocar\b.{0,100}\b(leak|leaks|leaked|leakage|gas leak|co2 leak|insufflation leak|loss of pneumoperitoneum)\b",
        r"\bport\b.{0,100}\b(leak|leaks|leaked|leakage|gas leak|co2 leak|insufflation leak)\b",
        r"\binsufflation\b.{0,100}\b(leak|leaks|leaked|leakage|loss)\b",
        r"\bco2\b.{0,100}\b(leak|leaks|leaked|leakage|loss)\b",
        r"\bloss of pneumoperitoneum\b",
        r"\bloss of visibility\b.{0,100}\b(leak|co2|insufflation|gas)\b",
    ],
}

ALLOWED_CATEGORIES = list(CATEGORY_PATTERNS.keys()) + ["Other"]


def classify_incident(text: object | None) -> str:
    """Classify incident text using deterministic category regexes in priority order."""
    normalized = normalize_text(text)
    for category, patterns in CATEGORY_PATTERNS.items():
        if any(re.search(pattern, normalized) for pattern in patterns):
            return category
    return "Other"

