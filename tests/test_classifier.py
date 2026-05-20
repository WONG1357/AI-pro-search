from pipeline.classifier import classify_incident


def test_classifies_broken_obturator_tip() -> None:
    text = "The trocar obturator tip fractured during the procedure."
    assert classify_incident(text) == "Broken Obturator tip"


def test_classifies_trocar_leakage() -> None:
    text = "The trocar had CO2 leakage and caused loss of pneumoperitoneum."
    assert classify_incident(text) == "Trocar Leakage"


def test_classifies_other_when_no_pattern_matches() -> None:
    assert classify_incident("Routine use with no reported product malfunction.") == "Other"

