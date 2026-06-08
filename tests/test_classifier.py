from pipeline.classifier import classify_custom_incident_details, classify_incident, classify_incident_details


def test_classifies_broken_obturator_tip() -> None:
    text = "The trocar obturator tip fractured during the procedure."
    assert classify_incident(text) == "Broken Obturator tip"


def test_classifies_trocar_leakage() -> None:
    text = "The trocar had CO2 leakage and caused loss of pneumoperitoneum."
    assert classify_incident(text) == "Trocar Leakage"


def test_classifies_other_when_no_pattern_matches() -> None:
    assert classify_incident("Routine use with no reported product malfunction.") == "Unrelated / insufficient detail"


def test_phrase_priority_classifies_seal_before_generic_leakage() -> None:
    text = "The trocar seal broke and the port leaked during surgery."
    assert classify_incident(text) == "seal breaking"


def test_negation_prevents_false_leakage_match() -> None:
    text = "The user reported no leak was observed during routine use."
    result = classify_incident_details(text)
    assert result.category != "Trocar Leakage"
    assert result.confidence == "Low"


def test_packaging_issue_is_separate_from_device_breakage() -> None:
    text = "The sterile pouch packaging was torn before use; device was not used."
    assert classify_incident(text) == "Sterility / packaging issue"


def test_unclassified_mechanical_issue_reduces_other_bucket() -> None:
    text = "The device malfunctioned during the procedure but the exact component was not identified."
    assert classify_incident(text) == "Unclassified mechanical issue"


def test_classification_confidence_high_for_specific_match() -> None:
    result = classify_incident_details("The retention balloon ruptured during placement.")
    assert result.category == "Balloon issue"
    assert result.confidence == "High"
    assert result.reason


def test_custom_classifier_combines_component_and_accident_terms() -> None:
    result = classify_custom_incident_details(
        "The catheter hub cracked during insertion.",
        components=["catheter hub"],
        accident_terms=["cracked"],
    )

    assert result.category == "Catheter Hub - Cracked issue"
    assert result.confidence == "High"


def test_custom_classifier_infers_accident_from_component_context() -> None:
    result = classify_custom_incident_details(
        "The pump housing fractured during setup.",
        components=["pump housing"],
        accident_terms=[],
    )

    assert result.category == "Pump Housing - crack/break issue"
    assert result.confidence == "Medium"


def test_custom_classifier_accident_only_category() -> None:
    result = classify_custom_incident_details(
        "The device leaked during use.",
        components=["catheter hub"],
        accident_terms=["leaked"],
    )

    assert result.category == "Leaked issue"
    assert result.confidence == "Medium"
