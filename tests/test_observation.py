from asyncio import AbstractEventLoop

import pytest


from . import run_observations_test, get_data


@pytest.mark.asyncio
async def test_observations_single_item_payload(
    database,
    loop: AbstractEventLoop,
) -> None:
    payload = [{
        "id": "21",
        "subject": {
            "reference": "Patient/patient-uuid-1",
        },
        "context": {
            "reference": "Encounter/encounter-uuid-1",
        },
        "effectiveDateTime": "2020-10-01",
        "code": {
            "coding": [
                {
                    "code": "code_value",
                    "system": "system_value",
                },
                {
                    "code": "code_value2",
                    "system": "system_value2",
                },
            ]
        },
        "valueQuantity": {
            "value": 75.3,
            "unit": "mm",
            "system": "metric",
        }
    }]

    await run_observations_test(loop, payload)

    data = get_data("observations")

    assert len(data) == 1
    assert data[0]["source_id"] == "21"
    assert data[0]["patient_id"] == 7
    assert data[0]["encounter_id"] == 3
    assert str(data[0]["observation_date"]) == "2020-10-01"
    assert data[0]["type_code"] == "code_value"
    assert data[0]["type_code_system"] == "system_value"
    assert float(data[0]["value"]) == 75.3
    assert data[0]["unit_code"] == "mm"
    assert data[0]["unit_code_system"] == "metric"


@pytest.mark.asyncio
async def test_observations_single_item_payload_with_components(
    database,
    loop: AbstractEventLoop,
) -> None:
    payload = [{
        "id": "21",
        "subject": {
            "reference": "Patient/patient-uuid-1",
        },
        "context": {
            "reference": "Encounter/encounter-uuid-1",
        },
        "effectiveDateTime": "2020-10-01",
        "component": [
            {
                "code": {
                    "coding": [
                        {
                            "code": "code_value0",
                            "system": "system_value0",
                        },
                        {
                            "code": "code_value2",
                            "system": "system_value2",
                        },
                    ]
                },
                "valueQuantity": {
                    "value": 75.3,
                    "unit": "mm",
                    "system": "metric",
                }
            },
            {
                "code": {
                    "coding": [
                        {
                            "code": "code_value1",
                            "system": "system_value1",
                        },
                        {
                            "code": "code_value3",
                            "system": "system_value3",
                        },
                    ]
                },
                "valueQuantity": {
                    "value": 0.003,
                    "unit": "km",
                    "system": "metric0",
                }
            },
        ]
    }]

    await run_observations_test(loop, payload)

    data = get_data("observations")

    assert len(data) == 2
    assert data[0]["source_id"] == data[1]["source_id"]
    assert data[0]["patient_id"] == data[1]["patient_id"]
    assert data[0]["encounter_id"] == data[1]["encounter_id"]
    assert data[0]["type_code"] == "code_value0"
    assert data[0]["type_code_system"] == "system_value0"
    assert float(data[0]["value"]) == 75.3
    assert data[0]["unit_code"] == "mm"
    assert data[0]["unit_code_system"] == "metric"
    assert data[1]["type_code"] == "code_value1"
    assert data[1]["type_code_system"] == "system_value1"
    assert float(data[1]["value"]) == 0.003
    assert data[1]["unit_code"] == "km"
    assert data[1]["unit_code_system"] == "metric0"


@pytest.mark.asyncio
async def test_observations_single_invalid_patient(
    database,
    loop: AbstractEventLoop,
) -> None:
    payload = [{
        "id": "21",
        "subject": {
            "reference": "Patient/patient-non-existing",
        },
        "context": {
            "reference": "Encounter/encounter-uuid-1",
        },
        "effectiveDateTime": "2020-10-01",
        "code": {
            "coding": [
                {
                    "code": "code_value",
                    "system": "system_value",
                },
                {
                    "code": "code_value2",
                    "system": "system_value2",
                },
            ]
        },
        "valueQuantity": {
            "value": 75.3,
            "unit": "mm",
            "system": "metric",
        }
    }]

    await run_observations_test(loop, payload)

    data = get_data("observations")

    assert len(data) == 0


@pytest.mark.asyncio
async def test_observations_single_invalid_encounter(
    database,
    loop: AbstractEventLoop,
) -> None:
    payload = [{
        "id": "21",
        "subject": {
            "reference": "Patient/patient-uuid-1",
        },
        "context": {
            "reference": "Encounter/encounter-non-existing",
        },
        "effectiveDateTime": "2020-10-01",
        "code": {
            "coding": [
                {
                    "code": "code_value",
                    "system": "system_value",
                },
                {
                    "code": "code_value2",
                    "system": "system_value2",
                },
            ]
        },
        "valueQuantity": {
            "value": 75.3,
            "unit": "mm",
            "system": "metric",
        }
    }]

    await run_observations_test(loop, payload)

    data = get_data("observations")

    assert len(data) == 1
    assert data[0]["encounter_id"] is None
