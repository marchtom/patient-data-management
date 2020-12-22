from asyncio import AbstractEventLoop

import pytest


from . import run_procedures_test, get_data


@pytest.mark.asyncio
async def test_procedures_single_item_payload(
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
        "performedDateTime": "2020-10-01",
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
    }]

    await run_procedures_test(loop, payload)

    data = get_data("procedures")
    assert len(data) == 1
    assert data[0]["source_id"] == "21"
    assert data[0]["patient_id"] == 7
    assert data[0]["encounter_id"] == 3
    assert str(data[0]["procedure_date"]) == "2020-10-01"
    assert data[0]["type_code"] == "code_value"
    assert data[0]["type_code_system"] == "system_value"


@pytest.mark.asyncio
async def test_procedures_single_invalid_patient(
    database,
    loop: AbstractEventLoop,
) -> None:
    payload = [{
        "id": "21",
        "subject": {
            "reference": "Patient/uuid-non-existing",
        },
        "context": {
            "reference": "Encounter/encounter-uuid-1",
        },
        "performedDateTime": "2020-10-01",
        "code": {
            "coding": [{
                "code": "code_value",
                "system": "system_value",
            }]
        },
    }]

    await run_procedures_test(loop, payload)

    data = get_data("procedures")
    assert len(data) == 0


@pytest.mark.asyncio
async def test_procedures_single_invalid_encounter(
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
        "performedDateTime": "2020-10-01",
        "code": {
            "coding": [{
                "code": "code_value",
                "system": "system_value",
            }]
        },
    }]

    await run_procedures_test(loop, payload)

    data = get_data("procedures")
    assert len(data) == 1


@pytest.mark.asyncio
async def test_procedures_single_invalid_date(
    database,
    loop: AbstractEventLoop,
) -> None:
    payload = [{
        "id": "21",
        "subject": {
            "reference": "Patient/patient-uuid-1",
        },
        "performedDateTime": "2020-10-01afsfa",
        "code": {
            "coding": [{
                "code": "code_value",
                "system": "system_value",
            }]
        },
    }]

    await run_procedures_test(loop, payload)

    data = get_data("procedures")
    assert len(data) == 0


@pytest.mark.asyncio
async def test_procedures_single_invalid_code(
    database,
    loop: AbstractEventLoop,
) -> None:
    payload = [{
        "id": "21",
        "subject": {
            "reference": "Patient/patient-uuid-1",
        },
        "performedDateTime": "2020-10-01",
        "code": {
            "coding": [{
                "key1": "code_value",
                "key2": "system_value",
            }]
        },
    }]

    await run_procedures_test(loop, payload)

    data = get_data("procedures")
    assert len(data) == 0


@pytest.mark.asyncio
async def test_procedures_single_alternative_date_source(
    database,
    loop: AbstractEventLoop,
) -> None:
    payload = [{
        "id": "21",
        "subject": {
            "reference": "Patient/patient-uuid-1",
        },
        "performedPeriod": {
            "start": "2010-12-12"
        },
        "code": {
            "coding": [{
                "code": "code_value",
                "system": "system_value",
            }]
        },
    }]

    await run_procedures_test(loop, payload)

    data = get_data("procedures")
    assert len(data) == 1
    assert str(data[0]["procedure_date"]) == "2010-12-12"
