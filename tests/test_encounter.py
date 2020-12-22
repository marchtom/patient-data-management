from asyncio import AbstractEventLoop

import pytest


from . import run_encounters_test, get_data


@pytest.mark.asyncio
async def test_encounters_single_item_payload(
    database,
    loop: AbstractEventLoop,
) -> None:
    payload = [{
        "id": "2",
        "subject": {
            "reference": "Patient/patient-uuid-1",
        },
        "period": {
            "start": "2011-11-01T00:05:23+04:00",
            "end": "2011-11-04T00:05:23+04:00",
        },
        "type": [{
            "coding": [{
                "code": "code_value",
                "system": "system_value",
            }]
        }],
    }]

    await run_encounters_test(loop, payload)

    data = get_data("encounters")

    assert len(data) == 1
    assert data[0]["source_id"] == payload[0]["id"]
    assert data[0]["patient_id"] == 7
    assert str(data[0]["start_date"]) == "2011-10-31 20:05:23+00:00"
    assert str(data[0]["end_date"]) == "2011-11-03 20:05:23+00:00"
    assert data[0]["type_code"] == "code_value"
    assert data[0]["type_code_system"] == "system_value"


@pytest.mark.asyncio
async def test_encounters_single_invalid_patient(
    database,
    loop: AbstractEventLoop,
) -> None:
    payload = [{
        "id": "2",
        "subject": {
            "reference": "Patient/uuid-non-existing",
        },
        "period": {
            "start": "2011-11-01T00:05:23+04:00",
            "end": "2011-11-04T00:05:23+04:00",
        },
    }]

    await run_encounters_test(loop, payload)

    data = get_data("encounters")

    assert len(data) == 0


@pytest.mark.asyncio
async def test_encounters_multiple_items(
    database,
    loop: AbstractEventLoop,
) -> None:
    payload = [
        {
            "id": "source-1",
            "subject": {
                "reference": "Patient/uuid-non-existing",
            },
            "period": {
                "start": "2011-11-01T00:05:23+04:00",
                "end": "2011-11-04T00:05:23+04:00",
            },
        },
        {
            "id": "source-1",
            "subject": {
                "reference": "Patient/patient-uuid-1",
            },
            "period": {
                "start": "2011-11-01T00:05:23+04:00",
                "end": "2011-11-04T00:05:23+04:00",
            },
        },
        {
            "id": "source-1",
            "subject": {
                "reference": "Patient/patient-uuid-2",
            },
            "period": {
                "start": "2011-11-01T00:05:23+04:00",
                "end": "2011-11-04T00:05:23+04:00",
            },
        },
    ]

    await run_encounters_test(loop, payload)

    data = get_data("encounters")

    assert len(data) == 2
