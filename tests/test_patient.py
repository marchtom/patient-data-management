from asyncio import AbstractEventLoop

import pytest

from app.tables.patients import ETHNICITY_CODE_URL, RACE_CODE_URL


from . import run_patients_test, get_data


@pytest.mark.asyncio
async def test_patients_single_item_basic_payload(
    database,
    loop: AbstractEventLoop,
) -> None:
    payload = [{"id": "2"}]

    await run_patients_test(loop, payload)

    data = get_data("patients")

    assert len(data) == 1
    assert data[0]["source_id"] == payload[0]["id"]


@pytest.mark.asyncio
async def test_patients_invalid_payload_missing_id(
    database,
    loop: AbstractEventLoop,
) -> None:
    payload = [{"invalid_key": "2"}]

    await run_patients_test(loop, payload)

    data = get_data("patients")

    assert len(data) == 0


@pytest.mark.asyncio
async def test_patients_invalid_optional_payload(
    database,
    loop: AbstractEventLoop,
) -> None:
    payload = [{
        "id": "2", "birthDate": "1a999-01+0h1",
        "addres": "invalid address",
        "extension": {"key": "val"},
    }]

    await run_patients_test(loop, payload)

    data = get_data("patients")

    assert len(data) == 1
    assert data[0]["source_id"] == payload[0]["id"]
    assert data[0]["birth_date"] is None
    assert data[0]["country"] is None
    assert data[0]["race_code"] is None


@pytest.mark.asyncio
async def test_patients_single_item_complex_payload(
    database,
    loop: AbstractEventLoop,
) -> None:
    country = "UK"
    race_code = "race_code_value"
    race_code_system = "race_code_system_value"
    ethnicity_code = "ethnicity_code_value"
    ethnicity_code_system = "ethnicity_code_system_value"

    payload = [{
        "id": "2", "birthDate": "1999-01-01",
        "address": [{"country": country}, {"country": "Invalid"}],
        "extension": [
            {
                "url": "http:invalid.com",
                "valueCodeableConcept": {
                    "coding": [
                        {"code": "11", "system": "11_system"}
                    ]
                }
            },
            {
                "url": ETHNICITY_CODE_URL,
                "valueCodeableConcept": {
                    "coding": [
                        {"code": ethnicity_code, "system": ethnicity_code_system}
                    ]
                }
            },
            {
                "url": RACE_CODE_URL,
                "valueCodeableConcept": {
                    "coding": [
                        {"code": race_code, "system": race_code_system}
                    ]
                }
            }
        ]
    }]

    await run_patients_test(loop, payload)

    data = get_data("patients")

    assert len(data) == 1
    assert data[0]["source_id"] == payload[0]["id"]
    assert data[0]["birth_date"].strftime('%Y-%m-%d') == payload[0]["birthDate"]
    assert data[0]["country"] == country
    assert data[0]["race_code"] == race_code
    assert data[0]["race_code_system"] == race_code_system
    assert data[0]["ethnicity_code"] == ethnicity_code
    assert data[0]["ethnicity_code_system"] == ethnicity_code_system


@pytest.mark.asyncio
async def test_patients_multiple_items_basic_payload(
    database,
    loop: AbstractEventLoop,
) -> None:
    payload = [{"id": "2"}, {"id": "uuid-abcd12"}, {"id": 9724}]

    await run_patients_test(loop, payload)

    data = get_data("patients")

    assert len(data) == len(payload)
    payload_source_ids = [item["id"] for item in payload]

    for row in data:
        assert data[0]["source_id"] in payload_source_ids


@pytest.mark.asyncio
async def test_patients_multiple_items_single_invalid_id(
    database,
    loop: AbstractEventLoop,
) -> None:
    payload = [{"id": "2jas"}, {"not_id": "1111-uuid-abcd12"}, {"id": 900724}]

    await run_patients_test(loop, payload)

    data = get_data("patients")

    payload_source_ids = [item.get("id") for item in payload if item.get("id")]
    assert len(data) == len(payload_source_ids)

    for row in data:
        assert data[0]["source_id"] in payload_source_ids
