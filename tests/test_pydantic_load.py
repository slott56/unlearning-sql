"""
Pytest unit tests of python_load_process

The focus is entirely on the ``Activation`` class.
"""
from collections import Counter
import datetime
from unittest.mock import Mock

from pydantic import ValidationError
import pytest

import pydantic_load_process

def mock_row(**override):
    return {
        'customer_name': 'mock customer',
        'device_name': 'mock device',
        'service_name': 'mock service',
        'start_date': '2022-07-10T11:12:13+00:00',
        'latitude': '35°21.2833′N',
        'longitude': '082°31.6333′W',
    } | override

@pytest.fixture()
def mock_db_connection():
    mock_cursor = Mock(
        fetchone=Mock(return_value=(1337,))
    )
    return Mock(
        cursor=Mock(return_value=mock_cursor)
    )

def test_activation_basics(mock_db_connection):
    row = mock_row()
    a = pydantic_load_process.Activation.model_validate_strings(row, context=mock_db_connection)
    final = a.model_dump()
    assert final["customer_device_id"] == 1337
    assert final["service_id"] == 1337
    assert final["start_date"] == datetime.datetime(2022, 7, 10, 11, 12, 13, tzinfo=datetime.timezone.utc)
    assert final["latitude"] == pytest.approx(35.354721666666)
    assert final["longitude"] == pytest.approx(-82.527221666666)
    assert set(final.keys()) == {"customer_device_id", "service_id", "start_date", "latitude", "longitude"}

bad_data_expected = [
    (mock_row(), False, None),
    (mock_row(customer_name=''), True, 'customer_name'),
    (mock_row(device_name=''), True, 'device_name'),
    (mock_row(service_name=''), True, 'service_name'),
    # etc.
]

@pytest.mark.parametrize(
    "row_value, is_bad, field",
    bad_data_expected)
def test_bad_data(row_value, is_bad, field, mock_db_connection):
    counts = Counter()
    try:
        a = pydantic_load_process.Activation.model_validate_strings(row_value, context=mock_db_connection)
    except ValidationError as error:
        assert is_bad
        errors = error.errors(include_url=False)
        assert len(errors) == 1
        assert errors[0]['loc'] == (field,)
