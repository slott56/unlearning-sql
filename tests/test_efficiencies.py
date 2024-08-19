"""
Tests for the efficiencies examples.
"""
from unittest.mock import Mock, sentinel

import pytest

import efficiencies

@pytest.fixture()
def db_connection():
    mock_cursor = Mock(
        fetchone=Mock(return_value=(sentinel.VALUE,))
    )
    return Mock(
        cursor=Mock(return_value=mock_cursor)
    )

def test_fetch_a_thing(db_connection):
    result = efficiencies.fetch_a_thing(db_connection, sentinel.THE_KEY)
    assert result == (sentinel.VALUE,)
    args_0 = db_connection.cursor.return_value.execute.mock_calls[0].args
    assert args_0 == ("\nSELECT * FROM table WHERE key = :bind\n", {"bind": sentinel.THE_KEY})

def test_ServiceNameMapping(db_connection):
    service_name_mapping = efficiencies.ServiceNameMapping(db_connection)
    cst_dev_svc = {'service_id': sentinel.THE_KEY, }  # And other values
    mapped_row = service_name_mapping[cst_dev_svc['service_id']]
    assert mapped_row == (sentinel.VALUE,)
    args_0 = db_connection.cursor.return_value.execute.mock_calls[0].args
    assert args_0 == ("\nSELECT *\nFROM service\nWHERE service_id = :service_id\n", {"service_id": sentinel.THE_KEY})

@pytest.fixture()
def db_connection2():
    rows = [
        {"key": sentinel.KEY1, "this": 2, "that": 3},
        {"key": sentinel.KEY2, "this": 5, "that": 7},
    ]
    mock_cursor_query = Mock(
        name="query",
        fetchall=Mock(return_value=iter(rows))
    )
    lookup1 = {"value": sentinel.VALUE1}
    lookup2 = {"value": sentinel.VALUE2}
    mock_cursor_lookup = Mock(
        name="lookup",
        fetchone=Mock(side_effect=[lookup1, lookup2])
    )
    return Mock(
        name="connection",
        cursor=Mock(side_effect=[mock_cursor_lookup, mock_cursor_query])
    )

def test_full_process(db_connection2):
    results = list(efficiencies.full_process(db_connection2))
    assert results == [
        {'key': sentinel.KEY1, 'this': 2, 'that': 3, 'something': sentinel.VALUE1, 'computed': 7},
        {'key': sentinel.KEY2, 'this': 5, 'that': 7, 'something': sentinel.VALUE2, 'computed': 17}
    ]
