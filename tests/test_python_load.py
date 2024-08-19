"""
Pytest unit tests of python_load_process
"""
from collections import Counter
import datetime
from unittest.mock import Mock, MagicMock, call
import sqlite3
from textwrap import dedent
from typing import Any

import pytest

import python_load_process

def test_latlon_conversion():
    assert pytest.approx(35.354721666666) == python_load_process.latlon_conversion('35°21.2833′N')
    assert pytest.approx(-82.527221666666) == python_load_process.latlon_conversion('082°31.6333′W')

def test_datetime_conversion():
    assert datetime.datetime(2024, 7, 31, 11, 12, 13, tzinfo=datetime.timezone.utc) == python_load_process.datetime_conversion("2024-07-31T11:12:13+00:00")

def mock_row(**override):
    return {
        'customer_name': 'mock customer',
        'device_name': 'mock device',
        'service_name': 'mock service',
        'start_date': '2022-07-10T11:12:13+00:00',
        'latitude': '35°21.2833′N',
        'longitude': '082°31.6333′W',
    } | override

bad_data_expected = [
    (mock_row(), False, 'valid'),
    (mock_row(customer_name=''), True, 'invalid'),
    (mock_row(device_name=''), True, 'invalid'),
    (mock_row(service_name=''), True, 'invalid'),
    # etc.
]

@pytest.mark.parametrize(
    "row_value, return_value, count_key", 
    bad_data_expected)
def test_bad_data(row_value, return_value, count_key):
    counts = Counter()
    assert python_load_process.bad_data(counts, row_value) == return_value
    assert dict(counts) == {count_key: 1}

bad_refererences_expected = [
    (mock_row(), False, 'valid references'),
    (mock_row(customer_name='not known'), True, 'invalid references'),
    (mock_row(device_name='not known'), True, 'invalid references'),
    (mock_row(service_name='really, not known'), True, 'invalid references'),
    # etc.
]

@pytest.fixture()
def mock_connection():
    """Stateful fixture. The cursor execute() bind variables defines the result value."""
    query_result = None

    def make_result(query, bind_vars):
        nonlocal query_result
        print(f"execute: {query}, {bind_vars}")
        if all(v.startswith("mock") for v in bind_vars.values()):
            query_result = ("mock_row",)
        else:
            query_result = None

    def return_result():
        nonlocal query_result
        return query_result

    mock_no_data_cursor = Mock(
        name="Cursor",
        execute=Mock(side_effect=make_result),
        fetchone=Mock(side_effect=return_result),
    )
    return Mock(
        name="Connection",
        cursor=Mock(return_value=mock_no_data_cursor),
    )

@pytest.mark.parametrize(
    "row_value, return_value, count_key",
    bad_refererences_expected)
def test_bad_references(row_value, return_value, count_key, mock_connection):
    counts = Counter()
    assert python_load_process.bad_references(counts, mock_connection, row_value) == return_value
    assert dict(counts) == {count_key: 1}


def test_activation_dataclass():
    a = python_load_process.Activation(**mock_row())
    assert a.customer_name == 'mock customer'
    assert a.device_name == 'mock device'
    assert a.service_name == 'mock service'
    assert a.start_date == '2022-07-10T11:12:13+00:00'
    assert a.latitude == '35°21.2833′N'
    assert a.longitude == '082°31.6333′W'
    assert a.lat_real == pytest.approx(35.354721666666)
    assert a.lon_real == pytest.approx(-82.527221666666)
    assert a.start_date_datetime == datetime.datetime(2022, 7, 10, 11, 12, 13, tzinfo=datetime.timezone.utc)

def test_activation_p_dataclass():
    a = python_load_process.Activation_p(**mock_row())
    assert a.customer_name == 'mock customer'
    assert a.device_name == 'mock device'
    assert a.service_name == 'mock service'
    assert a.start_date == '2022-07-10T11:12:13+00:00'
    assert a.latitude == '35°21.2833′N'
    assert a.longitude == '082°31.6333′W'
    assert a.lat_real == pytest.approx(35.354721666666)
    assert a.lon_real == pytest.approx(-82.527221666666)
    assert a.start_date_datetime == datetime.datetime(2022, 7, 10, 11, 12, 13, tzinfo=datetime.timezone.utc)

def test_transform_data_dict():
    counts = Counter()
    t = python_load_process.transform_data_dict(counts, mock_row())
    assert t['start_date_datetime'] == datetime.datetime(2022, 7, 10, 11, 12, 13, tzinfo=datetime.timezone.utc)
    assert t['lat_real'] == pytest.approx(35.354721666666)
    assert t['lon_real'] == pytest.approx(-82.527221666666)
    assert counts['transform'] == 1

def test_transform_data_dc():
    counts = Counter()
    t = python_load_process.transform_data_dc(counts, mock_row())
    assert t.start_date_datetime == datetime.datetime(2022, 7, 10, 11, 12, 13, tzinfo=datetime.timezone.utc)
    assert t.lat_real == pytest.approx(35.354721666666)
    assert t.lon_real == pytest.approx(-82.527221666666)
    assert counts['transform'] == 1

def test_transform_data_dcp():
    counts = Counter()
    t = python_load_process.transform_data_dcp(counts, mock_row())
    assert t.start_date_datetime == datetime.datetime(2022, 7, 10, 11, 12, 13, tzinfo=datetime.timezone.utc)
    assert t.lat_real == pytest.approx(35.354721666666)
    assert t.lon_real == pytest.approx(-82.527221666666)
    assert counts['transform'] == 1

def test_persist_data_dict(mock_connection):
    counts = Counter()
    t = python_load_process.transform_data_dict(counts, mock_row())
    p = python_load_process.persist_data_dict(counts, mock_connection, t)
    assert p['customer_device_id'] == 'mock_row'
    assert p['service_id'] == 'mock_row'
    assert p['start_date'] == datetime.datetime(2022, 7, 10, 11, 12, 13, tzinfo=datetime.timezone.utc)
    assert p['latitude'] == pytest.approx(35.354721666666)
    assert p['longitude'] == pytest.approx(-82.527221666666)
    assert counts['saved'] == 1


### Unit test -- isolated from database.

@pytest.fixture
def mock_good_rowid_cursor():
    return Mock(
        execute=Mock(),
        fetchone=Mock(return_value=["mock_row_id"]),
        close=Mock(),
    )

def test_fetch_customer_good(mock_good_rowid_cursor):
    mock_conn = Mock(
        cursor=Mock(return_value=mock_good_rowid_cursor)
    )
    row = python_load_process.fetch_customer_id(mock_conn, 'mock_name')
    assert row == "mock_row_id"
    mock_conn.cursor.assert_called_once()
    mock_good_rowid_cursor.execute.assert_called_once()
    args_0 = mock_good_rowid_cursor.execute.mock_calls[0].args
    assert args_0[0].strip().startswith("SELECT")
    assert "FROM customer" in args_0[0].strip()
    assert args_0[1] == {'customer_name': 'mock_name'}
    mock_good_rowid_cursor.fetchone.assert_called_once()
    mock_good_rowid_cursor.close.assert_called_once()
    mock_conn.cursor.commit.assert_not_called()

    # Cache Behavior: no second cursor operation.
    row = python_load_process.fetch_customer_id(mock_conn, 'mock_name')
    assert row == "mock_row_id"
    mock_conn.cursor.assert_called_once()


def test_fetch_service_good(mock_good_rowid_cursor):
    mock_conn = Mock(
        cursor=Mock(return_value=mock_good_rowid_cursor)
    )
    row = python_load_process.fetch_service_id(mock_conn, 'mock_name')
    assert row == "mock_row_id"
    mock_conn.cursor.assert_called_once()
    mock_good_rowid_cursor.execute.assert_called_once()
    args_0 = mock_good_rowid_cursor.execute.mock_calls[0].args
    assert args_0[0].strip().startswith("SELECT")
    assert "FROM service" in args_0[0].strip()
    assert args_0[1] == {'service_name': 'mock_name'}
    mock_good_rowid_cursor.fetchone.assert_called_once()
    mock_good_rowid_cursor.close.assert_called_once()
    mock_conn.cursor.commit.assert_not_called()

    # Cache Behavior: no second cursor operation.
    row = python_load_process.fetch_service_id(mock_conn, 'mock_name')
    assert row == "mock_row_id"
    mock_conn.cursor.assert_called_once()


def test_fetch_customer_device_good(mock_good_rowid_cursor):
    mock_conn = Mock(
        cursor=Mock(return_value=mock_good_rowid_cursor)
    )
    row = python_load_process.fetch_customer_device_id(mock_conn, 'mock_name', 'mock_device')
    assert row == "mock_row_id"
    mock_conn.cursor.assert_called_once()
    mock_good_rowid_cursor.execute.assert_called_once()
    args_0 = mock_good_rowid_cursor.execute.mock_calls[0].args
    assert args_0[0].strip().startswith("SELECT")
    assert "FROM customer_device" in args_0[0].strip()
    assert args_0[1] == {'customer_name': 'mock_name', 'device_name': 'mock_device'}
    mock_good_rowid_cursor.fetchone.assert_called_once()
    mock_good_rowid_cursor.close.assert_called_once()
    mock_conn.cursor.commit.assert_not_called()

    # Cache Behavior: no second cursor operation.
    row = python_load_process.fetch_customer_device_id(mock_conn, 'mock_name', 'mock_device')
    assert row == "mock_row_id"
    mock_conn.cursor.assert_called_once()


### Integration Test -- using database.
# Not all tables tested.

@pytest.fixture(scope="session")
def test_db_conn():
    connection = sqlite3.connect(":memory:")
    yield connection
    connection.close()
    
@pytest.fixture
def test_db_conn_schema(test_db_conn):
    cursor = test_db_conn.cursor()
    cursor.execute(dedent("""
        CREATE TABLE service(
            key INTEGER PRIMARY KEY, 
            service_name CHAR(16))
    """))
    cursor.execute(dedent("""
        INSERT INTO service(key, service_name) 
            VALUES(42, 'mock_service')
    """))
    # etc. for other tables.
    test_db_conn.commit()
    cursor.close()
    yield test_db_conn
    cursor = test_db_conn.cursor()
    cursor.execute(dedent("""
        DROP TABLE service
    """))
    # etc. for other tables.
    test_db_conn.commit()
    cursor.close()

def test_fetch_service_id(test_db_conn_schema):
    row_id = python_load_process.fetch_service_id(test_db_conn_schema, 'mock_service')
    assert row_id == 42
    failure = python_load_process.fetch_service_id(test_db_conn_schema, 'not a mock_service')
    assert failure is None
