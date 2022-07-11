"""
Pytest unit tests of python_load_process
"""
from unittest.mock import Mock, MagicMock, call
import sqlite3
import python_load_process
import pytest
from textwrap import dedent

def mock_row(**override):
    return {
        'customer_name': 'mock customer',
        'device_name': 'mock device',
        'service_name': 'mock service',
        'start_date': '2022-07-10T11:12:13Z',
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
    counts = MagicMock()
    assert python_load_process.bad_data(counts, row_value) == return_value
    counts.__getitem__.assert_called_once_with(count_key)
    counts.__setitem__.assert_called_once_with(count_key, counts.__getitem__.return_value.__iadd__.return_value)

@pytest.fixture
def good_rowid_cursor():
    return Mock(
        execute=Mock(),
        fetchone=Mock(return_value=["mock_row_id"]),
        close=Mock(),
    )

def test_fetch_customer_good(good_rowid_cursor):
    mock_conn = Mock(
        cursor=Mock(return_value=good_rowid_cursor)
    )
    row = python_load_process.fetch_customer_id(mock_conn, 'mock_name')
    assert row == "mock_row_id"
    mock_conn.cursor.assert_called_once()
    good_rowid_cursor.execute.assert_called_once()
    good_rowid_cursor.fetchone.assert_called_once()
    good_rowid_cursor.close.assert_called_once()

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
    test_db_conn.commit()
    cursor.close()
    yield test_db_conn
    cursor = test_db_conn.cursor()
    cursor.execute(dedent("""
        DROP TABLE service
    """))
    test_db_conn.commit()
    cursor.close()

def test_fetch_service_id(test_db_conn_schema):
    row_id = python_load_process.fetch_service_id(test_db_conn_schema, 'mock_service')
    assert row_id == 42
    failure = python_load_process.fetch_service_id(test_db_conn_schema, 'not a mock_service')
    assert failure is None
