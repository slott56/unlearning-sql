"""
Pytest unit tests of sql_load_process.

Low-level isolated tests with mock DB.

High-level integration tests with temp DB.
"""
from collections.abc import Iterator
from unittest.mock import Mock, sentinel, call
import sqlite3 as db

import pytest

import sql_load_process

### Unit Tests -- In Isolation

@pytest.fixture()
def mock_db() -> Mock:
    return Mock(
        name="Connection",
        cursor=Mock(
            return_value=Mock(
                name="cursor",
                execute=Mock(),
                close=Mock(),
                rowcount=1,
                fetchone=Mock(return_value=(sentinel.VALUE,)),
                fetchall=Mock(return_value=[sentinel.ROW]),
            )
        ),
        commit=Mock()
    )

@pytest.fixture()
def mock_source() -> Iterator[Mock]:
    rows = [
        sentinel.CSV_DICT
    ]
    return rows


def test_make_activation(mock_db: Mock) -> None:
    sql_load_process.make_activation(mock_db)
    mock_db.cursor.assert_called_once_with()
    mock_db.cursor.return_value.execute.assert_called_once()
    args = mock_db.cursor.return_value.execute.mock_calls[0].args
    assert args[0].strip().startswith("CREATE TABLE IF NOT EXISTS activation")
    mock_db.cursor.return_value.close.assert_called_once_with()
    mock_db.commit.assert_called_once_with()


def test_clear_activation(mock_db: Mock, capsys) -> None:
    sql_load_process.clear_activation(mock_db)
    mock_db.cursor.assert_called_once_with()
    mock_db.cursor.return_value.execute.assert_called_once()
    args = mock_db.cursor.return_value.execute.mock_calls[0].args
    assert args[0].strip().startswith("DELETE")
    mock_db.cursor.return_value.close.assert_called_once_with()
    mock_db.commit.assert_called_once_with()
    out, err = capsys.readouterr()
    assert out == "deleted 1 old rows\n"

def test_load_activation(mock_db: Mock, mock_source: Mock) -> None:
    sql_load_process.load_activation(mock_db, mock_source)
    mock_db.cursor.assert_called_once_with()
    mock_db.cursor.return_value.execute.assert_called_once()
    args = mock_db.cursor.return_value.execute.mock_calls[0].args
    assert args[0].strip().startswith("INSERT")
    assert args[1] is sentinel.CSV_DICT
    mock_db.cursor.return_value.close.assert_called_once_with()
    mock_db.commit.assert_called_once_with()

def test_activation_count(mock_db: Mock) -> None:
    count = sql_load_process.activation_count(mock_db)
    assert count == sentinel.VALUE
    mock_db.cursor.assert_called_once_with()
    mock_db.cursor.return_value.execute.assert_called_once()
    mock_db.cursor.return_value.fetchone.assert_called_once()
    args = mock_db.cursor.return_value.execute.mock_calls[0].args
    assert args[0].strip().startswith("SELECT")
    mock_db.cursor.return_value.close.assert_called_once_with()
    mock_db.commit.assert_called_once_with()


def test_activation_reject_bad_data_1(mock_db: Mock, capsys) -> None:
    sql_load_process.activation_reject_bad_data_1(mock_db)
    mock_db.cursor.assert_called_once_with()
    mock_db.cursor.return_value.execute.assert_called_once()
    args = mock_db.cursor.return_value.execute.mock_calls[0].args
    assert args[0].strip().startswith("DELETE")
    mock_db.cursor.return_value.close.assert_called_once_with()
    mock_db.commit.assert_called_once_with()
    out, err = capsys.readouterr()
    assert out == "removed 1 bad rows\n"

def test_activation_reject_bad_data_2(mock_db: Mock, capsys) -> None:
    sql_load_process.activation_reject_bad_data_2(mock_db)
    mock_db.cursor.assert_called_once_with()
    mock_db.cursor.return_value.execute.assert_called_once()
    args = mock_db.cursor.return_value.execute.mock_calls[0].args
    assert args[0].strip().startswith("DELETE")
    mock_db.cursor.return_value.close.assert_called_once_with()
    mock_db.commit.assert_called_once_with()
    out, err = capsys.readouterr()
    assert out == "removed 1 bad rows\n"

def test_activation_locate_disconnected_data(mock_db: Mock, capsys) -> None:
    sql_load_process.activation_locate_disconnected_data(mock_db)
    mock_db.cursor.assert_called_once_with()
    mock_db.cursor.return_value.execute.assert_called_once()
    args = mock_db.cursor.return_value.execute.mock_calls[0].args
    assert args[0].strip().startswith("SELECT")
    mock_db.cursor.return_value.fetchall.assert_called_once()
    mock_db.cursor.return_value.close.assert_called_once_with()
    mock_db.commit.assert_called_once_with()
    out, err = capsys.readouterr()
    assert out == "found missing connections: len(missing)=1\n"

def test_activation_transformation(mock_db: Mock, capsys) -> None:
    sql_load_process.activation_transformation(mock_db)
    mock_db.cursor.assert_called_once_with()
    mock_db.cursor.return_value.execute.assert_called_once()
    args = mock_db.cursor.return_value.execute.mock_calls[0].args
    assert args[0].strip().startswith("UPDATE")
    mock_db.cursor.return_value.close.assert_called_once_with()
    mock_db.commit.assert_called_once_with()
    out, err = capsys.readouterr()
    assert out == "updated 1 rows\n"

def test_persist(mock_db: Mock, capsys) -> None:
    sql_load_process.persist(mock_db)
    mock_db.cursor.assert_called_once_with()
    mock_db.cursor.return_value.execute.mock_calls == [call(), call()]
    args_0 = mock_db.cursor.return_value.execute.mock_calls[0].args
    assert args_0[0].strip().startswith("INSERT")
    args_1 = mock_db.cursor.return_value.execute.mock_calls[1].args
    assert args_1[0].strip().startswith("SELECT")
    mock_db.cursor.return_value.close.assert_called_once_with()
    mock_db.commit.assert_called_once_with()
    out, err = capsys.readouterr()
    assert out == "loaded 1 final rows\nsentinel.ROW\n"

### Integration Tests -- With Temp Database

@pytest.fixture()
def db_fixture(tmp_path):
    db_path = tmp_path / "test.db"
    connection = db.connect(db_path)
    yield connection
    connection.close()
    db_path.unlink()

def test_i_make_activation(db_fixture):
    sql_load_process.make_activation(db_fixture)
    cursor = db_fixture.cursor()
    cursor.execute("SELECT * FROM activation")
    rows = list(cursor.fetchall())
    assert rows == []
    cursor.close()

def test_i_clear_activation(db_fixture, capsys):
    sql_load_process.make_activation(db_fixture)
    sql_load_process.clear_activation(db_fixture)
    cursor = db_fixture.cursor()
    cursor.execute("SELECT * FROM activation")
    rows = list(cursor.fetchall())
    assert rows == []
    cursor.close()
    out, err = capsys.readouterr()
    assert out == "deleted 0 old rows\n"

@pytest.fixture()
def integration_source():
    rows = [
        {
            "customer_name": "Customer",
            "device_name": "Device",
            "service_name": "Service",
            "start_date": "2024-07-30T09:19:00+00:00",
            "latitude": "32°44.16′N",
            "longitude": "097°27.01′W",
        }
    ]
    return rows


def test_i_load_activation(db_fixture, integration_source, capsys):
    sql_load_process.make_activation(db_fixture)
    sql_load_process.clear_activation(db_fixture)
    sql_load_process.load_activation(db_fixture, integration_source)
    cursor = db_fixture.cursor()
    cursor.execute("SELECT * FROM activation")
    rows = list(cursor.fetchall())
    assert rows == [
        ('Customer', 'Device', 'Service', '2024-07-30T09:19:00+00:00', '32°44.16′N', '097°27.01′W', None, None, None)
    ]
    cursor.close()
    out, err = capsys.readouterr()
    assert out == "deleted 0 old rows\ninserted 1 new rows\n"

# Option 1:  Test **all** the functions in isolation.
# Option 2:  Rely on the acceptance test data to touch all the various options and combinations.
