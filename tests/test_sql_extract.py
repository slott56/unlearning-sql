"""
Pytest integration tests of sql_extract_process
"""
from pathlib import Path
import sqlite3 as db

import pytest

import sql_extract_process
import sql_db_preparation


@pytest.fixture(scope="session")
def loaded_db(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("tests") / "test.db"
    here = Path.cwd()
    schema_path = here / "activation_source.schema"
    data_path = here / "tests" / "activation_source.csv"
    sql_db_preparation.main(schema_path, db_path, [data_path])
    return db_path

def test_query_1(loaded_db, capsys):
    connection = db.connect(loaded_db)
    connection.row_factory = db.Row
    sql_extract_process.query_1(connection)
    out, err = capsys.readouterr()
    report = out.splitlines()
    assert len(report) == 58
    assert report[0] == "{'customer_name': 'Clrzawzksbvrjnwvgfygwwmqzcudi', 'device_name': 'Fjsonxkmtecqo'}"
    assert report[-1] == "{'customer_name': 'Ahxtpyxcpvcmdvxghqneyrtukoucvjhhqeotmxdqnhpobuaky', 'device_name': 'Zypa'}"

def test_query_2(loaded_db, capsys):
    connection = db.connect(loaded_db)
    connection.row_factory = db.Row
    sql_extract_process.query_2(connection)
    out, err = capsys.readouterr()
    report = out.splitlines()
    assert len(report) == 116
    assert report[0] == "{'customer_name': 'Clrzawzksbvrjnwvgfygwwmqzcudi'}"
    assert report[-1] == "{'customer_name': 'Woddwpjsiyg'}"

def test_query_3(loaded_db, capsys):
    connection = db.connect(loaded_db)
    connection.row_factory = db.Row

    cursor = connection.cursor()
    cursor.execute("INSERT INTO customer(customer_name) VALUES('Exceptional')")
    cursor.close()
    connection.commit()

    sql_extract_process.query_3(connection)
    out, err = capsys.readouterr()
    report = out.splitlines()
    assert len(report) == 59
    assert report[0] == "{'customer_name': 'Ahxtpyxcpvcmdvxghqneyrtukoucvjhhqeotmxdqnhpobuaky', 'device_name': 'Zypa'}"
    assert report[-1] == "{'customer_name': 'Yoqfbophwznzsrrspvizramelishodncrsnfmubjdiblgsritc', 'device_name': 'Fjafnvat'}"
    assert any('Exceptional' in line for line in report)

def test_query_group_by(loaded_db, capsys):
    connection = db.connect(loaded_db)
    connection.row_factory = db.Row
    sql_extract_process.query_group_by(connection)
    out, err = capsys.readouterr()
    report = out.splitlines()
    assert len(report) == 57
    assert report[0] == "{'device_type_name': 'Asjewi', 'count(*)': 1}"
    assert report[-1] == "{'device_type_name': 'Ypq', 'count(*)': 1}"


def test_query_group_by_having(loaded_db, capsys):
    connection = db.connect(loaded_db)
    connection.row_factory = db.Row
    sql_extract_process.query_group_by_having(connection)
    out, err = capsys.readouterr()
    report = out.splitlines()
    assert len(report) == 1
    assert report[0] == "{'device_type_name': 'F', 'count(*)': 2}"
