"""
Pytest integration tests of python_extract_1
"""
import csv
from pathlib import Path
import sqlite3 as db

import pytest

import python_extract_1
import sql_db_preparation
import python_load_process

def sqlite_import(activation_path, loaded_db):
    """
    The following shell command.

    ..  code-bash::

        sqlite3 data/unlearning_sql.db <<EOF
        .import -v --csv --skip 1 data/activation_load.csv CUSTOMER_DEVICE_SERVICE
        EOF
    """
    insert_sql = """
        INSERT INTO CUSTOMER_DEVICE_SERVICE(customer_device_id, service_id, start, latitude, longitude) 
        VALUES(:customer_device_id, :service_id, :start_date, :latitude, :longitude)
    """
    connection = db.connect(loaded_db)
    connection.row_factory = db.Row
    cursor = connection.cursor()
    with open(activation_path) as activation_file:
        source = csv.DictReader(activation_file)
        for row in source:
            cursor.execute(insert_sql, row)
    cursor.close()
    connection.commit()

@pytest.fixture(scope="session")
def loaded_db(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("tests") / "test.db"
    here = Path.cwd()
    schema_path = here / "activation_source.schema"
    data_path = here / "tests" / "activation_source.csv"
    sql_db_preparation.main(schema_path, db_path, [data_path])
    activation_path = here / "tests" / "activation_load.csv"
    sqlite_import(activation_path, db_path)
    return db_path


def test_service_names(loaded_db):
    connection = db.connect(loaded_db)
    connection.row_factory = db.Row
    mapping = python_extract_1.service_names(connection)
    assert len(mapping) == 58
    row_1 = mapping[1]
    assert list(row_1.keys()) == ['rowid', 'service_name']
    assert dict(row_1) == {'rowid': 1, 'service_name': 'Oxkxwnqrsrpemok'}


def test_cst_dev_svc_counts(loaded_db):
    connection = db.connect(loaded_db)
    connection.row_factory = db.Row
    counts = python_extract_1.cst_dev_svc_counts(connection)
    assert len(counts) == 55
    assert counts['Oxkxwnqrsrpemok'] == 1
    assert counts['Woddwpjsiyg'] == 1
