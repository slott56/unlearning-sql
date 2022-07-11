"""
Prepare the database prior to running a load.
"""

from collections import Counter, defaultdict
import csv
import json
from jsonschema import Draft202012Validator, FormatChecker
import re
import sqlite3 as db
from pathlib import Path
from textwrap import dedent

def make_tables(connection):
    create_customer_table = dedent("""
        CREATE TABLE IF NOT EXISTS customer(
            customer_name CHAR(64)
        )
        """)
    create_customer_device_table = dedent("""
        CREATE TABLE IF NOT EXISTS customer_device(
            customer_id INTEGER REFERENCES customer(rowid),
            device_name CHAR(64)
        )
        """)
    create_device_type_table = dedent("""
        CREATE TABLE IF NOT EXISTS device_type(
            device_type_name CHAR(64)
        )
        """)
    create_service_table = dedent("""
        CREATE TABLE IF NOT EXISTS service(
            service_name CHAR(64)
        )
        """)
    create_customer_device_service_assoc = dedent("""
        CREATE TABLE IF NOT EXISTS customer_device_service(
            customer_device_id INTEGER 
                REFERENCES customer_device(rowid),
            service_id INTGER 
                REFERENCES service(rowid),
            start DATETIME,
            latitude REAL,
            longitude REAL
        )
        """)
    cursor = connection.cursor()
    for table_sql in (
            create_customer_table, 
            create_customer_device_table,
            create_device_type_table,
            create_service_table,
            create_customer_device_service_assoc,
    ):
        print(table_sql)
        cursor.execute(table_sql)
        print("completed")
        connection.commit()
    cursor.close()

def survey(source, schema):
    """Only valid values collected
    For most columns this is non-empty.
    For a few others, there's a regex validation rule.
    """
    formats = FormatChecker()
    
    @formats.checks("latitude")
    def check_latitude(data: str) -> bool:
        return re.match(r"\d{2}°\d\d\.\d{4}′[NS]", data) is not None

    @formats.checks("longitude")
    def check_longitude(data: str) -> bool:
        return re.match(r"\d{3}°\d\d\.\d{4}′[EW]", data) is not None

    validator = Draft202012Validator(schema, format_checker=formats)
    domains = defaultdict(Counter)
    for row in source:
        if validator.is_valid(row):
            for col in row:
                domains[col][row[col]] += 1
            for col_group in [('customer_name', 'device_name')]:
                group = tuple(row[c] for c in col_group)
                domains[col_group][group] += 1
        else:
            e = ", ".join(f"{e.message} in field {e.json_path}" for e in validator.iter_errors(row))
            print(f"Errors {e} found in {row=}")
    return domains
    
def load_customer(connection, names):
    """Load customer, skipping one value."""
    remove_customer_rows = dedent("""
        DELETE FROM customer
    """)
    insert_customer_row = dedent("""
        INSERT INTO customer
            VALUES(:customer_name)
    """)
    cursor = connection.cursor()
    cursor.execute(remove_customer_rows)
    print(f"deleted {cursor.rowcount} customer rows")
    connection.commit()

    inserts = 0
    for customer_name in names[1:]:
        cursor.execute(insert_customer_row, {'customer_name': customer_name})
        inserts += cursor.rowcount
    connection.commit()
    print(f"inserted {inserts} customer rows")
    cursor.close()

def load_customer_device(connection, customer_device):
    """Load customer_device, skipping one of the pairs."""
    remove_customer_device_rows = dedent("""
        DELETE FROM customer_device
    """)
    get_customer_id = dedent("""
        SELECT rowid FROM customer
        WHERE customer_name = :customer_name
    """)
    insert_customer_device_row = dedent("""
        INSERT INTO customer_device
            VALUES(:customer_id, :device_name)
    """)
    
    cursor = connection.cursor()
    cursor.execute(remove_customer_device_rows)
    print(f"deleted {cursor.rowcount} customer_device rows")
    connection.commit()
    
    inserts = 0
    for customer_name, device_name  in customer_device[1:]:
        cursor.execute(get_customer_id, {'customer_name': customer_name})
        customer_id, = cursor.fetchone()        
        cursor.execute(
            insert_customer_device_row, 
            {'device_name': device_name, 'customer_id': customer_id})
        inserts += cursor.rowcount
    connection.commit()
    print(f"inserted {inserts} customer rows")
    cursor.close()

def load_service(connection, names):
    """Load service, skipping one value."""
    remove_service_rows = dedent("""
        DELETE FROM service
    """)
    insert_service_row = dedent("""
        INSERT INTO service
            VALUES(:service_name)
    """)
    cursor = connection.cursor()
    cursor.execute(remove_service_rows)
    print(f"deleted {cursor.rowcount} service rows")
    connection.commit()

    inserts = 0
    for service_name in names[1:]:
        cursor.execute(insert_service_row, {'service_name': service_name})
        inserts += cursor.rowcount
    connection.commit()
    print(f"inserted {inserts} service rows")
    cursor.close()

def main(
        schema_path=Path("activation_source.schema.json"),
        source_path=Path("activation_source.data"),
        database_connect="unlearning_sql.db"):
    
    with schema_path.open() as schema_file:
        schema = json.load(schema_file)
    Draft202012Validator.check_schema(schema)
    
    with source_path.open() as source_file:
        reader = csv.DictReader(source_file)
        domains = survey(reader, schema)

    connection = db.connect(database_connect)
    make_tables(connection)
    for name in domains:
        print(name, list(domains[name].keys()))
    customer_names = list(domains['customer_name'].keys())
    device_names = list(domains['device_name'].keys())
    customer_device = list(domains[('customer_name', 'device_name')].keys())
    service_names = list(domains['service_name'].keys())
    load_customer(connection, customer_names)
    load_customer_device(connection, customer_device)
    load_service(connection, service_names)

if __name__ == "__main__":
    main()
