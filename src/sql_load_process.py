"""
Sample SQL Load Process.

Populates the activation table after validating inputs.
"""

import argparse
import csv
import re
import sqlite3 as db
from pathlib import Path
import sys
from textwrap import dedent


def make_activation(connection: db.Connection) -> None:
    create_activation_table = dedent("""
        CREATE TABLE IF NOT EXISTS activation(
            customer_name CHAR(64),
            device_name CHAR(64),
            service_name CHAR(64),
            start_date CHAR(20),
            latitude CHAR(16),
            longitude CHAR(16),
            lat_real REAL,
            lon_real REAL,
            start_timestamp TIMESTAMP
        )
        """)
    cursor = connection.cursor()
    cursor.execute(create_activation_table)
    connection.commit()
    cursor.close()


def clear_activation(connection: db.Connection) -> None:
    clear_activation_tabble = dedent("""
        DELETE 
            FROM activation
        """)
    cursor = connection.cursor()
    cursor.execute(clear_activation_tabble)
    print(f"deleted {cursor.rowcount} old rows")
    connection.commit()
    cursor.close()


def load_activation(
    connection: db.Connection, activation_reader: csv.DictReader
) -> None:
    insert_activation_row = dedent("""
        INSERT 
            INTO activation(customer_name, device_name, service_name, 
                start_date, latitude, longitude)
            VALUES (
                :customer_name, :device_name, :service_name, 
                :start_date, :latitude, :longitude
            )
        """)
    cursor = connection.cursor()
    inserts = 0
    for row in activation_reader:
        cursor.execute(insert_activation_row, row)
        inserts += cursor.rowcount
    connection.commit()
    print(f"inserted {inserts} new rows")
    cursor.close()


def activation_count(connection: db.Connection) -> int:
    count_activation_rows = dedent("""
        SELECT 
            COUNT(*) 
            FROM activation
        """)
    cursor = connection.cursor()
    cursor.execute(count_activation_rows)
    (count,) = cursor.fetchone()
    connection.commit()
    cursor.close()
    return count


def activation_reject_bad_data_1(connection: db.Connection) -> None:
    count_activation_bad_customers = dedent("""
        DELETE
        FROM activation
        WHERE customer_name = ''
    """)
    cursor = connection.cursor()
    cursor.execute(count_activation_bad_customers)
    deletes = cursor.rowcount
    connection.commit()
    print(f"removed {deletes} bad rows")
    cursor.close()


def activation_reject_bad_data_2(connection: db.Connection) -> None:
    delete_activation_bad_customers_1 = dedent("""
        DELETE
            FROM activation
            WHERE customer_name = ''
        """)  # noqa: F841
    delete_activation_bad_customers_2 = dedent(r"""
        DELETE
            FROM activation
            WHERE length(customer_name) = 0
            OR length(device_name) = 0
            OR length(service_name) = 0
            OR start_date NOT REGEXP '\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\+\d\d:\d\d'
            OR latitude NOT REGEXP '\d{2}°\d\d\.\d{4}′[NS]'
            OR longitude NOT REGEXP '\d{3}°\d\d\.\d{4}′[EW]'
        """)
    cursor = connection.cursor()
    cursor.execute(delete_activation_bad_customers_2)
    deletes = cursor.rowcount
    connection.commit()
    print(f"removed {deletes} bad rows")
    cursor.close()


def activation_locate_disconnected_data(
    connection: db.Connection,
) -> None:
    """Not the final delete.
    This is a debugging query to be sure the condition is correct.
    Replace ``SELECT *`` with ``DELETE`` and check the row count.
    """
    find_activation_bad_customers = dedent("""
        SELECT *
            FROM activation
            WHERE NOT EXISTS(
                SELECT * FROM customer
                WHERE customer.customer_name = activation.customer_name
            )
            OR NOT EXISTS(
                SELECT * FROM service
                WHERE service.service_name = activation.service_name
            )
            OR NOT EXISTS(
                SELECT * FROM customer_device, customer
                WHERE activation.customer_name = customer.customer_name
                AND customer.rowid = customer_device.customer_id
                AND customer_device.device_name = activation.device_name
            )
            OR EXISTS(
                SELECT * FROM customer_device_service cd_s, customer_device, service, customer
                WHERE cd_s.customer_device_id = customer_device.rowid
                AND cd_s.service_id = service.rowid
                AND activation.service_name = service.service_name
                AND customer_device.customer_id = customer.rowid
                AND activation.customer_name = customer.customer_name
                AND activation.device_name = customer_device.device_name 
            )
        """)
    cursor = connection.cursor()
    cursor.execute(find_activation_bad_customers)
    missing = cursor.fetchall()
    print(f"found missing connections: {len(missing)=}")
    connection.commit()
    cursor.close()


def activation_transformation(connection: db.Connection) -> None:
    """
    Incomplete -- doesn't convert the date.

    Does convert latitude and longitude.
    latitude = "01°28.0000′N"
    longitude = "001°28.0000′W"
    """
    update_lat_lon_1 = dedent("""
        UPDATE activation
            SET 
                lat_real = (
                    CAST(substr(latitude, 1, 2) as REAL) 
                    + CAST(substr(latitude, 4, 7) as REAL)/60
                ) * (
                    CASE substr(latitude, 12, 1) WHEN 'N' THEN +1 ELSE -1 END
                ),
                lon_real = (
                    CAST(substr(longitude, 1, 3) as REAL) 
                    + CAST(substr(longitude, 5, 7) as REAL)/60
                ) * (
                    CASE substr(longitude, 13, 1) WHEN 'E' THEN +1 ELSE -1 END
                )
            WHERE 1 = 1
        """)
    # Alternative design, assuming two functions have been defined:
    # lat_decode and lon_decode()
    update_lat_lon_2 = dedent("""
        UPDATE activation
            SET 
                lat_real = lat_decode(latitude),
                lon_real = lon_decode(longitude),
            WHERE 1 = 1
        """)  # noqa: F841
    cursor = connection.cursor()
    cursor.execute(update_lat_lon_1)
    updates = cursor.rowcount
    connection.commit()
    print(f"updated {updates} rows")
    cursor.close()


def persist(connection: db.Connection) -> None:
    """
    Persists valid row dat into the customer_device_service table.

            customer_device_id INTEGER
            REFERENCES customer_device(rowid),
            service_id INTGER
            REFERENCES service(rowid),
            start DATETIME,
            latitude REAL,
            longitude REAL

    """
    load_query = dedent("""
        INSERT INTO customer_device_service(customer_device_id, service_id, start, latitude, longitude)
            SELECT customer_device.rowid, 
                service.rowid, 
                start_date, 
                lat_real, 
                lon_real
            FROM activation
            JOIN service
                ON service.service_name = activation.service_name
            JOIN customer_device
                ON customer_device.device_name = activation.device_name
                AND customer_device.customer_id = customer.rowid
            JOIN customer
                ON customer.customer_name = activation.customer_name
    """)
    cursor = connection.cursor()
    cursor.execute(load_query)
    inserts = cursor.rowcount
    connection.commit()
    print(f"loaded {inserts} final rows")

    cursor.execute("SELECT * FROM customer_device_service")
    for row in cursor.fetchall():
        print(row)

    cursor.close()


def setup(connection: db.Connection) -> None:
    """Adds regexp() to SQLite"""

    def regexp(pattern, data):
        return re.match(pattern, data) is not None

    connection.create_function("regexp", 2, regexp)


def get_options(argv: list[str] = sys.argv[1:]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--db",
        action="store",
        type=str,
        default="data/unlearning_sql.db",
    )
    parser.add_argument(
        "source",
        nargs="*",
        type=Path,
        default=[Path("data/activation_source.csv")],
    )
    options = parser.parse_args(argv)
    return options


def main(database_connect: str, sources: list[Path]) -> None:
    connection = db.connect(database_connect)
    setup(connection)

    # Schema Definition.
    make_activation(connection)
    clear_activation(connection)

    # Load raw activation records into database.
    # From these, create and persist activation
    # rows by resolving FK references.
    for source in sources:
        with source.open() as source_file:
            reader = csv.DictReader(source_file)
            load_activation(connection, reader)
        activation_reject_bad_data_2(connection)
        activation_locate_disconnected_data(connection)
        activation_transformation(connection)
        rows = activation_count(connection)
        print(f"Activations table has {rows} rows")
        persist(connection)


if __name__ == "__main__":
    options = get_options(sys.argv[1:])
    main(options.db, options.source)
