"""
Sample SQL Load Process
"""
import csv
import re
import sqlite3 as db
from pathlib import Path
from textwrap import dedent

def make_activation(connection):
    create_activation_table = dedent("""
        CREATE TABLE IF NOT EXISTS activation(
            customer_name CHAR(64),
            device_name CHAR(64),
            service_name CHAR(64),
            start_date CHAR(20),
            latitude CHAR(16),
            longitude CHAR(16),
            lat_real REAL,
            lon_real REAL
        )
        """)
    cursor = connection.cursor()
    cursor.execute(create_activation_table)
    connection.commit()
    cursor.close()

def clear_activation(connection):
    clear_activation_tabble = dedent("""
        DELETE 
            FROM activation
        """)
    cursor = connection.cursor()
    cursor.execute(clear_activation_tabble)
    print(f"deleted {cursor.rowcount} old rows")
    connection.commit()
    cursor.close()

def load_activation(connection, activation_reader):
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

def activation_count(connection):
    count_activation_rows = dedent("""
        SELECT 
            COUNT(*) 
            FROM activation
        """)
    cursor = connection.cursor()
    cursor.execute(count_activation_rows)
    count, = cursor.fetchone()
    connection.commit()
    cursor.close()
    return count

def activation_reject_bad_data(connection):
    delete_activation_bad_customers_1 = dedent("""
        DELETE
            FROM activation
            WHERE customer_name = ''
        """)
    delete_activation_bad_customers_2 = dedent("""
        DELETE
            FROM activation
            WHERE length(customer_name) = 0
            OR length(device_name) = 0
            OR length(service_name) = 0
            OR start_date NOT REGEXP '\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\dZ'
            OR latitude NOT REGEXP '\\d{2}°\\d\\d\\.\\d{4}′[NS]'
            OR longitude NOT REGEXP '\\d{3}°\\d\\d\\.\\d{4}′[EW]'
        """)
    cursor = connection.cursor()
    cursor.execute(delete_activation_bad_customers_2)
    deletes = cursor.rowcount
    connection.commit()
    print(f"removed {deletes} bad rows")
    cursor.close()

def activation_locate_disconnected_data(connection):
    """Not the final delete. This is a debugging version."""
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
    print(f"{missing=}")
    # deletes = cursor.rowcount
    connection.commit()
    # print(f"removed {deletes} bad rows")
    cursor.close()

def activation_transformation(connection):
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
    # Assuming additional functions have been defined.
    update_lat_lon_2 = dedent("""
        UPDATE activation
            SET 
                lat_real = lat_decode(latitude),
                lon_real = lon_decode(longitude),
            WHERE 1 = 1
        """)
    cursor = connection.cursor()
    cursor.execute(update_lat_lon_1)
    updates = cursor.rowcount
    connection.commit()
    print(f"updated {updates} rows")
    cursor.close()

def persist(connection):
    """
    Not complete. Doesn't include the INSERT clause.
    """
    load_query = dedent("""
        SELECT customer_device.rowid, service.rowid, 
            start_date, lat_real, lon_real
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
    for row in cursor.fetchall():
        print(f"insert customer_device_service values({row})")
    inserts = cursor.rowcount
    connection.commit()
    print(f"loaded {inserts} final rows")
    cursor.close()


def setup(connection):
    """Adds regexp() to SQLite"""
    def regexp(pattern, data): 
        return re.match(pattern, data) is not None

    connection.create_function("regexp", 2, regexp)

def main(
        source = Path("activation_source.data"), 
        database_connect="unlearning_sql.db"):
    connection = db.connect(database_connect)
    setup(connection)
    
    make_activation(connection)
    clear_activation(connection)
    
    with source.open() as source_file:
        reader = csv.DictReader(source_file)
        load_activation(connection, reader)
    activation_reject_bad_data(connection)
    activation_locate_disconnected_data(connection)
    activation_transformation(connection)
    rows = activation_count(connection)
    print(f"Activations table has {rows} rows")
    persist(connection)

if __name__ == "__main__":
    main()
