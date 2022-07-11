"""
Sample SQL Extract Processes
"""
import sqlite3 as db
from textwrap import dedent

def query_1(connection):
    query = dedent("""
        SELECT customer.customer_name, customer_device.device_name 
            FROM customer_device 
            JOIN customer
                ON customer_device.customer_id = customer.rowid
    """)
    cursor = connection.cursor()
    cursor.execute(query)
    for row in cursor.fetchall():
        print({k: row[k] for k in row.keys()})

def query_2(connection):
    query = dedent("""
        SELECT customer_name 
        FROM customer
        UNION ALL
        SELECT service_name 
        FROM service
    """)
    cursor = connection.cursor()
    cursor.execute(query)
    for row in cursor.fetchall():
        print({k: row[k] for k in row.keys()})

def query_3(connection):
    query = dedent("""
        SELECT customer_name, 'no device' 
        FROM customer
        WHERE NOT EXISTS (
            SELECT * 
            FROM customer_device 
            WHERE customer_device.customer_id = customer.rowid)
        UNION
        SELECT customer_name, device_name 
        FROM customer_device
        JOIN customer 
            ON customer_device.customer_id = customer.rowid
    """)
    cursor = connection.cursor()
    cursor.execute(query)
    for row in cursor.fetchall():
        print({k: row[k] for k in row.keys()})

def group_by(connection):
    query = dedent("""
        SELECT service.service_name, count(*) as count
        FROM customer_device_service
        JOIN service
           ON service.rowid = customer_device_service.service_id
        GROUP BY service.service_name
    """)
    cursor = connection.cursor()
    cursor.execute(query)
    for row in cursor.fetchall():
        print({k: row[k] for k in row.keys()})


def main(database_connect="unlearning_sql.db"):
    connection = db.connect(database_connect)
    connection.row_factory = db.Row

    with connection:
        query_1(connection)
        query_2(connection)
        query_3(connection)
        group_by(connection)

if __name__ == "__main__":
    main()
