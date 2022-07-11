"""
Sample Python Extract Processes

Query:
        SELECT service.service_name, count(*) as count
        FROM customer_device_service
        JOIN service
           ON service.rowid = customer_device_service.service_id
        GROUP BY service.service_name
"""
import csv
from collections import Counter
from functools import lru_cache
from pathlib import Path
import sqlite3 as db
from textwrap import dedent

def service_names(connection):
    service_name_query = dedent("""
        SELECT rowid, *
        FROM service
    """)
    cursor = connection.cursor()
    cursor.execute(service_name_query)
    mapping = {
        row['rowid']: row
        for row in cursor.fetchall()
    }
    cursor.close()
    return mapping

def cst_dev_svc_counts(connection):
    service_name_mapping = service_names(connection)
    counter = Counter()
    customer_device_service_query = dedent("""
        SELECT * 
        FROM customer_device_service
    """)
    cursor = connection.cursor()
    cursor.execute(customer_device_service_query)
    for cst_dev_svc in cursor.fetchall():
        service_name_row = service_name_mapping[
            cst_dev_svc['service_id']
        ]
        service_name = service_name_row['service_name']
        counter[service_name] += 1
    cursor.close()
    return counter

OUTPUT_FIELDNAMES = [
    'service_name',
    'count'
]

def main(
        target=Path("service_name_counts.csv"),
        database_connect="unlearning_sql.db"):
    connection = db.connect(database_connect)
    connection.row_factory = db.Row

    with target.open('w', newline='') as target_file:
        writer = csv.DictWriter(target_file, OUTPUT_FIELDNAMES)
        writer.writeheader()
        with connection:
            counts = cst_dev_svc_counts(connection)
            rows = (
                {
                    'service_name': key,
                    'count': value
                }
                for key, value in counts.items()
            )
            writer.writerows(rows)

if __name__ == "__main__":
    main()
