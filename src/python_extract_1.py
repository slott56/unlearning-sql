"""
Sample Python Extract Process version 1

Effective Query:
        SELECT service.service_name, count(*) as count
        FROM customer_device_service
        JOIN service
           ON service.rowid = customer_device_service.service_id
        GROUP BY service.service_name

This doesn't use an actual join -- it relies on a Python mapping.
"""

import argparse
import csv
from collections import Counter
from pathlib import Path
import sqlite3 as db
import sys
from textwrap import dedent
from typing import Any


def service_names(connection: db.Connection) -> dict[str, Any]:
    service_name_query = dedent("""
        SELECT rowid, *
        FROM service
    """)
    cursor = connection.cursor()
    cursor.execute(service_name_query)
    mapping = {row["rowid"]: row for row in cursor.fetchall()}
    cursor.close()
    return mapping


def cst_dev_svc_counts(connection: db.Connection) -> Counter[str]:
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
            cst_dev_svc["service_id"]
        ]
        service_name = service_name_row["service_name"]
        counter[service_name] += 1
    cursor.close()
    return counter


def get_options(argv: list[str] = sys.argv[1:]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db", action="store", default="data/unlearning_sql.db"
    )
    parser.add_argument(
        "-o",
        "--output",
        action="store",
        type=Path,
        default=Path("data/service_name_counts.csv"),
    )
    return parser.parse_args(argv)


def main(database_connect: str, target: Path) -> None:
    OUTPUT_FIELDNAMES = ["service_name", "count"]

    connection = db.connect(database_connect)
    connection.row_factory = db.Row

    with target.open("w", newline="") as target_file:
        writer = csv.DictWriter(target_file, OUTPUT_FIELDNAMES)
        writer.writeheader()
        with connection:
            counts = cst_dev_svc_counts(connection)
            rows = (
                {"service_name": key, "count": value}
                for key, value in counts.items()
            )
            writer.writerows(rows)


if __name__ == "__main__":
    options = get_options()
    main(
        database_connect=options.db,
        target=options.output,
    )
