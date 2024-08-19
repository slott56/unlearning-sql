"""
Sample Python Extract Process version 2

Query:
        SELECT service.service_name, count(*) as count
        FROM customer_device_service
        JOIN service
           ON service.rowid = customer_device_service.service_id
        GROUP BY service.service_name

This doesn't use an actual join -- it relies on an object
to provide the mapping by doing lookups as needed.

"""

import argparse
import csv
from collections import Counter
from functools import lru_cache
from pathlib import Path
import sqlite3 as db
import sys
from textwrap import dedent
from typing import Any


class ServiceNameMapping:
    """
    Initialized with a DB connection.
    This behaves like a mapping, so ``snm["service"]`` does the database lookup.
    """

    def __init__(self, connection: db.Connection) -> None:
        self.connection = connection

    @lru_cache(32)
    def get(self, service_id: str, default: str | None = None) -> Any:
        cursor = self.connection.cursor()
        cursor.execute(
            dedent("""
                SELECT * 
                FROM service 
                WHERE rowid = :service_id
            """),
            {"service_id": service_id},
        )
        row = cursor.fetchone()
        cursor.close()
        if row is None and default is None:
            raise ValueError(f"unknown service_id {service_id!r}")
        elif row is None and default is not None:
            return default
        return row

    def __getitem__(self, service_id: str) -> Any:
        return self.get(service_id)


def cst_dev_svc_counts(connection: db.Connection) -> Counter[str]:
    service_name_mapping = ServiceNameMapping(connection)
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
