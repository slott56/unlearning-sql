"""
Sample Python Load Process.
This filters activation records to create a subset that can be loaded into
the  ``CUSTOMER_DEVICE_SERVICE`` table of the database.

Follow-up processing might be the following CLI command to load the valid rows.

..  code-sample:: bash

        sqlite3 unlearning_sql.db <<EOF
        .import -v --csv --skip 1 activation_load.csv CUSTOMER_DEVICE_SERVICE
        EOF

"""

import argparse
from collections import Counter
import csv
from dataclasses import dataclass, field
import datetime
from functools import lru_cache
from pathlib import Path
import re
import sqlite3 as db
import sys
from textwrap import dedent
from typing import Any

from dateutil import parser as date_parser


def latlon_conversion(source: str) -> float:
    """Parses latitude or longitude string to produce a normalized float result."""
    pat = re.compile(r"(?P<deg>\d+)\D(?P<min>\d+\.\d+)\D(?P<h>\w)")
    if m := pat.match(source):
        fields = m.groupdict()
        deg, min, hemisphere = (
            float(fields["deg"]),
            float(fields["min"]),
            fields["h"],
        )
        sign = -1 if hemisphere in "SsWw" else +1
        return sign * (deg + min / 60)
    else:
        raise ValueError(f"invalid {source}")


def datetime_conversion(source: str) -> datetime.datetime:
    """
    Parses numerous date formats, including ISO 8601 dates.
    Python's standard library ``strptime()`` doesn't cover **all** the cases.
    """
    dt = date_parser.parse(source)
    if dt is None:
        raise ValueError("invalid date")
    return dt


from dataclasses import dataclass, field


@dataclass
class Activation:
    """Initialization handled eagerly by the application."""

    customer_name: str
    device_name: str
    service_name: str
    start_date: str
    latitude: str
    longitude: str
    lat_real: float = field(init=False)
    lon_real: float = field(init=False)
    start_date_datetime: datetime.datetime = field(init=False)

    def __post_init__(self) -> None:
        self.lat_real = latlon_conversion(self.latitude)
        self.lon_real = latlon_conversion(self.longitude)
        self.start_date_datetime = datetime_conversion(self.start_date)


from dataclasses import dataclass, field


@dataclass
class Activation_p:
    """Lazy properties to transform data."""

    customer_name: str
    device_name: str
    service_name: str
    start_date: str
    latitude: str
    longitude: str

    @property
    def lat_real(self) -> float:
        return latlon_conversion(self.latitude)

    @property
    def lon_real(self) -> float:
        return latlon_conversion(self.longitude)

    @property
    def start_date_datetime(self) -> datetime.datetime:
        return datetime_conversion(self.start_date)


def failure_report(
    row: dict[str, str], failures: dict[str, bool]
) -> None:
    names = (name for name, failed in failures.items() if failed)
    summary = [
        f"{name}={row[name]!r}"
        if "," not in name
        else f"{name} in {[row[e] for e in name.split(',')]!r}"
        for name in names
    ]
    print("Failure", summary)


def bad_data(counts: Counter[str], row: dict[str, str]) -> bool:
    """Atomic data validation rules."""
    rule_failure = {
        "customer_name": len(row["customer_name"]) == 0,
        "device_name": len(row["device_name"]) == 0,
        "service_name": len(row["service_name"]) == 0,
        "start_date": re.match(
            r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\+\d\d:\d\d",
            row["start_date"],
        )
        is None,
        "latitude": re.match(r"\d{2}°\d\d\.\d{4}′[NS]", row["latitude"])
        is None,
        "longitude": re.match(
            r"\d{3}°\d\d\.\d{4}′[EW]", row["longitude"]
        )
        is None,
    }
    bad = any(rule_failure.values())
    if bad:
        failure_report(row, rule_failure)
    count_name = "invalid" if bad else "valid"
    counts[count_name] += 1
    return bad


@lru_cache(128)
def fetch_customer_id(
    connection: db.Connection, customer_name: str
) -> Any | None:
    """Map customer name to customer ID."""
    customer_query = dedent("""
        SELECT rowid FROM customer
        WHERE customer_name = :customer_name
    """)
    cursor = connection.cursor()
    cursor.execute(customer_query, {"customer_name": customer_name})
    try:
        (rowid,) = cursor.fetchone()
    except TypeError:
        rowid = None
    cursor.close()
    return rowid


@lru_cache(128)
def fetch_service_id(
    connection: db.Connection, service_name: str
) -> Any | None:
    """Map service name to service ID."""
    service_query = dedent("""
        SELECT rowid FROM service
        WHERE service_name = :service_name
    """)
    cursor = connection.cursor()
    cursor.execute(service_query, {"service_name": service_name})
    try:
        (rowid,) = cursor.fetchone()
    except TypeError:
        rowid = None
    cursor.close()
    return rowid


@lru_cache(128)
def fetch_customer_device_id(
    connection: db.Connection, customer_name: str, device_name: str
) -> Any | None:
    """Map customer name and device name pair to customer_device ID."""
    customer_device_query = dedent("""
        SELECT customer_device.rowid 
        FROM customer_device
        JOIN customer ON customer.rowid = customer_device.customer_id
        WHERE customer_device.device_name = :device_name
        AND customer.customer_name = :customer_name
    """)
    cursor = connection.cursor()
    cursor.execute(
        customer_device_query,
        {"device_name": device_name, "customer_name": customer_name},
    )
    try:
        (rowid,) = cursor.fetchone()
    except TypeError:
        rowid = None
    cursor.close()
    return rowid


def bad_references(
    counts: Counter[str], connection: db.Connection, row: dict[str, Any]
) -> bool:
    """Attempt to fetch all required foreign key values."""
    rule_failure = {
        "customer_name": fetch_customer_id(
            connection, row["customer_name"]
        )
        is None,
        "service_name": fetch_service_id(
            connection, row["service_name"]
        )
        is None,
        "customer_name,device_name": fetch_customer_device_id(
            connection, row["customer_name"], row["device_name"]
        )
        is None,
    }
    bad = any(rule_failure.values())
    if bad:
        failure_report(row, rule_failure)

    count_name = "invalid references" if bad else "valid references"
    counts[count_name] += 1
    return bad


def transform_data_dict(
    counts: Counter[str], row: dict[str, Any]
) -> dict[str, Any]:
    """Base transformations from raw string values."""
    row["lat_real"] = latlon_conversion(row["latitude"])
    row["lon_real"] = latlon_conversion(row["longitude"])
    row["start_date_datetime"] = datetime_conversion(row["start_date"])
    counts["transform"] += 1
    return row


def transform_data_dc(
    counts: Counter[str], row: dict[str, Any]
) -> Activation:
    counts["transform"] += 1
    return Activation(**row)


def transform_data_dcp(
    counts: Counter[str], row: dict[str, Any]
) -> Activation_p:
    counts["transform"] += 1
    return Activation_p(**row)


def persist_data_dict(
    counts: Counter[str], connection: db.Connection, row: dict[str, Any]
) -> dict[str, Any]:
    """
    Write a final object suitable for loading the database.
    """
    output = {
        "customer_device_id": fetch_customer_device_id(
            connection, row["customer_name"], row["device_name"]
        ),
        "service_id": fetch_service_id(connection, row["service_name"]),
        "start_date": row["start_date_datetime"],
        "latitude": row["lat_real"],
        "longitude": row["lon_real"],
    }
    counts["saved"] += 1
    return output


def persist_data_dc(
    counts: Counter[str], connection: db.Connection, row: Activation
) -> dict[str, Any]:
    """
    Write a final object suitable for loading the database.
    """
    output = {
        "customer_device_id": fetch_customer_device_id(
            connection, row.customer_name, row.device_name
        ),
        "service_id": fetch_service_id(connection, row.service_name),
        "start_date": str(row.start_date_datetime),
        "latitude": row.lat_real,
        "longitude": row.lon_real,
    }
    counts["saved"] += 1
    return output


OUTPUT_FIELDNAMES = [
    "customer_device_id",
    "service_id",
    "start_date",
    "latitude",
    "longitude",
]


def activation_loader(
    counts: Counter[str],
    connection: db.Connection,
    reader: csv.DictReader,
    writer: csv.DictWriter,
) -> None:
    for row in reader:
        counts["raw"] += 1
        any_field_bad = bad_data(counts, row)
        if any_field_bad:
            continue
        any_reference_bad = bad_references(counts, connection, row)
        if any_reference_bad:
            continue
        # Uses dict[str, Any]
        try:
            good_row = transform_data_dict(counts, row)
        except ValueError as ex:
            print(ex)
            print(row)
            counts["invalid transform"] += 1
            continue
        final = persist_data_dict(counts, connection, good_row)
        print(final)
        writer.writerow(final)


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
        default=Path("data/activation_load.csv"),
    )
    parser.add_argument(
        "source",
        nargs=1,
        type=Path,
        default=Path("data/activation_source.csv"),
    )
    return parser.parse_args(argv)


def main(
    database_connect: str, target: Path, sources: list[Path]
) -> None:
    connection = db.connect(database_connect)
    counts = Counter()
    for source in sources:
        with source.open() as source_file:
            reader = csv.DictReader(source_file)
            with target.open("w", newline="") as target_file:
                writer = csv.DictWriter(target_file, OUTPUT_FIELDNAMES)
                writer.writeheader()
                activation_loader(counts, connection, reader, writer)

    print(f"Source had {counts['raw']} rows")
    print(f"Invalid {counts['invalid']} rows")
    print(f"Valid {counts['valid']} rows")
    print(f"Invalid references {counts['invalid references']} rows")
    print(f"valid references {counts['valid references']} rows")
    print(f"Invalid transformations {counts['invalid transform']} rows")
    print(f"valid transformations {counts['transform']} rows")
    print(f"Saved {counts['saved']} rows")


if __name__ == "__main__":
    options = get_options()
    main(options.db, options.output, options.source)
