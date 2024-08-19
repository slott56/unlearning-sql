"""
Sample Python Load Process -- version 2 -- using Pydantic.

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
import datetime
from functools import lru_cache
from pathlib import Path
import re
import sqlite3 as db
import sys
from textwrap import dedent
from typing import Any, Annotated, Self, cast


from pydantic import (
    BaseModel,
    ValidationError,
    Field,
    ValidationInfo,
    model_validator,
)
from pydantic.functional_validators import (
    BeforeValidator,
    AfterValidator,
)


def latlon_conversion(source: str | float) -> float:
    """Parses latitude or longitude string to produce a normalized float result."""
    match source:
        case str():
            pat = re.compile(
                r"(?P<deg>\d+)\D(?P<min>\d+\.\d+)\D(?P<h>\w)"
            )
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
        case float():
            return source
        case _:
            raise ValueError(f"unknown type for {source!r}")


@lru_cache(128)
def fetch_service_id(
    connection: db.Connection, service_name: str
) -> int | None:
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
    return cast(int, rowid)


@lru_cache(128)
def fetch_customer_device_id(
    connection: db.Connection, customer_name: str, device_name: str
) -> int | None:
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
    return cast(int, rowid)


from pydantic import (
    BaseModel,
    ValidationError,
    Field,
    ValidationInfo,
    model_validator,
)
from pydantic.functional_validators import (
    BeforeValidator,
    AfterValidator,
)


def non_empty(value: str) -> str:
    assert len(value) != 0, f"{value!r} should not be empty"
    return value


class Activation(BaseModel):
    """Model to validate and transform data."""

    start_date: datetime.datetime
    latitude: Annotated[float, BeforeValidator(latlon_conversion)]
    longitude: Annotated[float, BeforeValidator(latlon_conversion)]
    # Initialization-only values
    customer_name: Annotated[str, AfterValidator(non_empty)] = Field(
        exclude=True
    )
    device_name: Annotated[str, AfterValidator(non_empty)] = Field(
        exclude=True
    )
    service_name: Annotated[str, AfterValidator(non_empty)] = Field(
        exclude=True
    )
    # Derived during model validation...
    customer_device_id: int | None = Field(init=False, default=None)
    service_id: int | None = Field(init=False, default=None)

    @model_validator(mode="after")
    def resolve_fk(self, info: ValidationInfo) -> Self:
        db_connection = info.context
        self.customer_device_id = fetch_customer_device_id(
            db_connection, self.customer_name, self.device_name
        )
        self.service_id = fetch_service_id(
            db_connection, self.service_name
        )
        return self


def activation_loader(
    counts: Counter[str],
    connection: db.Connection,
    reader: csv.DictReader,
    writer: csv.DictWriter,
) -> None:
    for row in reader:
        counts["raw"] += 1
        try:
            good_row = Activation.model_validate_strings(
                row, context=connection
            )
            counts["valid and transformed"] += 1
        except ValidationError as error:
            print(error)
            counts["invalid"] += 1
            continue
        writer.writerow(good_row.model_dump())


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
    field_names = [
        n
        for n, info in Activation.model_fields.items()
        if not info.exclude
    ]
    for source in sources:
        with source.open() as source_file:
            reader = csv.DictReader(source_file)
            with target.open("w", newline="") as target_file:
                writer = csv.DictWriter(target_file, field_names)
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
