"""
Efficiencies
"""

from collections.abc import Iterable, Iterator
from collections import Counter
import csv
from pathlib import Path
from textwrap import dedent
from functools import lru_cache
import sqlite3 as db
from typing import Any


@lru_cache(128)
def fetch_a_thing(connection: db.Connection, the_key: Any) -> Any:
    cursor = connection.cursor()
    cursor.execute(
        dedent(
            """
            SELECT * FROM table WHERE key = :bind
            """
        ),
        {"bind": the_key},
    )
    row = cursor.fetchone()
    cursor.close()
    return row


class ServiceNameMapping:
    query = dedent("""
        SELECT *
        FROM service
        WHERE service_id = :service_id
    """)

    def __init__(self, connection: db.Connection):
        self.cursor = connection.cursor()

    def close(self):
        self.cursor.close()

    @lru_cache(128)
    def __getitem__(self, service_id: int) -> dict[str, Any]:
        self.cursor.execute(self.query, {"service_id": service_id})
        row = self.cursor.fetchone()
        return row


type Row = dict[str, Any]


def query_table(connection: db.Connection) -> Iterator[Row]:
    cursor = connection.cursor()
    print("Query", cursor, "from", connection)
    cursor.execute("Some Query")
    yield from cursor.fetchall()
    cursor.close()


def lookup_something(
    connection: db.Connection, row_source: Iterable[Row]
) -> Iterator[Row]:
    something_lookup = ServiceNameMapping(connection)
    for row in row_source:
        row["something"] = something_lookup[row["key"]]["value"]
        yield row
    something_lookup.close()


def compute_something(
    connection: db.Connection, row_source: Iterable[Row]
) -> Iterator[Row]:
    def f(this: int, that: int) -> int:
        return 2 * this + that

    for row in row_source:
        row["computed"] = f(row["this"], row["that"])
        yield row


def full_process(connection: db.Connection) -> Iterator[Row]:
    base = query_table(connection)
    joined = lookup_something(connection, base)
    computed = compute_something(connection, joined)
    yield from computed


def process_and_write(connection: db.Connection, target: Path) -> None:
    FIELDNAMES = ["something", "computed", "key", "this", "that"]
    with target.open("w", newline="") as tgt_file:
        writer = csv.DictWriter(tgt_file, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(full_process(connection))


def group_by(
    connection: db.Connection, row_source: Iterable[Row]
) -> Iterator[Row]:
    counts = Counter(row["group_id"] for row in row_source)
    group_rows = (
        {"group_id": group_id, "count(*)": count}
        for group_id, count in counts.items()
    )
    yield from group_rows
