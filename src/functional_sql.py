"""
Demonstration of Functional SQL.
"""

from collections.abc import Iterator
from typing import cast

import synthdata
import funcsql

from synthetic_data import Customer, DeviceType, CustomerDevice


def data() -> tuple[funcsql.Table, ...]:
    schema = synthdata.SchemaSynthesizer()
    schema.add(Customer, 100)
    schema.add(DeviceType, 8)
    schema.add(CustomerDevice, 200)

    customer_gen = cast(Iterator[Customer], schema.rows(Customer))
    customer_rows = [
        next(customer_gen).model_dump() for _ in range(100)
    ]
    CustTbl = funcsql.Table("CustTbl", customer_rows)

    device_type_gen = cast(
        Iterator[DeviceType], schema.rows(DeviceType)
    )
    device_type_rows = [
        next(device_type_gen).model_dump() for _ in range(8)
    ]
    DevTbl = funcsql.Table("DevTbl", device_type_rows)

    device_gen = cast(
        Iterator[CustomerDevice], schema.rows(CustomerDevice)
    )
    device_rows = [next(device_gen).model_dump() for _ in range(200)]
    CustDevTbl = funcsql.Table("CustDevTbl", device_rows)
    return CustTbl, DevTbl, CustDevTbl


def example_1(CustTbl, DevTbl, CustDevTbl) -> None:
    query = (
        funcsql.Select("*")
        .from_(CustTbl, DevTbl, CustDevTbl)
        .where(
            lambda cr: cr.CustTbl.rowid == cr.CustDevTbl.customer_id
            and cr.DevTbl.rowid == cr.CustDevTbl.type_id
        )
    )
    for row in funcsql.fetch(query):
        print(row)


def example_2(CustTbl, DevTbl, CustDevTbl) -> None:
    query = (
        funcsql.Select(
            name=lambda cr: cr.DevTbl.device_type_name,
            count=funcsql.Aggregate(
                funcsql.count, lambda cr: cr.CustDevTbl.rowid
            ),
        )
        .from_(DevTbl, CustDevTbl)
        .where(lambda cr: cr.DevTbl.rowid == cr.CustDevTbl.type_id)
        .group_by("name")
    )
    for row in funcsql.fetch(query):
        print(row)


if __name__ == "__main__":
    CustTbl, DevTbl, CustDevTbl = data()
    example_1(CustTbl, DevTbl, CustDevTbl)
    example_2(CustTbl, DevTbl, CustDevTbl)
