"""
More complicated schema with synthetic data.
"""

from collections.abc import Iterator
import json
from typing import Annotated, Any, cast

from pydantic import BaseModel, Field
from annotated_types import MinLen, MaxLen

import synthdata


class Customer(BaseModel):
    rowid: Annotated[
        int, Field(json_schema_extra={"sql": {"key": "primary"}})
    ]
    customer_name: Annotated[
        str, MaxLen(64), Field(json_schema_extra={"domain": "name"})
    ]


class DeviceType(BaseModel):
    rowid: Annotated[
        int, Field(json_schema_extra={"sql": {"key": "primary"}})
    ]
    device_type_name: Annotated[
        str, MaxLen(32), Field(json_schema_extra={"domain": "name"})
    ]


class CustomerDevice(BaseModel):
    rowid: Annotated[
        int, Field(json_schema_extra={"sql": {"key": "primary"}})
    ]
    customer_id: Annotated[
        int,
        Field(
            json_schema_extra={
                "sql": {
                    "key": "foreign",
                    "reference": "Customer.rowid",
                }
            }
        ),
    ]
    type_id: Annotated[
        int,
        Field(
            json_schema_extra={
                "sql": {
                    "key": "foreign",
                    "reference": "DeviceType.rowid",
                }
            }
        ),
    ]
    device_name: Annotated[
        str, MaxLen(64), Field(json_schema_extra={"domain": "name"})
    ]


def schema_dump() -> None:
    print(json.dumps(Customer.model_json_schema(), indent=2))
    print(json.dumps(DeviceType.model_json_schema(), indent=2))
    print(json.dumps(CustomerDevice.model_json_schema(), indent=2))
    print()


def main() -> None:
    schema = synthdata.SchemaSynthesizer()
    schema.add(Customer, 100)
    schema.add(DeviceType, 8)
    schema.add(CustomerDevice, 200)

    customer_gen = cast(Iterator[Customer], schema.rows(Customer))
    customer_rows = (next(customer_gen) for _ in range(100))
    customers = {c.rowid: c for c in customer_rows}

    device_type_gen = cast(
        Iterator[DeviceType], schema.rows(DeviceType)
    )
    device_type_rows = (next(device_type_gen) for _ in range(8))
    device_types = {dt.rowid: dt for dt in device_type_rows}

    device_gen = cast(
        Iterator[CustomerDevice], schema.rows(CustomerDevice)
    )
    device_rows = (next(device_gen) for _ in range(200))
    devices = {d.rowid: d for d in device_rows}

    examples = list(devices.keys())[:3]

    for d in (devices[e] for e in examples):
        print(d)

    for c in (
        customers.get(
            devices[e].customer_id,
            f"No customer {devices[e].customer_id}",
        )
        for e in examples
    ):
        print(c)

    for dt in (
        device_types.get(
            devices[e].type_id, f"No device type {devices[e].type_id}"
        )
        for e in examples
    ):
        print(dt)


if __name__ == "__main__":
    schema_dump()
    main()
