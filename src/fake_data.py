"""
Some sample data for the validation processing.

Leverages the https://github.com/slott56/DataSynthTool project's synthesizer.

For most fields, strings are sufficient.
The ``start_date`` field  is a ``datetime.datetime``.
The default string representation is not RFC-3339.
This requires a little bonus conversion to create bits of non-invalid data.
"""

import argparse
from collections.abc import Iterator
import csv
import datetime
import json
from pathlib import Path
from pprint import pprint
import random
import sys
from typing import Annotated, Any, cast
from pydantic import BaseModel, Field, ConfigDict
from annotated_types import MinLen, MaxLen

from jsonschema import Draft202012Validator

import synthdata


class SynthesizeLatitude(synthdata.SynthesizeString):
    def value_gen(self, sequence: int | None = None) -> Any:
        deg, min = divmod(cast(int, sequence), 60)
        return f"{deg:02d}°{min:02d}.0000′N"

    def noise_gen(self, sequence: int | None = None) -> Any:
        deg, min = random.randint(91, 100), random.randint(61, 90)
        h = random.choice("EW")
        return f"{deg:02d}°{min:02d}.0000′{h}"

    @classmethod
    def match(
        cls, field_type: type, json_schema_extra: dict[str, Any]
    ) -> bool:
        """Requires ``Annotated[str, ...]`` and ``json_schema_extra`` with ``{"domain": "latitude"}``"""
        if issubclass(field_type, str):
            return (
                json_schema_extra is not None
                and cast(dict[str, str], json_schema_extra).get(
                    "domain"
                )
                == "latitude"
            )
        return False


class SynthesizeLongitude(synthdata.SynthesizeString):
    def value_gen(self, sequence: int | None = None) -> Any:
        deg, min = divmod(cast(int, sequence), 60)
        return f"{deg:03d}°{min:02d}.0000′W"

    def noise_gen(self, sequence: int | None = None) -> Any:
        deg, min = random.randint(181, 210), random.randint(61, 90)
        h = random.choice("NS")
        return f"{deg:02d}°{min:02d}.0000′{h}"

    @classmethod
    def match(
        cls, field_type: type, json_schema_extra: dict[str, Any]
    ) -> bool:
        """Requires ``Annotated[str, ...]`` and ``json_schema_extra`` with ``{"domain": "longitude"}``"""
        if issubclass(field_type, str):
            return (
                json_schema_extra is not None
                and cast(dict[str, str], json_schema_extra).get(
                    "domain"
                )
                == "longitude"
            )
        return False


class Activation(BaseModel):
    """
    The source activation records from some other application.
    Must be reformatted and validated to be useful.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "activation_source.schema",
        }
    )

    customer_name: Annotated[str, MinLen(1), MaxLen(64)] = Field(
        json_schema_extra={"domain": "name"}
    )
    device_name: Annotated[str, MinLen(1), MaxLen(16)] = Field(
        json_schema_extra={"domain": "name"}
    )
    device_type_name: Annotated[str, MinLen(1), MaxLen(8)] = Field(
        json_schema_extra={"domain": "name"}
    )
    service_name: Annotated[str, MinLen(1), MaxLen(16)] = Field(
        json_schema_extra={"domain": "name"}
    )
    start_date: datetime.datetime
    latitude: Annotated[str, MaxLen(16)] = Field(
        json_schema_extra={"domain": "latitude"}
    )
    longitude: Annotated[str, MaxLen(16)] = Field(
        json_schema_extra={"domain": "longitude"}
    )


def make_data(generator: Iterator[dict[str, Any]]) -> dict[str, str]:
    """
    Most of the generated data is string.
    start_date, however, requires some additional care.

    :param generator: The ``DataIter`` instance to provide noisy data.
    :return: A cleaned up instance with proper date-time formatting.
    """
    row = next(generator)
    if row["start_date"]:
        try:
            date_str = row["start_date"].isoformat(timespec="seconds")
            row["start_date"] = date_str
        except Exception:
            pass  # Invalid non-None date? Leave it alone.
    return row


def get_options(argv: list[str] = sys.argv[1:]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--schema", action="store", type=Path, default=None
    )
    parser.add_argument(
        "-o",
        "--output",
        action="store",
        type=Path,
        default=Path("data/new_activation_source.csv"),
    )
    parser.add_argument(
        "--count", action="store", type=int, default=100
    )
    parser.add_argument("--seed", action="store", type=int, default=42)
    parser.add_argument(
        "--noise", action="store", type=float, default=0.10
    )
    return parser.parse_args(argv)


def dump_schema(
    schema_path: Path = Path("activation_source.schema"),
) -> None:
    json_schema = Activation.model_json_schema()
    Draft202012Validator.check_schema(json_schema)
    schema_path.write_text(json.dumps(json_schema, indent=2))
    pprint(json_schema)


def main(
    count: int = 100,
    output_path: Path = Path("data/activation_source.csv"),
    seed: int = 42,
    noise: float = 0.10,
) -> None:
    random.seed(seed)

    schema = synthdata.SchemaSynthesizer()
    schema.add(Activation)

    generator = schema.data(Activation, noise=noise)
    with output_path.open("w", newline="") as output_file:
        writer = csv.DictWriter(
            output_file, fieldnames=Activation.model_fields
        )
        writer.writeheader()
        data = (make_data(generator) for i in range(count))
        writer.writerows(data)

    print(
        f"Output: {output_path} has {count} rows using Activation schema"
    )


if __name__ == "__main__":
    options = get_options(sys.argv[1:])
    print(options)
    if options.schema:
        dump_schema(options.schema)
    main(
        count=options.count,
        output_path=options.output,
        seed=options.seed,
        noise=options.noise,
    )
