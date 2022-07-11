"""
Make some fake data for a schema.
"""
import csv
import datetime
from jsonschema import Draft202012Validator
import json
from pathlib import Path
import random
import string
from typing import Any
from collections.abc import Iterator

def trash_string(size: int = 4) -> str:
    text = "".join(random.choice(string.printable) for _ in range(size))
    return text

class NullGenerator(Iterator[None]):
    def __init__(self, schema) -> None:
        self.schema = schema
    def __next__(self) -> None:
        return None
    def trash(self) -> str:
        return trash_string(4)

class BoolGenerator(Iterator[bool]):
    def __init__(self, schema) -> None:
        self.schema = schema
        self.count = 0
    def __next__(self) -> int:
        self.count = self.count + 1
        return self.count % 2 == 0
    def trash(self) -> str:
        return trash_string(4)

class IntGenerator(Iterator[int]):
    def __init__(self, schema) -> None:
        self.schema = schema
        # TODO: Get minimum and maximum
        self.count = 0
    def __next__(self) -> int:
        self.count = self.count + 1
        return self.count
    def trash(self) -> str:
        return trash_string(4)

class FloatGenerator(Iterator[float]):
    def __init__(self, schema) -> None:
        self.schema = schema
        # TODO: Get minimum and maximum
        self.count = 0
    def __next__(self) -> float:
        self.count = self.count + 1
        return float(self.count)
    def trash(self) -> str:
        return trash_string(4)

class DateTimeGenerator(Iterator[str]):
    def __init__(self, schema) -> None:
        self.schema = schema
        # TODO: Get minimumDate and maximumDate
        self.date = datetime.datetime(1970, 1, 1)
    def __next__(self) -> str:
        self.date = self.date + datetime.timedelta(days=1)
        return self.date.isoformat() + "Z"
    def trash(self) -> str:
        return trash_string(8)

class StringGenerator(Iterator[str]):
    domain = string.ascii_letters
    def __init__(self, schema) -> None:
        self.schema = schema
        # TODO: Get format and example and pattern 
        # TODO: get minLength and maxLength
        self.minLength = schema.get("minLength", 0)
        self.maxLength = schema.get("maxLength", 12)
        self.count = 0
    def __next__(self) -> str:
        self.count = self.count + 1
        sz = self.count % (self.maxLength - self.minLength) + self.minLength
        text = "".join(random.choice(self.domain) for _ in range(sz))
        return text
    def trash(self) -> str:
        if self.minLength > 0:
            return ""
        else:
            return trash_string(self.maxLength+1)
        
class LatGenerator(Iterator[str]):
    def __init__(self, schema) -> None:
        self.schema = schema
        self.count = 0
    def __next__(self) -> str:
        self.count = self.count + 1
        deg, min = divmod(self.count, 60)
        return f"{deg:02d}°{min:02d}.0000′N"
    def trash(self) -> str:
        return trash_string(8)

class LonGenerator(Iterator[str]):
    def __init__(self, schema) -> None:
        self.schema = schema
        self.count = 0
    def __next__(self) -> str:
        self.count = self.count + 1
        deg, min = divmod(self.count, 60)
        return f"{deg:03d}°{min:02d}.0000′W"
    def trash(self) -> str:
        return trash_string(8)

class ArrayGenerator(Iterator[list[Any]]):
    def __init__(self, schema) -> None:
        self.item_gen : list[Iterator[Any]] = make_generator(schema["items"])
        self.minItems = schema.get("minItems", 1)
        self.maxItems = schema.get("maxItems", 10)
        # TODO: Handle prefixItems and additionalItems to support tuples with varying types
        self.count = 0
    def __next__(self) -> list[Any]:
        self.count = self.count + 1
        sz = self.count % (self.maxItems - self.minItems) + self.minItems
        data = [
            next(g) for g in self.item_gen
        ]
        return data

class ObjectGenerator(Iterator[dict[str, Any]]):
    def __init__(self, schema) -> None:
        self.property_map: dict[str, Iterator[Any]] = {
            p: make_generator(schema["properties"][p]) for p in schema["properties"]
        }
    def __next__(self) -> dict[str, Any]:
        data = {
            n: next(self.property_map[n]) for n in self.property_map
        }
        return data

def make_generator(schema):
    match schema:
        case {"type": "integer"}:
            return IntGenerator(schema)
        case {"type": "number"}:
            return FloatGenerator(schema)
        case {"type": "string"}:
            match schema:
                case {"format": "date-time"}:
                    return DateTimeGenerator(schema)
                case {"format": "latitude"}:
                    return LatGenerator(schema)
                case {"format": "longitude"}:
                    return LonGenerator(schema)
                case _:  # TODO: https://json-schema.org/understanding-json-schema/reference/string.html#built-in-formats
                    return StringGenerator(schema)
        case {"type": "boolean"}:
            return BoolGenerator(schema)
        case {"type": "null"}:
            return NullGenerator(schema)
        case {"type": "array"}:
            return ArrayGenerator(schema)
        case {"type": "object"}:
            return ObjectGenerator(schema)
        # TODO: allOf, anyOf, oneOf

def null_injector(property_names, obj_generator) -> int:
    """One row with a NULL value for each attribute"""
    for p in property_names:
        bad_row = next(obj_generator)
        bad_row[p] = ""
        yield bad_row
    
def trash_injector(property_names, obj_generator) -> int:
    """One row with a trash value for each attribute"""
    for p in property_names:
        bad_row = next(obj_generator)
        bad_row[p] = obj_generator.property_map[p].trash()
        yield bad_row

def main(count = 100, schema_path = Path("activation_source.schema.json"), output_path = Path("activation_source.data"), seed = 42) -> None:
    random.seed(seed)
    with schema_path.open() as schema_file:
        schema = json.load(schema_file)
    Draft202012Validator.check_schema(schema)
    generator = make_generator(schema)
    with output_path.open("w", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=schema["properties"])
        writer.writeheader()
        data = (next(generator) for i in range(count))
        writer.writerows(data)
        # Append some rows with other validation problems.
        bad = 0
        if schema["type"] == "object":
            writer.writerows(null_injector(schema["properties"], generator))
            bad = len(schema["properties"])
            writer.writerows(trash_injector(schema["properties"], generator))
            bad += len(schema["properties"])

    print(f"Output: {output_path} has {count} good rows and {bad} bad rows for {schema['title']} schema")

if __name__ == "__main__":
    main()
