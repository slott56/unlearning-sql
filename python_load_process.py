"""
Sample Python Load Process.

Follow-up processing might be

::

	sqlite3 unlearning_sql.db <<EOF
	.import -v --csv --skip 1 activation_load.csv customer_device_service
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
from dataclasses import dataclass, field

@dataclass
class Activation:
    customer_name: str
    device_name: str
    service_name: str
    start_date: str
    latitude: str
    longitude: str
    lat_real: float = field(init=False)
    lon_real: float = field(init=False)
    start_date_datetime: datetime.datetime = field(init=False)

@dataclass
class Activation_p:
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
        return latlon_conversion(self.latitude)
    @property
    def start_date_datetime(self) -> datetime.datetime:
        return datetime.datetime.strptime(
        row['start_date'],
        "%Y-%m-%dT%H:%M:%SZ"
    )

def bad_data(counts, row):
    bad = (
        len(row['customer_name']) == 0
        or len(row['device_name']) == 0
        or len(row['service_name']) == 0
        or re.match(
            '\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\dZ', 
            row['start_date']) is None
        or re.match(
            '\\d{2}°\\d\\d\\.\\d{4}′[NS]', 
            row['latitude']) is None
        or re.match(
            '\\d{3}°\\d\\d\\.\\d{4}′[EW]', 
            row['longitude']) is None
    )
    count_name = 'invalid' if bad else 'valid'
    counts[count_name] += 1
    return bad

@lru_cache(128)
def fetch_customer_id(connection, customer_name):
    customer_query = dedent("""
        SELECT rowid FROM customer
        WHERE customer_name = :customer_name
    """)
    cursor = connection.cursor()
    cursor.execute(
        customer_query,
        {'customer_name': customer_name}
    )
    try:
        rowid, = cursor.fetchone()
    except TypeError:
        rowid = None
    cursor.close()
    return rowid

@lru_cache(128)
def fetch_service_id(connection, service_name):
    service_query = dedent("""
        SELECT rowid FROM service
        WHERE service_name = :service_name
    """)
    cursor = connection.cursor()
    cursor.execute(
        service_query,
        {'service_name': service_name}
    )
    try:
        rowid, = cursor.fetchone()
    except TypeError:
        rowid = None
    cursor.close()
    return rowid

@lru_cache(128)
def fetch_customer_device_id(connection, customer_name, device_name):
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
        {'device_name': device_name, 'customer_name': customer_name}
    )
    try:
        rowid, = cursor.fetchone()
    except TypeError:
        rowid = None
    cursor.close()
    return rowid

def bad_references(counts, connection, row):
    bad = any([
        fetch_customer_id(connection, row['customer_name']) is None,
        fetch_service_id(connection, row['service_name']) is None,
        fetch_customer_device_id(connection, row['customer_name'], row['device_name']) is None,
    ])
    count_name = 'invalid references' if bad else 'valid references'
    counts[count_name] += 1
    return bad

def transform_data(counts, row):
    parse_pat = re.compile('(\d+)\D(\d+\.\d+)\D(\w)')
    deg, min, h = parse_pat.match(row['latitude']).groups()
    row['lat_real'] = (
        (float(deg) + float(min) / 60) * (1 if h == "N" else -1)
    )
    deg, min, h = parse_pat.match(row['longitude']).groups()
    row['lon_real'] = (
        (float(deg) + float(min) / 60) * (1 if h == "E" else -1)
    )
    row['start_date'] = datetime.datetime.strptime(
        row['start_date'],
        "%Y-%m-%dT%H:%M:%SZ"
    )
    counts['transform'] += 1
    return row

def persist_data(counts, connection, row):
    """A dataclass is better choice for this."""
    output = {
        'customer_device_id': fetch_customer_device_id(connection, row['customer_name'], row['device_name']),
        'service_id': fetch_service_id(connection, row['service_name']),
        'start_date': row['start_date'],
        'latitude': row['lat_real'],
        'longitude': row['lon_real']
    }
    print(output)
    counts['saved'] += 1
    return output

OUTPUT_FIELDNAMES = [
    'customer_device_id',
    'service_id',
    'start_date', 
    'latitude',
    'longitude',
]

def activation_loader(counts, connection, reader, writer):
    for row in reader:
        counts['raw'] += 1
        any_field_bad = bad_data(counts, row)
        if any_field_bad:
            continue
        any_reference_bad = bad_references(counts, connection, row)
        if any_reference_bad:
            continue
        try:
            good_row = transform_data(counts, row)
        except ValueError:
            counts['transform invalid'] += 1
            continue
        final = persist_data(counts, connection, good_row)
        writer.writerow(final)


def get_options(argv = sys.argv[1:]):
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", action="store", default="unlearning_sql.db")
    parser.add_argument("-o", "--output", action="store", type=Path, default=Path("activation_load.csv"))
    parser.add_argument("source", nargs=1, type=Path, default=Path("activation_source.data"))
    return parser.parse_args(argv)


def main(argv = sys.argv[1:]):
    options = get_options()
    source = options.source[0]
    target = options.output
    database_connect = options.db
    connection = db.connect(database_connect)
    counts = Counter()
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
    print(f"Transformed {counts['transform']} rows")
    print(f"Saved {counts['saved']} rows")
    
if __name__ == "__main__":
    main()
