"""
Step definitions for behave testing.
"""
from behave import given, when, then
import csv
from pathlib import Path
import subprocess
from textwrap import dedent
    
    
@given(u'a file of activations')
def step_impl(context):
    temp_path = Path("data/temp_activations.data")
    context.config.cleanup.append(temp_path)
    with temp_path.open('w', newline='') as temp_file:
        writer = csv.writer(temp_file)
        writer.writerow(context.table.headings)
        for row in context.table:
            writer.writerow(row.cells)

@given(u'a working test database')
def step_impl(context):
    assert context.config.connection is not None


@given(u'the database has matching customers')
def step_impl(context):
    customer_map = {}
    cursor = context.config.connection.cursor()
    for row in context.table:
        cursor.execute(
            dedent("""
            INSERT INTO customer(customer_name)
                VALUES(:customer_name)
            """),
            {"customer_name": row.cells[0]}
        )
        customer_map[cursor.lastrowid] = row.cells
    cursor.close()
    context.config.connection.commit()
    context.config.customer_map = customer_map


@given(u'the database has matching customer-device ownership')
def step_impl(context):
    customer_device_map = {}
    cursor = context.config.connection.cursor()
    for row in context.table:
        cursor.execute(
            dedent("""
            INSERT INTO customer_device(customer_id, device_name)
                SELECT rowid, :device_name
                FROM customer
                WHERE customer_name = :customer_name
            """),
            {"customer_name": row.cells[0], "device_name": row.cells[1]}
        )
        customer_device_map[cursor.lastrowid] = row.cells
    cursor.close()
    context.config.connection.commit()
    context.config.customer_device_map = customer_device_map

@given(u'the database has matching service names')
def step_impl(context):
    service_map = {}
    cursor = context.config.connection.cursor()
    for row in context.table:
        cursor.execute(
            dedent("""
            INSERT INTO service(service_name)
                VALUES(:service_name)
            """),
            {"service_name": row.cells[0]}
        )
        service_map[cursor.lastrowid] = row.cells
    cursor.close()
    context.config.connection.commit()
    context.config.service_map = service_map

@when(u'the python_load_process.py application is run')
def step_impl(context):
    context.config.cleanup.append(Path("data/temp_load.csv"))
    command = [
        "python", "src/python_load_process.py",
            "--db", "data/temp.db",
            "-o", "data/temp_load.csv",
            "data/temp_activations.data"
    ]
    capture_path = Path("data/temp_output.log")
    try:
        with capture_path.open('w') as capture:
            subprocess.run(command,
                check=True, text=True,
                stdout=capture, stderr=subprocess.STDOUT
            )
        context.config.process_log = capture_path.read_text()
        print(context.config.process_log)
        context.config.cleanup.append(capture_path)
    except subprocess.CalledProcessError as ex:
        print(ex)
        print("LOG")
        print(capture_path.read_text())
        raise


@then(u'the {saved} record(s) with valid data are loaded')
def step_impl(context, saved):
    assert f"Saved {saved} rows" in context.config.process_log


@then(u'the {invalid} record(s) with invalid data are ignored')
def step_impl(context, invalid):
    assert f"Invalid {invalid} rows" in context.config.process_log


@then(u'the output file has 1 valid record')
def step_impl(context):
    output_path = Path("data/temp_load.csv")
    with output_path.open() as output_file:
        output_reader = csv.DictReader(output_file)
        results = list(output_reader)
    assert results == [
        {
            'customer_device_id': '1', 
            'service_id': '1', 
            'start_date': '2022-07-10 11:12:13+00:00',
            'latitude': '35.35472166666667', 
            'longitude': '-82.52722166666666'
        }
    ]
    row, = results
    customer_device = context.config.customer_device_map[
        int(row['customer_device_id'])
    ]
    assert customer_device == ['customer x', 'device y'], f"unexpected {customer_device}"
    service = context.config.service_map[
        int(row['service_id'])
    ]
    assert service == ['service z'], f"unexpected {service}"
