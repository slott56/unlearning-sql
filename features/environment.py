"""Behave test environment"""
from pathlib import Path
import sqlite3
from textwrap import dedent

def before_all(context):
    """
    Create test database and global connection.
    Each feature creates and drops the feature's schema.
    """
    connection = sqlite3.connect("temp.db")
    context.config.connection = connection

def after_all(context):
    """Remove test database"""
    Path("temp.db").unlink()
    
def before_feature(context, feature):
    print(f"Building schema for {feature}")
    connection = context.config.connection
    
    cursor = connection.cursor()
    cursor.execute(dedent("""
        CREATE TABLE service(
            service_name CHAR(16)
        )
    """))
    cursor.execute(dedent("""
        CREATE TABLE customer(
            customer_name CHAR(16)
        )
    """))
    cursor.execute(dedent("""
            CREATE TABLE customer_device(
            customer_id INTEGER REFERENCES customer(rowid),
            device_name CHAR(64)
        )
    """))
    connection.commit()
    cursor.close()

def before_scenario(context, scenario):
    context.config.cleanup = []
    
def after_scenario(context, scenario):
    for path in context.config.cleanup:
        print(f"Removing {path}")
        try:
            path.unlink()
        except FileNotFoundError:
            pass
