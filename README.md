# unlearning-sql
The examples from the Unlearning SQL book

## Installation

This requires Python 3.10

The `requirements.txt` file lists other packages that are 
used by the various applications.

```sh
    pytthon -m pip install -r requirements.txt
```

There's no `setup.py` because there's nothing
to install for these demo applications.

## General Operation

In order to play with the examples, it's helpful
to have a file named `activate_source.data`.
This file contains the raw data to be bulk-loaded
into the database.

It's also helpful, of course, to have a database.
In this case, it will be `unlearning_sql.db`. 

To build the file, and the database, execute
the following two steps:

```sh
python fake_data.py
python sql_db_preparation.py
```

The `fake_data.py` program builds the `activate_source.data` file.
The `sql_db_preparation.py` program loads the database with some (but not all) of the fake data.
This omission of data permits some of the data validation rules to spot 
bad data in the input.

## Makefile

The `Makefile` has targets to help.

```sh
make sql_load
```

The `sql_load` target will build the fake data and populate the database if needed.
It will run the SQL-based loader application.

```sh
make python_load
```

The `python_load` target will build the fake data and populate the database if needed.
It will run the Python-based loader application.

## Diagrams

The diagrams were built with PlantUML. See https://plantuml.com for more information.
This is not required to run the demonstration applications.
It's handy for creating entity-relationship diagrams.
