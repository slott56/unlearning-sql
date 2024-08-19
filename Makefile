.PHONY : diagrams db_prep sql_load python_load test acceptance

SOURCE_DIAGRAMS = docs/database.png

diagrams: $(SOURCE_DIAGRAMS)

$(SOURCE_DIAGRAMS): docs/database.uml
	java -jar plantuml-1.2024.6.jar $<

# Make a database for use with the sql_load example.
db_prep: data/activation_source.csv src/fake_data.py src/sql_db_preparation.py
	python src/fake_data.py --schema activation_source.schema --output data/activation_source.csv
	python src/sql_db_preparation.py --schema activation_source.schema --db data/unlearning_sql.db data/activation_source.csv

# Load using SQL commands
sql_load: data/unlearning_sql.db data/activation_source.csv src/sql_load_process.py
	python src/sql_load_process.py --db data/unlearning_sql.db data/activation_source.csv

define sqlite_script_text
sqlite3 data/unlearning_sql.db <<EOF
.import -v --csv --skip 1 data/activation_load.csv CUSTOMER_DEVICE_SERVICE
EOF
endef
export sqlite_load = $(value sqlite_script_text)

# Load using Pure Python
python_load: data/unlearning_sql.db data/activation_source.csv src/python_load_process.py
	python src/python_load_process.py --db data/unlearning_sql.db -o data/activation_load.csv data/activation_source.csv
	# sqlite bulk import
	@ eval "$$sqlite_load"
	python src/python_extract_1.py --db data/unlearning_sql.db -o data/service_name_counts.csv

test:
	PYTHONPATH=src pytest
	ruff check src

acceptance:
	behave
