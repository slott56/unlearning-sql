.PHONY : diagrams sql_load python_load test acceptance

SOURCE_DIAGRAMS = database.png

diagrams: $(SOURCE_DIAGRAMS)

$(SOURCE_DIAGRAMS) : database.uml
	java -jar plantuml-1.2022.6.jar $<

unlearning_sql.db activation_source.data : activation_source.schema.json fake_data.py
	python fake_data.py
	python sql_db_preparation.py
	
sql_load: unlearning_sql.db activation_source.data sql_load_process.py
	python sql_load_process.py

python_load: unlearning_sql.db activation_source.data python_load_process.py
	python python_load_process.py --db unlearning_sql.db -o activation_load.csv activation_source.data

test:
	pytest

acceptance:
	behave
