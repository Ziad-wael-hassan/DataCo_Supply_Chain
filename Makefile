up:
	docker compose up

down:
	docker compose down

load:
	python3 scripts/load_raw.py

validate:
	python3 scripts/validate_raw.py

dbt:
	cd dbt/dataco_analytics && dbt run

test:
	cd dbt/dataco_analytics && dbt test

pipeline: load validate dbt test
