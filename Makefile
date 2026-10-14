PY ?= python
LOANS ?= 20000
DBT = cd dbt/loan_warehouse && DBT_SEND_ANONYMOUS_USAGE_STATS=false dbt

.PHONY: all data silver dbt report test clean

all: data silver dbt report  ## full pipeline: generate -> Spark -> dbt -> report

data:  ## generate synthetic bronze extracts
	PYTHONPATH=src $(PY) -m loanrisk.generate --loans $(LOANS)

silver:  ## Spark: clean, dedupe, quarantine -> silver Parquet
	PYTHONPATH=src $(PY) -m loanrisk.spark_silver

dbt:  ## build warehouse models and run all data tests
	$(DBT) build --profiles-dir .

report:  ## regenerate docs/results.md and charts
	$(PY) scripts/report.py

test:  ## unit tests for the generator and Spark transforms
	$(PY) -m pytest -q

clean:
	rm -rf data dbt/loan_warehouse/target dbt/loan_warehouse/logs
