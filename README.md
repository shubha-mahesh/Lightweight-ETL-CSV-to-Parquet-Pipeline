# Lightweight ETL — CSV to Parquet Pipeline

**Short summary :**  
Built an end-to-end ETL pipeline to ingest large CSV datasets, perform schema validation & cleaning, and write partitioned Parquet outputs (partitioned by `date` and `event_type`). Implemented idempotent incremental loads, transformation unit tests, and documentation for reproducible runs.

> NOTE: This repository includes a synthetic data generator (`generate_synthetic_data.py`) so you can produce demo datasets locally. Always label demo results as *synthetic* when publishing.

## What’s included
- `generate_synthetic_data.py` — generates synthetic CSV data for demo/testing
- `etl_pipeline.py` — reads CSV in chunks, validates, and writes partitioned Parquet using PyArrow
- `validate_schema.py` — validation & casting logic
- `tests/` — pytest unit tests
- `Dockerfile` — optional for containerized runs

## How to run (local)
```bash
git clone <repo-url>
cd project-root
python -m pip install -r requirements.txt

# generate demo data (100k rows)
python generate_synthetic_data.py --rows 100000 --out data/events.csv

# run pipeline
python etl_pipeline.py --input data/events.csv --out-dir output/parquet --chunk-size 20000

# run tests
pytest -q
