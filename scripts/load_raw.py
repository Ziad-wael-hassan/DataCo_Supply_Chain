"""
Loads the ORIGINAL, untouched DataCoSupplyChainDataset.csv into raw.orders_raw.
No cleaning here -- that's dbt's job now (see models/staging/stg_orders.sql).

Run this once before `dbt run`. Also records a row in warehouse.etl_runs so
pipeline health/runtime becomes queryable (see PRE_DASHBOARD_CHECKLIST.md step 5).
"""
import logging
import os
import time
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("load_raw")

CSV_PATH = "raw/DataCoSupplyChainDataset.csv"
POSTGRES_URI = os.getenv(
    "POSTGRES_URI", "postgresql://postgres:postgres@localhost:5432/postgres"
)
RUN_ID_FILE = "/tmp/.last_run_id"  # simple handoff so validate_raw.py can update the same run row

ETL_RUNS_DDL = """
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE TABLE IF NOT EXISTS warehouse.etl_runs (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_ms INT,
    rows_loaded INT,
    rows_failed INT,
    validation_status VARCHAR
);
"""


def main():
    start = time.time()
    start_ts = datetime.now()

    engine = create_engine(POSTGRES_URI)

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.execute(text(ETL_RUNS_DDL))

    log.info("Reading raw CSV (no cleaning)...")
    df = pd.read_csv(CSV_PATH, encoding="ISO-8859-1")

    log.info(f"Loading {len(df):,} rows into raw.orders_raw...")
    with engine.begin() as conn:
        conn.execute(text(
            "DROP VIEW IF EXISTS warehouse_staging.stg_orders;"
        ))
        conn.execute(text(
            "DO $$ BEGIN "
            "IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='raw' AND tablename='orders_raw') THEN "
            "  TRUNCATE TABLE raw.orders_raw; "
            "END IF; END $$;"
        ))
    df.to_sql(
        "orders_raw",
        engine,
        schema="raw",
        if_exists="append",
        index=False,
        chunksize=10000,
        method="multi",
    )

    end_ts = datetime.now()
    duration_ms = int((time.time() - start) * 1000)

    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO warehouse.etl_runs
                    (pipeline_name, start_time, end_time, duration_ms, rows_loaded, rows_failed, validation_status)
                VALUES
                    (:pipeline_name, :start_time, :end_time, :duration_ms, :rows_loaded, :rows_failed, :validation_status)
                RETURNING run_id
                """
            ),
            {
                "pipeline_name": "supply_chain_pipeline",
                "start_time": start_ts,
                "end_time": end_ts,
                "duration_ms": duration_ms,
                "rows_loaded": len(df),
                "rows_failed": 0,
                "validation_status": "pending",
            },
        )
        run_id = result.scalar()

    with open(RUN_ID_FILE, "w") as f:
        f.write(str(run_id))

    log.info(f"Done in {duration_ms}ms. run_id={run_id}. Raw table ready -- now run: dbt run")


if __name__ == "__main__":
    main()
