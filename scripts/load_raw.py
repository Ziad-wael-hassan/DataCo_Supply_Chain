"""
Incrementally loads DataCoSupplyChainDataset.csv into raw.orders_raw.

On each Airflow daily run:
  1. Create an etl_runs row (status=in_progress) to get a run_id
  2. CSV -> temporary staging table (raw.orders_raw_staging)
  3. Stamp staging rows with etl_run_id
  4. PostgreSQL NOT EXISTS inserts only rows whose "Order Item Id"
     is not already in raw.orders_raw
  5. Staging table is dropped
  6. etl_runs row is finalized with load stats

If zero new rows exist the task succeeds without inserting.
Never truncates, never drops, never replaces the raw table.

Handoff to validate_raw.py goes through warehouse.etl_runs (not /tmp files),
so this is safe across CeleryExecutor / KubernetesExecutor.
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
STAGING_TABLE = "orders_raw_staging"

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


# ---------------------------------------------------------------------------
# Schema + run management
# ---------------------------------------------------------------------------

def _ensure_raw_schema(engine):
    """Create the raw schema if it doesn't exist. Clean up crashed-run leftovers."""
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.execute(text(ETL_RUNS_DDL))
        conn.execute(text("DROP TABLE IF EXISTS raw.orders_raw_staging"))


def _create_etl_run(engine, start_ts):
    """Create an etl_runs row with status='in_progress' and return the run_id."""
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO warehouse.etl_runs
                    (pipeline_name, start_time, validation_status)
                VALUES ('supply_chain_pipeline', :start_time, 'in_progress')
                RETURNING run_id
                """
            ),
            {"start_time": start_ts},
        )
        run_id = result.scalar()
    log.info("Created etl_run      : run_id=%d", run_id)
    return run_id


def _finalize_etl_run(engine, run_id, end_ts, duration_ms, rows_loaded):
    """Update the etl_runs row with final load stats."""
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE warehouse.etl_runs
                SET end_time = :end_time,
                    duration_ms = :duration_ms,
                    rows_loaded = :rows_loaded
                WHERE run_id = :run_id
                """
            ),
            {
                "end_time": end_ts,
                "duration_ms": duration_ms,
                "rows_loaded": rows_loaded,
                "run_id": run_id,
            },
        )
    log.info("Finalized etl_run    : run_id=%d, rows_loaded=%d", run_id, rows_loaded)


# ---------------------------------------------------------------------------
# CSV + staging
# ---------------------------------------------------------------------------

def _read_csv():
    """Read the source CSV and return the DataFrame."""
    log.info("Reading raw CSV ...")
    df = pd.read_csv(CSV_PATH, encoding="ISO-8859-1")
    log.info("CSV rows             : %d", len(df))
    return df


def _load_staging(engine, df, run_id):
    """Load CSV into a temporary staging table, then stamp with etl_run_id."""
    df.to_sql(
        STAGING_TABLE,
        engine,
        schema="raw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        method="multi",
    )
    with engine.begin() as conn:
        conn.execute(
            text('ALTER TABLE raw."orders_raw_staging" ADD COLUMN etl_run_id INT')
        )
        conn.execute(
            text('ALTER TABLE raw."orders_raw_staging" ADD COLUMN ingested_at TIMESTAMP DEFAULT now()')
        )
        conn.execute(
            text('UPDATE raw."orders_raw_staging" SET etl_run_id = :run_id'),
            {"run_id": run_id},
        )
    log.info("Staging table loaded : %d rows, etl_run_id=%d", len(df), run_id)


# ---------------------------------------------------------------------------
# Counting + inserting
# ---------------------------------------------------------------------------

def _count_existing(engine):
    """Return the current row count of raw.orders_raw (0 if table absent)."""
    with engine.begin() as conn:
        result = conn.execute(
            text(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.tables"
                "  WHERE table_schema = 'raw' AND table_name = 'orders_raw'"
                ")"
            )
        )
        exists = result.scalar()

        if not exists:
            return 0

        result = conn.execute(text('SELECT COUNT(*) FROM raw.orders_raw'))
        return result.scalar()


def _insert_new_rows(engine):
    """
    Insert rows from staging that don't already exist in raw.orders_raw.

    Handles both cases:
      - Table doesn't exist -> CREATE TABLE IF NOT EXISTS copies schema
        from staging (including etl_run_id), then INSERT ... SELECT fills it.
      - Table exists -> NOT EXISTS filters to new rows only.
      - Pre-existing table without etl_run_id -> ALTER TABLE adds it.

    Returns the number of inserted rows.
    """
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS raw.orders_raw "
            "(LIKE raw.orders_raw_staging INCLUDING DEFAULTS)"
        ))
        # Handle pre-existing table without etl_run_id / ingested_at
        conn.execute(text(
            "DO $$ BEGIN "
            "ALTER TABLE raw.orders_raw ADD COLUMN IF NOT EXISTS etl_run_id INT; "
            "EXCEPTION WHEN undefined_table THEN NULL; END $$;"
        ))
        conn.execute(text(
            "DO $$ BEGIN "
            "ALTER TABLE raw.orders_raw ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP DEFAULT now(); "
            "EXCEPTION WHEN undefined_table THEN NULL; END $$;"
        ))

        result = conn.execute(text(
            "INSERT INTO raw.orders_raw "
            'SELECT s.* '
            "FROM raw.orders_raw_staging s "
            'WHERE NOT EXISTS ('
            '  SELECT 1 FROM raw.orders_raw e '
            '  WHERE e."Order Item Id" = s."Order Item Id"'
            ")"
        ))
        return result.rowcount


def _drop_staging(engine):
    """Drop the temporary staging table."""
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS raw.orders_raw_staging"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    start = time.time()
    start_ts = datetime.now()

    engine = create_engine(POSTGRES_URI)

    # --- 0. Schema --------------------------------------------------------
    _ensure_raw_schema(engine)

    # --- 1. Create etl_run row (in_progress) to get run_id ---------------
    run_id = _create_etl_run(engine, start_ts)

    # --- 2. CSV + staging + etl_run_id stamp -----------------------------
    df = _read_csv()
    csv_rows = len(df)
    already_loaded = _count_existing(engine)
    log.info("Already loaded       : %d", already_loaded)

    # Short-circuit: if the source CSV is append-only and every row is
    # already in raw.orders_raw, skip the staging/insert cycle entirely.
    if already_loaded == csv_rows:
        log.info("-----------------------------------")
        log.info("No new rows detected (%d/%d). Skipping load.",
                 already_loaded, csv_rows)
        log.info("-----------------------------------")
        end_ts = datetime.now()
        duration_ms = int((time.time() - start) * 1000)
        _finalize_etl_run(engine, run_id, end_ts, duration_ms, rows_loaded=0)
        log.info("Done. run_id=%d", run_id)
        return

    _load_staging(engine, df, run_id)

    # --- 3. Insert only new rows (PostgreSQL NOT EXISTS) ------------------
    inserted = _insert_new_rows(engine)
    log.info("Inserted             : %d", inserted)
    log.info("New rows detected    : %d", inserted)
    log.info("Skipped              : %d", already_loaded)

    # --- 4. Drop staging table --------------------------------------------
    _drop_staging(engine)

    # --- 5. Finalize etl_run row ------------------------------------------
    end_ts = datetime.now()
    duration_ms = int((time.time() - start) * 1000)
    log.info("Load duration        : %.1f sec", duration_ms / 1000)

    if inserted == 0:
        log.info("-----------------------------------")
        log.info("No new rows detected.")
        log.info("Skipping raw load.")
        log.info("-----------------------------------")

    _finalize_etl_run(engine, run_id, end_ts, duration_ms, inserted)
    log.info("Done. run_id=%d", run_id)


if __name__ == "__main__":
    main()
