"""
validate_raw.py
===============
Validates raw.orders_raw with pandas BEFORE dbt touches it.
Catches bad source data (nulls where there shouldn't be,
negative sales, zero/negative quantities, duplicate order items) at the
landing layer, so garbage never reaches stg_orders / the dims / the fact.

Exits non-zero on validation failure, which fails the Airflow task and
stops dbt_run from ever running against bad data.  Also updates the
warehouse.etl_runs row created by load_raw.py with the validation outcome.

Handoff from load_raw.py goes through warehouse.etl_runs, not /tmp files,
so this is safe across CeleryExecutor / KubernetesExecutor.

Run standalone:
    python scripts/validate_raw.py

Requires:
    pip install pandas sqlalchemy psycopg2-binary python-dotenv
"""
from __future__ import annotations

import logging
import os
import sys

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("validate_raw")

POSTGRES_URI = os.getenv(
    "POSTGRES_URI", "postgresql://postgres:postgres@localhost:5432/postgres"
)


# ---------------------------------------------------------------------------
# etl_runs interaction
# ---------------------------------------------------------------------------

def _get_pending_run_id(engine):
    """
    Return the run_id of the latest pending etl_runs row.

    Guards against stale rows: ORDER BY run_id DESC LIMIT 1 picks the
    most recent load_raw.py run that hasn't been validated yet.
    """
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT run_id FROM warehouse.etl_runs "
                "WHERE validation_status = 'in_progress' "
                "ORDER BY run_id DESC LIMIT 1"
            )
        )
        row = result.fetchone()
        if row is None:
            return None
        return row[0]


def _update_etl_run(engine, run_id, status, rows_failed):
    """Update the etl_runs row with the validation outcome."""
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE warehouse.etl_runs
                SET validation_status = :status,
                    rows_failed = :rows_failed
                WHERE run_id = :run_id
                """
            ),
            {"status": status, "rows_failed": rows_failed, "run_id": run_id},
        )
    log.info("Updated etl_runs     : run_id=%d -> validation_status=%s", run_id, status)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate(df):
    errors = []

    for col in ["Order Id", "Order Item Id", "Product Card Id", "order date (DateOrders)"]:
        null_count = df[col].isna().sum()
        if null_count > 0:
            errors.append(f"{col}: {null_count} null values (expected 0)")

    dup_count = df["Order Item Id"].duplicated().sum()
    if dup_count > 0:
        errors.append(f"Order Item Id: {dup_count} duplicates (expected unique)")

    bad_qty = (df["Order Item Quantity"] < 1).sum()
    if bad_qty > 0:
        errors.append(f"Order Item Quantity: {bad_qty} values < 1")

    non_cancelled = df[~df["Order Status"].isin(["CANCELED", "SUSPECTED_FRAUD"])]
    bad_sales = (non_cancelled["Sales"] < 0).sum()
    if bad_sales > 0:
        errors.append(f"Sales: {bad_sales} negative values in non-cancelled orders")

    bad_discount = ((df["Order Item Discount Rate"] < 0) | (df["Order Item Discount Rate"] > 1)).sum()
    if bad_discount > 0:
        errors.append(f"Order Item Discount Rate: {bad_discount} values outside [0, 1]")

    bad_risk = (~df["Late_delivery_risk"].isin([0, 1])).sum()
    if bad_risk > 0:
        errors.append(f"Late_delivery_risk: {bad_risk} values not in {{0, 1}}")

    return errors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    engine = create_engine(POSTGRES_URI)

    # --- 1. Find the pending run from load_raw.py -------------------------
    run_id = _get_pending_run_id(engine)
    if run_id is None:
        log.warning("No pending etl_runs row found. Was load_raw.py run first?")
        log.info("VALIDATION SKIPPED -- nothing to validate.")
        sys.exit(0)

    log.info("Validating rows from etl_run_id=%d ...", run_id)

    # --- 2. Load only rows from this run (incremental validation) ---------
    df = pd.read_sql(
        text("SELECT * FROM raw.orders_raw WHERE etl_run_id = :run_id"),
        engine,
        params={"run_id": run_id},
    )
    log.info("Loaded %d rows for validation.", len(df))

    if df.empty:
        log.info("No rows with etl_run_id=%d -- nothing to validate.", run_id)
        _update_etl_run(engine, run_id, status="passed", rows_failed=0)
        sys.exit(0)

    # --- 3. Run validation checks -----------------------------------------
    errors = validate(df)

    if errors:
        for e in errors:
            log.error("FAIL: %s", e)
        log.error("VALIDATION FAILED -- %d check(s) failed. Stopping before dbt runs.", len(errors))
        _update_etl_run(engine, run_id, status="failed", rows_failed=len(errors))
        sys.exit(1)

    log.info("All validation checks passed. Safe to proceed to dbt_run.")
    _update_etl_run(engine, run_id, status="passed", rows_failed=0)
    sys.exit(0)


if __name__ == "__main__":
    main()
