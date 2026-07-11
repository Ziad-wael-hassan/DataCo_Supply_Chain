"""
validate_raw.py
===============
Validates raw.orders_raw with pandas BEFORE dbt touches it.
Catches bad source data (nulls where there shouldn't be,
negative sales, zero/negative quantities, duplicate order items) at the
landing layer, so garbage never reaches stg_orders / the dims / the fact.

Exits non-zero on validation failure, which fails the Airflow task and
stops dbt_run from ever running against bad data. Also updates the
warehouse.etl_runs row created by load_raw.py with the validation outcome.

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
RUN_ID_FILE = "/tmp/.last_run_id"


def update_etl_run(engine, status: str, rows_failed: int):
    if not os.path.exists(RUN_ID_FILE):
        log.warning(f"{RUN_ID_FILE} not found -- skipping etl_runs update (was load_raw.py run first?)")
        return

    with open(RUN_ID_FILE) as f:
        run_id = f.read().strip()

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE warehouse.etl_runs
                SET validation_status = :status, rows_failed = :rows_failed
                WHERE run_id = :run_id
                """
            ),
            {"status": status, "rows_failed": rows_failed, "run_id": run_id},
        )
    log.info(f"Updated etl_runs run_id={run_id} -> validation_status={status}")


def validate(df: pd.DataFrame) -> list[str]:
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


def main():
    engine = create_engine(POSTGRES_URI)

    log.info("Reading raw.orders_raw for validation...")
    df = pd.read_sql("SELECT * FROM raw.orders_raw", engine)
    log.info(f"Loaded {len(df):,} rows.")

    errors = validate(df)

    if errors:
        for e in errors:
            log.error(f"FAIL: {e}")
        log.error(f"VALIDATION FAILED -- {len(errors)} check(s) failed. Stopping before dbt runs.")
        update_etl_run(engine, status="failed", rows_failed=len(errors))
        sys.exit(1)

    log.info("All 9 validation checks passed. Safe to proceed to dbt_run.")
    update_etl_run(engine, status="passed", rows_failed=0)
    sys.exit(0)


if __name__ == "__main__":
    main()
