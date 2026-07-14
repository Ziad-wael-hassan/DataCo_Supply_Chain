"""
publish_to_neon.py — Synchronize final warehouse tables to Neon.

Reads from local PostgreSQL (SQLAlchemy), writes to Neon (psycopg2 execute_values)
for batch insert performance. Each table wrapped in its own transaction.

Sync modes (configured in config/sync_tables.yml):
  full         — Compare PK counts; if different, DELETE + batch INSERT
  upsert       — Compare PKs; INSERT only new rows (append-only)
  incremental  — Filter by date_column > last_sync; batch UPSERT

Usage:
    python scripts/publish_to_neon.py              # incremental sync
    python scripts/publish_to_neon.py --full-sync  # force full sync all tables
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import psycopg2
import psycopg2.extras
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
POSTGRES_URI: str = os.getenv(
    "POSTGRES_URI", "postgresql://postgres:postgres@localhost:5432/postgres"
)

NEON_HOST: str = os.getenv("NEON_HOST", "")
NEON_PORT: str = os.getenv("NEON_PORT", "5432")
NEON_DATABASE: str = os.getenv("NEON_DATABASE", "")
NEON_USER: str = os.getenv("NEON_USER", "")
NEON_PASSWORD: str = os.getenv("NEON_PASSWORD", "")

SYNC_CONFIG_PATH: Path = (
    Path(__file__).resolve().parent.parent / "config" / "sync_tables.yml"
)

NEON_PAGE_SIZE: int = 10000


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _log(msg: str) -> None:
    print(msg)


def _log_section(msg: str) -> None:
    print(f"\n{'─' * 50}")
    print(msg)
    print(f"{'─' * 50}")


# ---------------------------------------------------------------------------
# Neon connection
# ---------------------------------------------------------------------------
def get_neon_dsn() -> str:
    return (
        f"dbname={NEON_DATABASE} user={NEON_USER} password={NEON_PASSWORD}"
        f" host={NEON_HOST} port={NEON_PORT} sslmode=require"
    )


def get_neon_conn():
    return psycopg2.connect(get_neon_dsn())


# ---------------------------------------------------------------------------
# Local engine (SQLAlchemy for reads)
# ---------------------------------------------------------------------------
def create_local_engine() -> Engine:
    return create_engine(POSTGRES_URI)


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------
def load_sync_config() -> List[Dict[str, Any]]:
    with open(SYNC_CONFIG_PATH) as f:
        return yaml.safe_load(f)["tables"]


# ---------------------------------------------------------------------------
# Schema introspection (SQLAlchemy, read-only)
# ---------------------------------------------------------------------------
def get_columns(engine: Engine, schema: str, table: str) -> List[str]:
    inspector = inspect(engine)
    return [col["name"] for col in inspector.get_columns(table, schema=schema)]


def table_exists(engine: Engine, schema: str, table: str) -> bool:
    return inspect(engine).has_table(table, schema=schema)


def count_rows(engine: Engine, schema: str, table: str) -> int:
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT count(*) FROM {schema}.{table}")
        )
        return result.scalar()


def get_pks_neon(schema: str, table: str, pk: str) -> Set[Any]:
    conn = get_neon_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT {pk} FROM {schema}.{table}")
        pks = {row[0] for row in cur.fetchall()}
        cur.close()
        return pks
    finally:
        conn.close()


def get_max_timestamp_neon(table: str, date_column: str) -> Optional[str]:
    conn = get_neon_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT MAX({date_column}) FROM {table}")
        val = cur.fetchone()[0]
        cur.close()
        return str(val) if val is not None else None
    finally:
        conn.close()


def read_local(engine: Engine, schema: str, table: str) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        return [row._asdict() for row in conn.execute(text(f"SELECT * FROM {schema}.{table}"))]


# ---------------------------------------------------------------------------
# Neon write helpers (psycopg2 execute_values)
# ---------------------------------------------------------------------------
def _neon_delete_all(schema: str, table: str) -> None:
    conn = get_neon_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {schema}.{table}")
        conn.commit()
        cur.close()
    finally:
        conn.close()


def _neon_insert(schema: str, table: str, columns: List[str], rows: List[tuple]) -> None:
    cols_str = ", ".join(columns)
    sql = f"INSERT INTO {schema}.{table} ({cols_str}) VALUES %s"
    chunk_size = NEON_PAGE_SIZE
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        conn = get_neon_conn()
        try:
            cur = conn.cursor()
            psycopg2.extras.execute_values(cur, sql, chunk, page_size=NEON_PAGE_SIZE)
            conn.commit()
            cur.close()
        finally:
            conn.close()


def _neon_insert_on_conflict_do_nothing(
    schema: str, table: str, columns: List[str], rows: List[tuple], pk: str
) -> None:
    cols_str = ", ".join(columns)
    sql = f"INSERT INTO {schema}.{table} ({cols_str}) VALUES %s ON CONFLICT ({pk}) DO NOTHING"
    chunk_size = NEON_PAGE_SIZE
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        conn = get_neon_conn()
        try:
            cur = conn.cursor()
            psycopg2.extras.execute_values(cur, sql, chunk, page_size=NEON_PAGE_SIZE)
            conn.commit()
            cur.close()
        finally:
            conn.close()


def _neon_upsert(
    schema: str, table: str, columns: List[str], rows: List[tuple], pk: str
) -> None:
    cols_str = ", ".join(columns)
    update_clause = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in columns if c != pk
    )
    sql = (
        f"INSERT INTO {schema}.{table} ({cols_str}) VALUES %s"
        f" ON CONFLICT ({pk}) DO UPDATE SET {update_clause}"
    )
    chunk_size = NEON_PAGE_SIZE
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        conn = get_neon_conn()
        try:
            cur = conn.cursor()
            psycopg2.extras.execute_values(cur, sql, chunk, page_size=NEON_PAGE_SIZE)
            conn.commit()
            cur.close()
        finally:
            conn.close()


def _rows_to_tuples(rows: List[Dict[str, Any]], columns: List[str]) -> List[tuple]:
    return [tuple(row[c] for c in columns) for row in rows]


def _reset_neon_sequences(schema: str, table: str, serial_columns: Set[str]) -> None:
    conn = get_neon_conn()
    try:
        cur = conn.cursor()
        for col in serial_columns:
            seq_name = f"{schema}.{table}_{col}_seq"
            cur.execute(
                f"SELECT setval('{seq_name}', (SELECT COALESCE(MAX({col}), 0) FROM {schema}.{table}))"
            )
            val = cur.fetchone()[0]
            _log(f"  Reset sequence {seq_name} to {val}")
        conn.commit()
        cur.close()
    finally:
        conn.close()



# ---------------------------------------------------------------------------
# Sync: full (dim tables)
# ---------------------------------------------------------------------------
def sync_full(
    local_engine: Engine,
    schema: str,
    table: str,
    pk: str,
    force: bool = False,
) -> Dict[str, Any]:
    full_table = f"{schema}.{table}"
    local_count = count_rows(local_engine, schema, table)

    if not force:
        try:
            neon_count_rows = get_pks_neon(schema, table, pk)
            neon_count = len(neon_count_rows)
        except Exception:
            neon_count = 0

        if local_count == neon_count and neon_count > 0:
            _log(f"  Skipped (no change: {local_count:,} rows)")
            return {"inserted": 0, "updated": 0, "skipped": local_count, "mode": "skip"}

    columns = get_columns(local_engine, schema, table)
    rows = read_local(local_engine, schema, table)
    tuples = _rows_to_tuples(rows, columns)

    _neon_delete_all(schema, table)
    _neon_insert(schema, table, columns, tuples)

    _log(f"  Full sync: {local_count:,} rows")
    return {"inserted": local_count, "updated": 0, "skipped": 0, "mode": "full"}


# ---------------------------------------------------------------------------
# Sync: upsert (append-only — insert new PKs only)
# ---------------------------------------------------------------------------
def sync_upsert(
    local_engine: Engine,
    schema: str,
    table: str,
    pk: str,
) -> Dict[str, Any]:
    full_table = f"{schema}.{table}"
    local_count = count_rows(local_engine, schema, table)

    neon_pks = get_pks_neon(schema, table, pk)
    if len(neon_pks) == 0:
        _log(f"  Neon table empty — inserting all {local_count:,} rows")
    else:
        _log(f"  Local: {local_count:,} rows | Neon: {len(neon_pks):,} PKs")

    columns = get_columns(local_engine, schema, table)
    all_rows = read_local(local_engine, schema, table)
    new_rows = [r for r in all_rows if r[pk] not in neon_pks]

    if not new_rows:
        _log(f"  No new rows to insert")
        return {"inserted": 0, "updated": 0, "skipped": local_count, "mode": "upsert"}

    tuples = _rows_to_tuples(new_rows, columns)
    _neon_insert_on_conflict_do_nothing(schema, table, columns, tuples, pk)

    _log(f"  Inserted: {len(new_rows):,} new rows")
    return {"inserted": len(new_rows), "updated": 0, "skipped": local_count - len(new_rows), "mode": "upsert"}


# ---------------------------------------------------------------------------
# Sync: incremental (filter by date_column)
# ---------------------------------------------------------------------------
def sync_incremental(
    local_engine: Engine,
    schema: str,
    table: str,
    pk: str,
    date_column: str,
    exclude_columns: Optional[List[str]] = None,
) -> Dict[str, Any]:
    full_table = f"{schema}.{table}"

    last_sync = get_max_timestamp_neon(full_table, date_column)
    if last_sync:
        _log(f"  Last sync: {last_sync}")
    else:
        _log(f"  Neon table empty — full insert")

    columns = get_columns(local_engine, schema, table)

    if last_sync:
        query = text(
            f"SELECT * FROM {schema}.{table} WHERE {date_column} > :last_sync"
        )
        with local_engine.connect() as conn:
            rows = [row._asdict() for row in conn.execute(query, {"last_sync": last_sync})]
    else:
        rows = read_local(local_engine, schema, table)

    local_count = count_rows(local_engine, schema, table)

    if not rows:
        _log(f"  No changed rows (total local: {local_count:,})")
        return {"inserted": 0, "updated": 0, "skipped": local_count, "mode": "incremental"}

    skip = set(exclude_columns or [])
    insert_columns = [c for c in columns if c not in skip]

    if skip:
        _reset_neon_sequences(schema, table, skip)

    insert_rows = [tuple(row[c] for c in insert_columns) for row in rows]

    _neon_upsert(schema, table, insert_columns, insert_rows, pk)

    _log(f"  Upserted: {len(rows):,} rows (total local: {local_count:,})")
    return {
        "inserted": len(rows),
        "updated": 0,
        "skipped": local_count - len(rows),
        "mode": "incremental",
    }


# ---------------------------------------------------------------------------
# Per-table orchestration
# ---------------------------------------------------------------------------
def sync_table(
    local_engine: Engine,
    table_config: Dict[str, Any],
    force_full: bool = False,
) -> Dict[str, Any]:
    schema = table_config["schema"]
    table = table_config["table"]
    pk = table_config["pk"]
    sync_mode = table_config["sync_mode"]
    date_column = table_config.get("date_column")
    exclude_columns = table_config.get("exclude_columns")
    full_name = f"{schema}.{table}"

    _log(f"\nPublishing {full_name}")

    t0 = time.time()

    try:
        if not table_exists(local_engine, schema, table):
            _log(f"  Skipped (table does not exist locally)")
            return {"status": "skipped", "reason": "missing_locally"}

        if sync_mode == "full":
            stats = sync_full(local_engine, schema, table, pk, force=force_full)
        elif sync_mode == "upsert":
            stats = sync_upsert(local_engine, schema, table, pk)
        elif sync_mode == "incremental":
            if not date_column:
                _log(f"  ERROR: incremental mode requires date_column")
                return {"status": "failed", "reason": "missing_date_column"}
            stats = sync_incremental(local_engine, schema, table, pk, date_column, exclude_columns)
        else:
            _log(f"  ERROR: unknown sync_mode '{sync_mode}'")
            return {"status": "failed", "reason": f"unknown_mode:{sync_mode}"}

        elapsed = time.time() - t0
        _log(f"  Completed in {elapsed:.1f} sec")
        stats["status"] = "ok"
        stats["elapsed"] = elapsed
        return stats

    except Exception as e:
        elapsed = time.time() - t0
        _log(f"  FAILED after {elapsed:.1f} sec: {e}")
        return {"status": "failed", "error": str(e), "elapsed": elapsed}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Synchronize warehouse tables from local PostgreSQL to Neon."
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Force full sync on all tables (ignore row counts)",
    )
    args = parser.parse_args()

    _log_section("Neon Publisher")

    if not all([NEON_HOST, NEON_DATABASE, NEON_USER, NEON_PASSWORD]):
        _log("ERROR: Neon not configured. Set NEON_HOST, NEON_DATABASE,")
        _log("       NEON_USER, NEON_PASSWORD environment variables.")
        sys.exit(1)

    local_engine = create_local_engine()
    tables = load_sync_config()
    _log(f"Tables to sync: {len(tables)}")

    results: List[Dict[str, Any]] = []
    total_inserted = total_updated = total_skipped = 0
    t_start = time.time()

    for tc in tables:
        result = sync_table(local_engine, tc, force_full=args.full_sync)
        results.append({"table": f"{tc['schema']}.{tc['table']}", **result})
        total_inserted += result.get("inserted", 0)
        total_updated += result.get("updated", 0)
        total_skipped += result.get("skipped", 0)

    total_elapsed = time.time() - t_start

    succeeded = sum(1 for r in results if r.get("status") == "ok")
    failed = sum(1 for r in results if r.get("status") == "failed")

    _log_section("Summary")
    for r in results:
        status_icon = "ok" if r["status"] == "ok" else "FAIL" if r["status"] == "failed" else "skip"
        _log(f"  {r['table']:40s}  {status_icon}")

    _log("")
    _log(f"Published {succeeded} tables successfully")
    if failed:
        _log(f"Failed: {failed} tables")
    _log(f"  Total inserted : {total_inserted:,}")
    _log(f"  Total updated  : {total_updated:,}")
    _log(f"  Total skipped  : {total_skipped:,}")
    _log(f"  Total duration : {total_elapsed:.1f} sec")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
