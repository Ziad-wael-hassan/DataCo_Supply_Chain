"""
publish_to_neon.py — Synchronize final warehouse tables to Neon.

Reads from local PostgreSQL, writes to Neon using batch operations.
Each table wrapped in its own transaction — a failure rolls back that
table and continues with the rest.

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
from typing import Any, Dict, List, Optional, Set, Tuple

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

BATCH_SIZE: int = 500


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
# Engine creation
# ---------------------------------------------------------------------------
def create_neon_engine() -> Optional[Engine]:
    """Create SQLAlchemy engine for Neon. Returns None if unconfigured."""
    if not all([NEON_HOST, NEON_DATABASE, NEON_USER, NEON_PASSWORD]):
        return None
    uri = (
        f"postgresql://{NEON_USER}:{NEON_PASSWORD}"
        f"@{NEON_HOST}:{NEON_PORT}/{NEON_DATABASE}?sslmode=require"
    )
    return create_engine(uri)


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------
def load_sync_config() -> List[Dict[str, Any]]:
    """Load table registry from config/sync_tables.yml."""
    with open(SYNC_CONFIG_PATH) as f:
        return yaml.safe_load(f)["tables"]


# ---------------------------------------------------------------------------
# Schema introspection
# ---------------------------------------------------------------------------
def get_columns(engine: Engine, schema: str, table: str) -> List[str]:
    """Get ordered column names from an existing table."""
    inspector = inspect(engine)
    return [col["name"] for col in inspector.get_columns(table, schema=schema)]


def table_exists(engine: Engine, schema: str, table: str) -> bool:
    """Check if a table exists."""
    return inspect(engine).has_table(table, schema=schema)


def count_rows(engine: Engine, schema: str, table: str) -> int:
    """Count rows in a table."""
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT count(*) FROM {schema}.{table}")
        )
        return result.scalar()


def get_pks(engine: Engine, schema: str, table: str, pk: str) -> Set[Any]:
    """Get all values of the primary key column."""
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT {pk} FROM {schema}.{table}")
        )
        return {row[0] for row in result}


def get_max_timestamp(
    engine: Engine, table: str, date_column: str
) -> Optional[str]:
    """Get MAX(date_column) from a table. Returns None if empty."""
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT MAX({date_column}) FROM {table}")
        )
        val = result.scalar()
        return str(val) if val is not None else None


# ---------------------------------------------------------------------------
# Sync: full (dim tables)
# ---------------------------------------------------------------------------
def sync_full(
    local_engine: Engine,
    neon_engine: Engine,
    schema: str,
    table: str,
    pk: str,
    force: bool = False,
) -> Dict[str, Any]:
    """Full sync: DELETE all + INSERT if row count differs."""
    full_table = f"{schema}.{table}"
    local_count = count_rows(local_engine, schema, table)

    if not force:
        try:
            neon_count = count_rows(neon_engine, schema, table)
        except Exception:
            neon_count = 0

        if local_count == neon_count and neon_count > 0:
            _log(f"  Skipped (no change: {local_count:,} rows)")
            return {"inserted": 0, "updated": 0, "skipped": local_count, "mode": "skip"}

    columns = get_columns(local_engine, schema, table)
    cols_str = ", ".join(columns)
    placeholders = ", ".join(f":{c}" for c in columns)
    insert_sql = text(
        f"INSERT INTO {full_table} ({cols_str}) VALUES ({placeholders})"
    )

    with local_engine.connect() as conn:
        rows = [dict(row) for row in conn.execute(text(f"SELECT * FROM {schema}.{table}"))]

    with neon_engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {full_table}"))
        for i in range(0, len(rows), BATCH_SIZE):
            conn.execute(insert_sql, rows[i : i + BATCH_SIZE])

    _log(f"  Full sync: {local_count:,} rows")
    return {"inserted": local_count, "updated": 0, "skipped": 0, "mode": "full"}


# ---------------------------------------------------------------------------
# Sync: upsert (append-only — insert new PKs only)
# ---------------------------------------------------------------------------
def sync_upsert(
    local_engine: Engine,
    neon_engine: Engine,
    schema: str,
    table: str,
    pk: str,
) -> Dict[str, Any]:
    """Upsert sync: insert only rows with PKs not yet in Neon."""
    full_table = f"{schema}.{table}"
    local_count = count_rows(local_engine, schema, table)

    neon_pks = get_pks(neon_engine, schema, table, pk)
    if len(neon_pks) == 0:
        _log(f"  Neon table empty — inserting all {local_count:,} rows")
    else:
        _log(f"  Local: {local_count:,} rows | Neon: {len(neon_pks):,} PKs")

    columns = get_columns(local_engine, schema, table)
    cols_str = ", ".join(columns)
    placeholders = ", ".join(f":{c}" for c in columns)
    insert_sql = text(
        f"INSERT INTO {full_table} ({cols_str}) VALUES ({placeholders})"
        f" ON CONFLICT ({pk}) DO NOTHING"
    )

    with local_engine.connect() as conn:
        all_rows = [dict(row) for row in conn.execute(text(f"SELECT * FROM {schema}.{table}"))]

    new_rows = [r for r in all_rows if r[pk] not in neon_pks]

    if not new_rows:
        _log(f"  No new rows to insert")
        return {"inserted": 0, "updated": 0, "skipped": local_count, "mode": "upsert"}

    inserted = 0
    with neon_engine.begin() as conn:
        for i in range(0, len(new_rows), BATCH_SIZE):
            batch = new_rows[i : i + BATCH_SIZE]
            conn.execute(insert_sql, batch)
            inserted += len(batch)

    _log(f"  Inserted: {inserted:,} new rows")
    return {"inserted": inserted, "updated": 0, "skipped": local_count - inserted, "mode": "upsert"}


# ---------------------------------------------------------------------------
# Sync: incremental (filter by date_column)
# ---------------------------------------------------------------------------
def sync_incremental(
    local_engine: Engine,
    neon_engine: Engine,
    schema: str,
    table: str,
    pk: str,
    date_column: str,
) -> Dict[str, Any]:
    """Incremental sync: rows where date_column > MAX in Neon."""
    full_table = f"{schema}.{table}"

    last_sync = get_max_timestamp(neon_engine, full_table, date_column)
    if last_sync:
        _log(f"  Last sync: {last_sync}")
    else:
        _log(f"  Neon table empty — full insert")

    columns = get_columns(local_engine, schema, table)
    cols_str = ", ".join(columns)
    placeholders = ", ".join(f":{c}" for c in columns)
    update_clause = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in columns if c != pk
    )
    upsert_sql = text(
        f"INSERT INTO {full_table} ({cols_str}) VALUES ({placeholders})"
        f" ON CONFLICT ({pk})"
        f" DO UPDATE SET {update_clause}"
    )

    if last_sync:
        query = text(
            f"SELECT * FROM {schema}.{table} WHERE {date_column} > :last_sync"
        )
        with local_engine.connect() as conn:
            rows = [dict(row) for row in conn.execute(query, {"last_sync": last_sync})]
    else:
        with local_engine.connect() as conn:
            rows = [dict(row) for row in conn.execute(text(f"SELECT * FROM {schema}.{table}"))]

    local_count = count_rows(local_engine, schema, table)

    if not rows:
        _log(f"  No changed rows (total local: {local_count:,})")
        return {"inserted": 0, "updated": 0, "skipped": local_count, "mode": "incremental"}

    # Count new vs existing
    neon_pks = get_pks(neon_engine, schema, table, pk)
    inserted = updated = 0
    with neon_engine.begin() as conn:
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            new_in_batch = [r for r in batch if r[pk] not in neon_pks]
            existing_in_batch = [r for r in batch if r[pk] in neon_pks]

            if new_in_batch:
                conn.execute(insert_sql, new_in_batch)
                inserted += len(new_in_batch)
                neon_pks.update(r[pk] for r in new_in_batch)

            if existing_in_batch:
                conn.execute(upsert_sql, existing_in_batch)
                updated += len(existing_in_batch)

    _log(f"  Changed: {len(rows):,} | Inserted: {inserted:,} | Updated: {updated:,}")
    return {
        "inserted": inserted,
        "updated": updated,
        "skipped": local_count - inserted - updated,
        "mode": "incremental",
    }


# ---------------------------------------------------------------------------
# Per-table orchestration
# ---------------------------------------------------------------------------
def sync_table(
    local_engine: Engine,
    neon_engine: Engine,
    table_config: Dict[str, Any],
    force_full: bool = False,
) -> Dict[str, Any]:
    """Sync one table. Returns stats dict."""
    schema = table_config["schema"]
    table = table_config["table"]
    pk = table_config["pk"]
    sync_mode = table_config["sync_mode"]
    date_column = table_config.get("date_column")
    full_name = f"{schema}.{table}"

    _log(f"\nPublishing {full_name}")

    t0 = time.time()

    try:
        if not table_exists(local_engine, schema, table):
            _log(f"  Skipped (table does not exist locally)")
            return {"status": "skipped", "reason": "missing_locally"}

        if sync_mode == "full":
            stats = sync_full(local_engine, neon_engine, schema, table, pk, force=force_full)
        elif sync_mode == "upsert":
            stats = sync_upsert(local_engine, neon_engine, schema, table, pk)
        elif sync_mode == "incremental":
            if not date_column:
                _log(f"  ERROR: incremental mode requires date_column")
                return {"status": "failed", "reason": "missing_date_column"}
            stats = sync_incremental(
                local_engine, neon_engine, schema, table, pk, date_column
            )
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

    # Create engines
    local_engine = create_engine(POSTGRES_URI)
    neon_engine = create_neon_engine()

    if neon_engine is None:
        _log("ERROR: Neon not configured. Set NEON_HOST, NEON_DATABASE,")
        _log("       NEON_USER, NEON_PASSWORD environment variables.")
        sys.exit(1)

    # Load table registry
    tables = load_sync_config()
    _log(f"Tables to sync: {len(tables)}")

    # Sync each table
    results: List[Dict[str, Any]] = []
    total_inserted = total_updated = total_skipped = 0
    t_start = time.time()

    for tc in tables:
        result = sync_table(local_engine, neon_engine, tc, force_full=args.full_sync)
        results.append({"table": f"{tc['schema']}.{tc['table']}", **result})
        total_inserted += result.get("inserted", 0)
        total_updated += result.get("updated", 0)
        total_skipped += result.get("skipped", 0)

    total_elapsed = time.time() - t_start

    # Summary
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
