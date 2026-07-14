import os
from pathlib import Path
from typing import Dict, Any, List, Optional

import psycopg2
import yaml
from sqlalchemy import create_engine, text
from .config import POSTGRES_URI
from .logger import log

class MetricsProvider:
    def __init__(self):
        self.engine = create_engine(POSTGRES_URI) if POSTGRES_URI else None
        self.sync_config_path = Path(__file__).resolve().parent.parent / "config" / "sync_tables.yml"

    def get_pipeline_metrics(self) -> Dict[str, Any]:
        """Collects runtime pipeline metrics for the success notification."""
        metrics: Dict[str, Any] = {}

        latest_etl_run = self.get_latest_etl_run()
        if latest_etl_run:
            rows_loaded = latest_etl_run.get("rows_loaded")
            if rows_loaded is not None:
                metrics["Raw Loaded"] = self._format_count(rows_loaded)

            validation_status = (latest_etl_run.get("validation_status") or "").strip().lower()
            if validation_status == "passed" and rows_loaded is not None:
                metrics["Validated"] = self._format_count(rows_loaded)

        fact_rows = self.get_table_count("warehouse", "fact_order_items")
        if fact_rows is not None:
            metrics["Fact Rows"] = self._format_count(fact_rows)

        predictions = self.get_table_count("warehouse", "predictions")
        if predictions is not None:
            metrics["Predictions"] = self._format_count(predictions)

        metrics.update(self.get_neon_publish_metrics())
        return metrics

    def _format_count(self, value: Any) -> str:
        if value is None:
            return ""
        return f"{int(value):,}"

    def get_latest_etl_run(self) -> Dict[str, Any]:
        """Returns the most recent warehouse.etl_runs row."""
        if not self.engine:
            return {}

        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT
                        run_id,
                        rows_loaded,
                        rows_failed,
                        validation_status,
                        start_time,
                        end_time
                    FROM warehouse.etl_runs
                    ORDER BY run_id DESC
                    LIMIT 1
                """)
                row = conn.execute(query).mappings().fetchone()
                if row:
                    return dict(row)
        except Exception as e:
            log.warning(f"Failed to query latest warehouse.etl_runs row: {e}")
        return {}

    def get_table_count(self, schema: str, table: str) -> Optional[int]:
        """Returns row count for a local warehouse table."""
        if not self.engine:
            return None

        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    text(f"SELECT COUNT(*) FROM {schema}.{table}")
                ).fetchone()
                if row:
                    return int(row[0])
        except Exception as e:
            log.warning(f"Failed to query {schema}.{table}: {e}")
        return None

    def _load_sync_tables(self) -> List[Dict[str, Any]]:
        if not self.sync_config_path.exists():
            return []
        try:
            with open(self.sync_config_path) as f:
                payload = yaml.safe_load(f) or {}
            return payload.get("tables", [])
        except Exception as e:
            log.warning(f"Failed to load sync config: {e}")
            return []

    def _has_neon_config(self) -> bool:
        required_envs = ["NEON_HOST", "NEON_DATABASE", "NEON_USER", "NEON_PASSWORD"]
        return all(os.getenv(name) for name in required_envs)

    def _get_neon_conn(self):
        return psycopg2.connect(
            dbname=os.getenv("NEON_DATABASE"),
            user=os.getenv("NEON_USER"),
            password=os.getenv("NEON_PASSWORD"),
            host=os.getenv("NEON_HOST"),
            port=os.getenv("NEON_PORT", "5432"),
            sslmode="require",
            connect_timeout=5,
        )

    def get_neon_publish_metrics(self) -> Dict[str, Any]:
        """Verifies the configured Neon tables against local row counts."""
        metrics: Dict[str, Any] = {}
        sync_tables = self._load_sync_tables()
        if not sync_tables or not self.engine or not self._has_neon_config():
            return metrics

        matched_tables = 0
        compared_tables = 0

        try:
            with self.engine.connect() as local_conn, self._get_neon_conn() as neon_conn:
                with neon_conn.cursor() as neon_cur:
                    for table_config in sync_tables:
                        schema = table_config["schema"]
                        table = table_config["table"]

                        try:
                            local_count = local_conn.execute(
                                text(f"SELECT COUNT(*) FROM {schema}.{table}")
                            ).scalar()
                            neon_cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                            neon_count = neon_cur.fetchone()[0]
                        except Exception as table_error:
                            log.warning(f"Failed to inspect Neon sync for {schema}.{table}: {table_error}")
                            continue

                        compared_tables += 1
                        if int(local_count) == int(neon_count):
                            matched_tables += 1
        except Exception as e:
            log.warning(f"Failed to inspect Neon publishing metrics: {e}")
            return metrics

        if compared_tables == 0:
            return metrics

        metrics["Tables Updated"] = self._format_count(matched_tables)
        if matched_tables == compared_tables == len(sync_tables):
            metrics["Neon Sync"] = "SUCCESS"
        elif matched_tables == 0:
            metrics["Neon Sync"] = "FAILED"
        else:
            metrics["Neon Sync"] = f"PARTIAL ({matched_tables}/{compared_tables})"
        return metrics
