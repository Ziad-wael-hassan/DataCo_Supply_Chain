"""
Feature engineering module.

All feature engineering (joins, temporal derivation, leakage/PII exclusion,
target computation) is handled by the Gold AI dbt model
``warehouse.fraud_features``.  Python-side responsibilities are limited to:

  1. Loading data from that table.
  2. Lightweight validation (shape, required columns, target integrity).
  3. Profiling / logging (distribution summaries, per-column stats).

Encoding, scaling, train/test split, and SMOTE belong in train.py.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import POSTGRES_URI  # noqa: E402
from utils import get_logger  # noqa: E402

log = get_logger("feature_engineering")

# ---------------------------------------------------------------------------
# Gold AI table name
# ---------------------------------------------------------------------------
_GOLD_TABLE = "warehouse.fraud_features"

# Columns expected to always be present in the Gold AI output
_REQUIRED_COLS: list[str] = [
    "order_item_id",
    "order_id",
    "customer_id",
    "order_status",
    "target",
    "payment_type",
    "sales",
    "customer_segment",
    "category_name",
    "latitude",
    "longitude",
]


# ---------------------------------------------------------------------------
# Helpers (profiling / logging — modeling concern, stays in Python)
# ---------------------------------------------------------------------------

def _validate_raw(df):
    """Run lightweight checks on the loaded DataFrame."""
    if df.empty:
        raise ValueError("Loaded DataFrame is empty — check the Gold AI table / DB connection.")

    missing = [c for c in _REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Required columns missing from dataset: {missing}")

    null_target = df["target"].isnull().sum()
    if null_target > 0:
        raise ValueError(
            f"target contains {null_target} NULL values — "
            "check fraud_features.sql case expression."
        )

    log.info("Gold AI dataset validation passed (%d rows, %d cols).", len(df), df.shape[1])


def _log_dataset_summary(df, target_col="target"):
    """Log a compact summary of the feature set."""
    cat_features = [
        c for c in df.columns
        if df[c].dtype == "object" or df[c].dtype.name == "category"
    ]
    num_features = [
        c for c in df.columns
        if c != target_col and df[c].dtype in ("int8", "int16", "int32", "int64",
                                                 "float16", "float32", "float64",
                                                 "bool")
    ]

    log.info("─" * 50)
    log.info("Dataset summary")
    log.info("  Rows               : %d", len(df))
    log.info("  Columns            : %d", df.shape[1])

    if target_col in df.columns:
        counts = df[target_col].value_counts()
        n_fraud = counts.get(1, 0)
        n_clean = counts.get(0, 0)
        log.info("  Fraud (target=1)  : %d  (%.2f%%)", n_fraud, 100 * n_fraud / len(df))
        log.info("  Clean (target=0)  : %d  (%.2f%%)", n_clean, 100 * n_clean / len(df))
        log.info("  Class imbalance   : 1 : %.1f", n_clean / max(n_fraud, 1))

    log.info("  Categorical feats  : %s", cat_features)
    log.info("  Numerical feats    : %s", num_features)
    log.info("─" * 50)


def _log_feature_profile(df):
    """Log a per-column data profile."""
    log.info("─" * 50)
    log.info("Feature profile")
    log.info("  %-*s  %-10s  %s" % (28, "column", "dtype", "stats"))
    log.info("  " + "─" * 72)

    for col in df.columns:
        dtype = str(df[col].dtype)

        if df[col].dtype == "object" or df[col].dtype.name == "category":
            n_unique = df[col].nunique()
            top_vals = df[col].value_counts().head(5).to_dict()
            top_str = ", ".join(f"{k}({v})" for k, v in top_vals.items())
            log.info("  %-28s  %-10s  unique=%d  top=[%s]", col, dtype, n_unique, top_str)

        elif df[col].dtype in ("int8", "int16", "int32", "int64",
                                "float16", "float32", "float64"):
            log.info(
                "  %-28s  %-10s  min=%.4f  max=%.4f  mean=%.4f  std=%.4f",
                col,
                dtype,
                df[col].min(),
                df[col].max(),
                df[col].mean(),
                df[col].std(),
            )

        elif df[col].dtype == "bool":
            n_true = int(df[col].sum())
            log.info("  %-28s  %-10s  True=%d  False=%d", col, dtype, n_true, len(df) - n_true)

        else:
            log.info("  %-28s  %-10s  (no stats)", col, dtype)

    log.info("─" * 50)


# ---------------------------------------------------------------------------
# Public API — training
# ---------------------------------------------------------------------------

def load_dataset():
    """
    Load the Gold AI feature table for training.

    All feature engineering is in warehouse.fraud_features (dbt).
    No joins, no column drops, no temporal derivation in Python.

    Returns
    -------
    pd.DataFrame
        Full Gold AI output including target, order_status, created_at.
    """
    engine = create_engine(POSTGRES_URI)
    sql = f"SELECT * FROM {_GOLD_TABLE}"

    log.info("Loading Gold AI feature table …")
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    log.info("Loaded %d rows, %d columns.", len(df), df.shape[1])
    _validate_raw(df)
    return df


def prepare_features(df):
    """
    Thin validation wrapper — target is already computed in SQL.

    Parameters
    ----------
    df : pd.DataFrame
        Output of load_dataset().  Contains target, order_status,
        created_at, and all features.

    Returns
    -------
    pd.DataFrame
        Same DataFrame, with profiling logged.
    """
    df = df.copy()

    # Drop order_status (target already captures this as binary)
    if "order_status" in df.columns:
        df.drop(columns=["order_status"], inplace=True)

    # Drop created_at (audit column, not a feature)
    if "created_at" in df.columns:
        df.drop(columns=["created_at"], inplace=True)

    _log_dataset_summary(df)
    _log_feature_profile(df)
    return df


# ---------------------------------------------------------------------------
# Public API — prediction
# ---------------------------------------------------------------------------

def load_orders(order_ids=None):
    """
    Load orders from the Gold AI table for prediction.

    Parameters
    ----------
    order_ids : list[int] | None
        If provided, filter to these order_ids only.

    Returns
    -------
    pd.DataFrame
        Gold AI rows with order_id retained.
    """
    engine = create_engine(POSTGRES_URI)

    if order_ids:
        placeholders = ", ".join([f":id_{i}" for i in range(len(order_ids))])
        sql = f"SELECT * FROM {_GOLD_TABLE} WHERE order_id IN ({placeholders})"
        params = {f"id_{i}": oid for i, oid in enumerate(order_ids)}
    else:
        sql = f"SELECT * FROM {_GOLD_TABLE}"
        params = {}

    log.info("Loading orders for prediction …")
    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    log.info("Loaded %d orders for prediction.", len(df))
    return df


def prepare_features_for_prediction(df):
    """
    Prepare features for prediction — drops target / audit columns only.

    No column drops for leakage/PII (Gold AI already excluded them).
    No temporal derivation (Gold AI already derived order_month/day/hour/dow).

    Parameters
    ----------
    df : pd.DataFrame
        Output of load_orders().

    Returns
    -------
    pd.DataFrame
        Clean feature set with order_id retained.
    """
    df = df.copy()

    # Drop columns not needed for prediction
    for col in ["target", "order_status", "created_at"]:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    log.info("Prepared %d orders for prediction (%d columns).", len(df), df.shape[1])
    return df
