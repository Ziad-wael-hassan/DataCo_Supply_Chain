"""
predict.py — Production fraud prediction pipeline.

Loads a trained model from saved_models/fraud_model.pkl (complete inference
artifact containing model, fitted preprocessor, feature columns, and
threshold) and generates fraud predictions from the warehouse.

Usage:
    python predict.py --order-id 759281     # predict one order (upsert)
    python predict.py --all-new             # predict all unscored orders
"""
from __future__ import annotations

import argparse
import pickle
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import POSTGRES_URI, SAVED_MODELS_DIR
from feature_engineering import load_orders, prepare_features_for_prediction
from utils import get_logger

log = get_logger("predict")

MODEL_VERSION = "1.0.0"

_PREDICTIONS_DDL = """
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE TABLE IF NOT EXISTS warehouse.predictions (
    prediction_id     SERIAL PRIMARY KEY,
    order_id          INT NOT NULL UNIQUE,
    fraud_probability DOUBLE PRECISION,
    predicted_label   BOOLEAN,
    threshold_used    DOUBLE PRECISION,
    model_version     VARCHAR,
    pipeline_version  VARCHAR,
    predicted_at      TIMESTAMP,
    created_at        TIMESTAMP DEFAULT now(),
    modified_at       TIMESTAMP DEFAULT now()
);
"""


# -----------------------------------------------------------------------
# Artifact loading
# -----------------------------------------------------------------------

def load_artifact():
    """Load the complete inference artifact from disk."""
    path = SAVED_MODELS_DIR / "fraud_model.pkl"
    if not path.exists():
        log.error("Model artifact not found: %s", path)
        sys.exit(1)

    log.info("Loading inference artifact: %s", path)
    with open(path, "rb") as f:
        artifact = pickle.load(f)

    required_keys = {"model", "preprocessor", "feature_columns", "threshold", "model_version"}
    missing = required_keys - set(artifact.keys())
    if missing:
        log.error("Artifact missing keys: %s", missing)
        sys.exit(1)

    log.info("  model_version : %s", artifact["model_version"])
    log.info("  threshold     : %.2f", artifact["threshold"])
    log.info("  features      : %d columns", len(artifact["feature_columns"]))
    return artifact


# -----------------------------------------------------------------------
# Prediction engine
# -----------------------------------------------------------------------

def predict(artifact, df):
    """
    Apply the fitted preprocessor and model to a prepared DataFrame.

    Parameters
    ----------
    artifact : dict
        Complete inference artifact from load_artifact().
    df : pd.DataFrame
        Output of prepare_features_for_prediction() — contains order_id
        but NOT the target column.

    Returns
    -------
    pd.DataFrame with order_id, fraud_probability, predicted_label
    """
    model = artifact["model"]
    preprocessor = artifact["preprocessor"]
    feature_columns = artifact["feature_columns"]
    threshold = artifact["threshold"]

    # Separate order_id for output, build feature matrix X
    order_ids = df["order_id"].values
    X = df.drop(columns=["order_id"], errors="ignore")

    # Encode using the fitted preprocessor (no refit!)
    X_enc = preprocessor.transform(X)

    # Recover column names after passthrough
    cat_cols = artifact.get("cat_cols", [])
    remainder_cols = artifact.get("remainder_cols", [])
    all_col_names = cat_cols + remainder_cols
    X_enc = pd.DataFrame(X_enc, columns=all_col_names, index=X.index)

    # Ensure column order matches training exactly
    X_enc = X_enc[feature_columns]

    # Predict
    y_prob = model.predict_proba(X_enc)[:, 1]
    y_label = (y_prob >= threshold).astype(bool)

    result = pd.DataFrame({
        "order_id":           order_ids,
        "fraud_probability":  np.round(y_prob, 6),
        "predicted_label":    y_label,
    })

    log.info("Generated %d predictions (threshold=%.2f).", len(result), threshold)
    return result


# -----------------------------------------------------------------------
# Storage
# -----------------------------------------------------------------------

def ensure_predictions_table(engine):
    """Create the predictions table if it doesn't exist; add columns if it does."""
    with engine.begin() as conn:
        conn.execute(text(_PREDICTIONS_DDL))
        # Handle pre-existing table without new columns
        conn.execute(text(
            "DO $$ BEGIN "
            "ALTER TABLE warehouse.predictions ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT now(); "
            "EXCEPTION WHEN undefined_table THEN NULL; END $$;"
        ))
        conn.execute(text(
            "DO $$ BEGIN "
            "ALTER TABLE warehouse.predictions ADD COLUMN IF NOT EXISTS modified_at TIMESTAMP DEFAULT now(); "
            "EXCEPTION WHEN undefined_table THEN NULL; END $$;"
        ))
    log.info("Predictions table ready.")


def store_predictions(engine, pred_df, threshold, model_version, upsert=False):
    """
    Insert predictions into warehouse.predictions.

    Parameters
    ----------
    upsert : bool
        If True (--order-id mode): INSERT ... ON CONFLICT DO UPDATE.
        If False (--all-new mode): only insert rows not already present.
    """
    now = datetime.now(timezone.utc)
    pipeline_version = "1.0.0"

    rows = pred_df.to_dict("records")
    if not rows:
        log.info("No predictions to store.")
        return 0

    if upsert:
        sql = text("""
            INSERT INTO warehouse.predictions
                (order_id, fraud_probability, predicted_label,
                 threshold_used, model_version, pipeline_version, predicted_at)
            VALUES
                (:order_id, :fraud_probability, :predicted_label,
                 :threshold_used, :model_version, :pipeline_version, :predicted_at)
            ON CONFLICT (order_id) DO UPDATE SET
                fraud_probability = EXCLUDED.fraud_probability,
                predicted_label   = EXCLUDED.predicted_label,
                predicted_at      = EXCLUDED.predicted_at,
                modified_at       = now()
        """)
    else:
        sql = text("""
            INSERT INTO warehouse.predictions
                (order_id, fraud_probability, predicted_label,
                 threshold_used, model_version, pipeline_version, predicted_at)
            SELECT
                :order_id, :fraud_probability, :predicted_label,
                :threshold_used, :model_version, :pipeline_version, :predicted_at
            WHERE NOT EXISTS (
                SELECT 1 FROM warehouse.predictions p
                WHERE p.order_id = :order_id
            )
        """)

    inserted = 0
    with engine.begin() as conn:
        for row in rows:
            params = {
                "order_id":           int(row["order_id"]),
                "fraud_probability":  float(row["fraud_probability"]),
                "predicted_label":    bool(row["predicted_label"]),
                "threshold_used":     float(threshold),
                "model_version":      str(model_version),
                "pipeline_version":   str(pipeline_version),
                "predicted_at":       now,
            }
            result = conn.execute(sql, params)
            inserted += result.rowcount

    return inserted


# -----------------------------------------------------------------------
# Output
# -----------------------------------------------------------------------

def print_results(pred_df, threshold, mode):
    """Print formatted prediction results."""
    log.info("─" * 50)
    log.info("Prediction results (%s)", mode)
    log.info("─" * 50)

    for _, row in pred_df.iterrows():
        label_str = "FRAUD" if row["predicted_label"] else "CLEAN"
        log.info("Order ID              : %d", int(row["order_id"]))
        log.info("Fraud Probability     : %.2f", row["fraud_probability"])
        log.info("Prediction            : %s", label_str)
        log.info("Threshold             : %.2f", threshold)
        log.info("Prediction Time       : %s", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))
        log.info("─" * 50)


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Fraud prediction pipeline")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--order-id", type=int, help="Predict a single order by ID")
    group.add_argument("--all-new", action="store_true", help="Predict all unscored orders")
    args = parser.parse_args()

    start = time.time()
    engine = create_engine(POSTGRES_URI)

    # --- 1. Load artifact ------------------------------------------------
    artifact = load_artifact()
    threshold = artifact["threshold"]
    model_version = artifact["model_version"]

    # --- 2. Ensure predictions table -------------------------------------
    ensure_predictions_table(engine)

    # --- 3. Load orders from warehouse -----------------------------------
    if args.order_id:
        df = load_orders(order_ids=[args.order_id])
        mode = f"--order-id {args.order_id}"
    else:
        # --all-new: LEFT JOIN to exclude already-scored orders
        log.info("Finding unscored orders …")
        unscored_sql = (
            "SELECT DISTINCT f.order_id "
            "FROM warehouse.fraud_features f "
            "LEFT JOIN warehouse.predictions p ON f.order_id = p.order_id "
            "WHERE p.order_id IS NULL"
        )
        with engine.connect() as conn:
            result = conn.execute(text(unscored_sql))
            unscored_ids = [row[0] for row in result.fetchall()]

        if not unscored_ids:
            log.info("─" * 50)
            log.info("Orders requested : 0")
            log.info("Already predicted : all")
            log.info("New predictions  : 0")
            log.info("Completed in     : %.2f sec", time.time() - start)
            log.info("─" * 50)
            return

        df = load_orders(order_ids=unscored_ids)
        mode = f"--all-new ({len(unscored_ids)} orders)"

    if df.empty:
        log.warning("No matching orders found in the warehouse.")
        return

    orders_loaded = len(df)
    log.info("Orders requested : %d", orders_loaded)

    # --- 4. Prepare features ---------------------------------------------
    df = prepare_features_for_prediction(df)

    # --- 5. Predict ------------------------------------------------------
    pred_df = predict(artifact, df)

    # --- 6. Store results ------------------------------------------------
    if args.order_id:
        inserted = store_predictions(engine, pred_df, threshold, model_version, upsert=True)
        print_results(pred_df, threshold, mode)
        log.info("Upserted %d prediction.", inserted)
    else:
        inserted = store_predictions(engine, pred_df, threshold, model_version, upsert=False)
        log.info("New predictions : %d", inserted)

    elapsed = time.time() - start
    log.info("Completed in     : %.2f sec", elapsed)


if __name__ == "__main__":
    main()
