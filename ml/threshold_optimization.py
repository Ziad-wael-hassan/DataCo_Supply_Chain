"""
threshold_optimization.py — Decision-threshold sweep for the baseline model.

Reproduces the same train.py pipeline, then evaluates thresholds 0.05–0.95
(step 0.05) on the test set.  Saves:

    reports/threshold_metrics.csv   — per-threshold precision/recall/F1/specificity
    reports/threshold_plot.png      — threshold vs precision / recall / F1
    reports/test_predictions.csv    — order_id, actual, probability, predicted_label

Usage (inside container):
    cd /opt/airflow/project/ml
    python threshold_optimization.py
"""

from __future__ import annotations

import pickle
import sys
from typing import Dict, List, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from imblearn.over_sampling import SMOTE
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.metrics import (
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    confusion_matrix,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OrdinalEncoder

from config import SAVED_MODELS_DIR, REPORTS_DIR
from feature_engineering import create_target, load_dataset, prepare_features
from utils import get_logger

log = get_logger("threshold_opt")

# Columns excluded from the feature matrix X (same as train.py)
_COLS_TO_EXCLUDE: list[str] = [
    "order_id",
    "target",
]

# ── Threshold grid ────────────────────────────────────────────────────
THRESHOLDS = np.arange(0.05, 1.00, 0.05)


# -----------------------------------------------------------------------
# 1. Reproduce the exact train.py pipeline
# -----------------------------------------------------------------------
log.info("Loading dataset …")
df = load_dataset()
df = create_target(df)
df = prepare_features(df)

y = df["target"].astype(np.int8).values
X = df.drop(columns=_COLS_TO_EXCLUDE, errors="ignore")

cat_cols = X.select_dtypes(include=["object", "category"]).columns.tolist()
num_cols = X.select_dtypes(include=[np.number]).columns.tolist()

# Stratified split — same random_state and test_size as train.py
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y,
)

# Keep test indices to align with order_id later
test_indices = X_test.index

# OrdinalEncoder
preprocessor = ColumnTransformer(
    transformers=[
        ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), cat_cols),
    ],
    remainder="passthrough",
    verbose_feature_names_out=False,
)
X_train_enc = preprocessor.fit_transform(X_train)
X_test_enc  = preprocessor.transform(X_test)

remainder_cols = [c for c in X.columns if c not in cat_cols]
all_col_names = cat_cols + remainder_cols
X_train_enc = pd.DataFrame(X_train_enc, columns=all_col_names, index=X_train.index)
X_test_enc  = pd.DataFrame(X_test_enc,  columns=all_col_names, index=X_test.index)

# SMOTE
log.info("Applying SMOTE …")
smote = SMOTE(sampling_strategy=0.5, random_state=42)
X_train_res, y_train_res = smote.fit_resample(X_train_enc, y_train)
log.info("After SMOTE — rows: %d  fraud: %d (%.1f%%)",
         len(y_train_res), int(y_train_res.sum()), y_train_res.mean() * 100)

# Train
log.info("Training ExtraTreesClassifier …")
clf = ExtraTreesClassifier(
    n_estimators=300,
    max_depth=None,
    min_samples_split=5,
    random_state=42,
    n_jobs=-1,
)
clf.fit(X_train_res, y_train_res)
log.info("Training complete.")

# Predictions
y_prob = clf.predict_proba(X_test_enc)[:, 1]
roc_auc = roc_auc_score(y_test, y_prob)
log.info("ROC-AUC on test set: %.4f", roc_auc)


# -----------------------------------------------------------------------
# 2. Threshold sweep
# -----------------------------------------------------------------------
log.info("Sweeping %d thresholds from %.2f to %.2f …",
         len(THRESHOLDS), THRESHOLDS[0], THRESHOLDS[-1])

rows: List[Dict[str, float]] = []

for t in THRESHOLDS:
    y_pred_t = (y_prob >= t).astype(np.int8)

    tn, fp, fn, tp = confusion_matrix(y_test, y_pred_t, labels=[0, 1]).ravel()

    precision = precision_score(y_test, y_pred_t, zero_division=0)
    recall    = recall_score(y_test, y_pred_t, zero_division=0)
    f1        = f1_score(y_test, y_pred_t, zero_division=0)
    specificity = tn / (tn + fp) if (tn + fp) > 0 else 0.0

    rows.append({
        "threshold":   round(float(t), 2),
        "precision":   round(precision, 4),
        "recall":      round(recall, 4),
        "f1":          round(f1, 4),
        "specificity": round(specificity, 4),
    })

    log.info("  t=%.2f  P=%.4f  R=%.4f  F1=%.4f  Spec=%.4f",
             t, precision, recall, f1, specificity)

metrics_df = pd.DataFrame(rows)


# -----------------------------------------------------------------------
# 3. Best threshold (highest F1)
# -----------------------------------------------------------------------
best_idx = metrics_df["f1"].idxmax()
best_row = metrics_df.loc[best_idx]
best_threshold = best_row["threshold"]
log.info("─" * 50)
log.info("Best threshold (max F1): %.2f", best_threshold)
log.info("  Precision : %.4f", best_row["precision"])
log.info("  Recall    : %.4f", best_row["recall"])
log.info("  F1        : %.4f", best_row["f1"])
log.info("  Specificity: %.4f", best_row["specificity"])
log.info("  ROC-AUC   : %.4f  (unchanged)", roc_auc)
log.info("─" * 50)


# -----------------------------------------------------------------------
# 4. Save threshold_metrics.csv
# -----------------------------------------------------------------------
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

metrics_path = REPORTS_DIR / "threshold_metrics.csv"
metrics_df.to_csv(metrics_path, index=False)
log.info("Threshold metrics saved: %s", metrics_path)


# -----------------------------------------------------------------------
# 5. Threshold vs Precision / Recall / F1 plot
# -----------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(8, 5))
ax.plot(metrics_df["threshold"], metrics_df["precision"], "o-", label="Precision", linewidth=2)
ax.plot(metrics_df["threshold"], metrics_df["recall"],    "s-", label="Recall",    linewidth=2)
ax.plot(metrics_df["threshold"], metrics_df["f1"],        "^-", label="F1",        linewidth=2)
ax.axvline(x=best_threshold, color="red", linestyle="--", alpha=0.7, label=f"Best F1 threshold = {best_threshold:.2f}")
ax.set_xlabel("Decision Threshold")
ax.set_ylabel("Score")
ax.set_title("Threshold vs Precision / Recall / F1")
ax.set_xlim(0, 1)
ax.set_ylim(0, 1.05)
ax.legend()
ax.grid(True, alpha=0.3)
fig.tight_layout()

plot_path = REPORTS_DIR / "threshold_plot.png"
fig.savefig(plot_path, dpi=150)
plt.close(fig)
log.info("Threshold plot saved: %s", plot_path)


# -----------------------------------------------------------------------
# 6. Save test_predictions.csv
# -----------------------------------------------------------------------
# Retrieve order_id from the original DataFrame using test indices
order_ids = df.loc[test_indices, "order_id"].values

pred_df = pd.DataFrame({
    "order_id":              order_ids,
    "actual_target":         y_test.astype(int),
    "predicted_probability": np.round(y_prob, 6),
    "predicted_label":       (y_prob >= best_threshold).astype(int),
})

pred_path = REPORTS_DIR / "test_predictions.csv"
pred_df.to_csv(pred_path, index=False)
log.info("Test predictions saved: %s  (%d rows)", pred_path, len(pred_df))

log.info("Done.")
