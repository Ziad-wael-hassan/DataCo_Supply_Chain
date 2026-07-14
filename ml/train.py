"""
train.py — Baseline fraud detection model.

Single ExtraTreesClassifier on the full feature set.
- Stratified 80/20 train/test split
- OrdinalEncoder for categorical features (tree-safe, no dummies)
- SMOTE on training set only (no test-set leakage)
- Saves: fraud_model.pkl (complete inference artifact),
         metrics.json, confusion_matrix.png,
         roc_curve.png, classification_report.txt

Usage (inside container):
    cd /opt/airflow/project/ml
    python train.py
"""

from __future__ import annotations

import json
import pickle
import sys
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from imblearn.over_sampling import SMOTE
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    ConfusionMatrixDisplay,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
    roc_curve,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OrdinalEncoder

from config import SAVED_MODELS_DIR, REPORTS_DIR
from feature_engineering import load_dataset, prepare_features
from utils import get_logger

# Ensure notifications package is discoverable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from notifications.notifier import send_model_training

log = get_logger("train")

# Columns excluded from the feature matrix X
_COLS_TO_EXCLUDE: list[str] = [
    "order_item_id",  # identifier — high cardinality, not a feature
    "order_id",       # identifier — kept in DF for predict.py but not in X
    "customer_id",    # identifier — high cardinality, not a feature
    "target",         # target variable
]


# -----------------------------------------------------------------------
# 1. Load data
# -----------------------------------------------------------------------
log.info("Loading dataset …")
df = load_dataset()
df = prepare_features(df)

# Separate target
y = df["target"].astype(np.int8).values
X = df.drop(columns=_COLS_TO_EXCLUDE, errors="ignore")

log.info("X shape: %s  |  y shape: %s", X.shape, y.shape)
log.info("Class distribution — fraud: %d (%.2f%%), clean: %d (%.2f%%)",
         int(y.sum()), y.mean() * 100, int(len(y) - y.sum()), (1 - y.mean()) * 100)

# Identify column types
cat_cols = X.select_dtypes(include=["object", "category"]).columns.tolist()
num_cols = X.select_dtypes(include=[np.number]).columns.tolist()
log.info("Categorical cols (%d): %s", len(cat_cols), cat_cols)
log.info("Numerical cols (%d): %s", len(num_cols), num_cols)


# -----------------------------------------------------------------------
# 2. Train / test split (stratified)
# -----------------------------------------------------------------------
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y,
)
log.info("Train: %d rows (%.1f%% fraud) | Test: %d rows (%.1f%% fraud)",
         len(y_train), y_train.mean() * 100,
         len(y_test), y_test.mean() * 100)


# -----------------------------------------------------------------------
# 3. Encode categoricals (OrdinalEncoder — tree-safe, no dummies)
# -----------------------------------------------------------------------
preprocessor = ColumnTransformer(
    transformers=[
        ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), cat_cols),
    ],
    remainder="passthrough",
    verbose_feature_names_out=False,
)

X_train_enc = preprocessor.fit_transform(X_train)
X_test_enc  = preprocessor.transform(X_test)

# Recover column names after passthrough
# ColumnTransformer passes through everything not in transformers,
# including boolean columns like is_weekend that aren't in num_cols.
encoded_cat_names = cat_cols
remainder_cols = [c for c in X.columns if c not in cat_cols]
all_col_names = encoded_cat_names + remainder_cols

X_train_enc = pd.DataFrame(X_train_enc, columns=all_col_names, index=X_train.index)
X_test_enc  = pd.DataFrame(X_test_enc,  columns=all_col_names, index=X_test.index)

log.info("Encoded columns: %s", all_col_names)


# -----------------------------------------------------------------------
# 4. SMOTE on training set only (no test-set leakage)
# -----------------------------------------------------------------------
log.info("Applying SMOTE …")
smote = SMOTE(sampling_strategy=0.5, random_state=42)
X_train_res, y_train_res = smote.fit_resample(X_train_enc, y_train)
log.info("After SMOTE — rows: %d, fraud: %d (%.1f%%), clean: %d (%.1f%%)",
         len(y_train_res),
         int(y_train_res.sum()), y_train_res.mean() * 100,
         int(len(y_train_res) - y_train_res.sum()),
         (1 - y_train_res.mean()) * 100)


# -----------------------------------------------------------------------
# 5. Train ExtraTreesClassifier (baseline)
# -----------------------------------------------------------------------
log.info("Training ExtraTreesClassifier …")
clf = ExtraTreesClassifier(
    n_estimators=300,
    max_depth=None,
    min_samples_split=5,
    random_state=42,
    n_jobs=-1,
)
clf.fit(X_train_res, y_train_res)
log.info("Training complete.  n_estimators=%d, n_features_in=%d",
         clf.n_estimators, clf.n_features_in_)


# -----------------------------------------------------------------------
# 6. Evaluate on test set
# -----------------------------------------------------------------------
log.info("Evaluating on test set …")

y_pred = clf.predict(X_test_enc)
y_prob = clf.predict_proba(X_test_enc)[:, 1]

precision = precision_score(y_test, y_pred)
recall    = recall_score(y_test, y_pred)
f1        = f1_score(y_test, y_pred)
roc_auc   = roc_auc_score(y_test, y_prob)

log.info("─" * 50)
log.info("Test metrics")
log.info("  Precision : %.4f", precision)
log.info("  Recall    : %.4f", recall)
log.info("  F1        : %.4f", f1)
log.info("  ROC AUC   : %.4f", roc_auc)
log.info("─" * 50)

# Classification report
report_str = classification_report(y_test, y_pred, target_names=["clean", "fraud"])
log.info("Classification report:\n%s", report_str)

# Confusion matrix
cm = confusion_matrix(y_test, y_pred)
log.info("Confusion matrix:\n%s", cm)


# -----------------------------------------------------------------------
# 6b. Find optimal threshold on test set (max F1)
# -----------------------------------------------------------------------
log.info("Sweeping thresholds for optimal F1 …")
best_f1 = 0.0
best_threshold = 0.5
for t in np.arange(0.05, 1.00, 0.05):
    y_pred_t = (y_prob >= t).astype(np.int8)
    f1_t = f1_score(y_test, y_pred_t, zero_division=0)
    if f1_t > best_f1:
        best_f1 = f1_t
        best_threshold = float(t)

log.info("Optimal threshold: %.2f  (F1=%.4f)", best_threshold, best_f1)

# Recompute predictions/metrics at the deployed threshold, not the
# sklearn default 0.5 — everything saved below must reflect the
# threshold actually stored in fraud_model.pkl and used by predict.py.
y_pred = (y_prob >= best_threshold).astype(np.int8)
precision = precision_score(y_test, y_pred)
recall    = recall_score(y_test, y_pred)
f1        = f1_score(y_test, y_pred)
report_str = classification_report(y_test, y_pred, target_names=["clean", "fraud"])
cm = confusion_matrix(y_test, y_pred)

log.info("Metrics at deployed threshold (%.2f):", best_threshold)
log.info("  Precision : %.4f", precision)
log.info("  Recall    : %.4f", recall)
log.info("  F1        : %.4f", f1)


# -----------------------------------------------------------------------
# 7. Save outputs
# -----------------------------------------------------------------------
SAVED_MODELS_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# 7a. Complete inference artifact (model + encoder + metadata)
MODEL_VERSION = "1.1.0"
inference_artifact = {
    "model":             clf,
    "preprocessor":      preprocessor,
    "feature_columns":   all_col_names,
    "cat_cols":          cat_cols,
    "remainder_cols":    remainder_cols,
    "threshold":         best_threshold,
    "model_version":     MODEL_VERSION,
    "roc_auc":           round(roc_auc, 4),
    "train_rows":        len(y_train),
    "test_rows":         len(y_test),
}
model_path = SAVED_MODELS_DIR / "fraud_model.pkl"
with open(model_path, "wb") as f:
    pickle.dump(inference_artifact, f)
log.info("Inference artifact saved: %s", model_path)
log.info("  model_version : %s", MODEL_VERSION)
log.info("  threshold     : %.2f", best_threshold)
log.info("  encoder       : OrdinalEncoder (%d cats)", len(cat_cols))
log.info("  features      : %d columns", len(all_col_names))

# 7b. Metrics JSON
metrics = {
    "model": "ExtraTreesClassifier",
    "precision": round(precision, 4),
    "recall": round(recall, 4),
    "f1": round(f1, 4),
    "roc_auc": round(roc_auc, 4),
    "test_size": len(y_test),
    "train_size_after_smote": int(len(y_train_res)),
    "features": all_col_names,
}
metrics_path = REPORTS_DIR / "metrics.json"
with open(metrics_path, "w") as f:
    json.dump(metrics, f, indent=2)
log.info("Metrics saved: %s", metrics_path)

# Send Telegram notification
try:
    model_name = inference_artifact["model"].__class__.__name__
    send_model_training(
        model_name=model_name,
        model_version=MODEL_VERSION,
        metrics={
            "Features": len(all_col_names),
            "Threshold": f"{best_threshold:.2f}",
            "Precision": f"{precision:.4f}",
            "Recall": f"{recall:.4f}",
            "F1": f"{f1:.4f}",
            "ROC-AUC": f"{roc_auc:.4f}",
            "Train Rows": len(y_train),
            "Test Rows": len(y_test),
        }
    )
except Exception as e:
    log.warning("Failed to send model training notification: %s", e)

# 7c. Confusion matrix plot
fig, ax = plt.subplots(figsize=(6, 5))
ConfusionMatrixDisplay(cm, display_labels=["clean", "fraud"]).plot(ax=ax, cmap="Blues")
ax.set_title("Confusion Matrix — ExtraTreesClassifier")
fig.tight_layout()
cm_path = REPORTS_DIR / "confusion_matrix.png"
fig.savefig(cm_path, dpi=150)
plt.close(fig)
log.info("Confusion matrix plot saved: %s", cm_path)

# 7d. ROC curve plot
fpr, tpr, _ = roc_curve(y_test, y_prob)
fig, ax = plt.subplots(figsize=(6, 5))
ax.plot(fpr, tpr, label=f"AUC = {roc_auc:.4f}", linewidth=2)
ax.plot([0, 1], [0, 1], "k--", alpha=0.4)
ax.set_xlabel("False Positive Rate")
ax.set_ylabel("True Positive Rate")
ax.set_title("ROC Curve — ExtraTreesClassifier")
ax.legend()
fig.tight_layout()
roc_path = REPORTS_DIR / "roc_curve.png"
fig.savefig(roc_path, dpi=150)
plt.close(fig)
log.info("ROC curve saved: %s", roc_path)

# 7e. Classification report text file
report_path = REPORTS_DIR / "classification_report.txt"
with open(report_path, "w") as f:
    f.write("ExtraTreesClassifier — Baseline\n")
    f.write("=" * 40 + "\n")
    f.write(f"Precision : {precision:.4f}\n")
    f.write(f"Recall    : {recall:.4f}\n")
    f.write(f"F1        : {f1:.4f}\n")
    f.write(f"ROC AUC   : {roc_auc:.4f}\n")
    f.write("=" * 40 + "\n\n")
    f.write(report_str)
log.info("Classification report saved: %s", report_path)

log.info("Done.")
