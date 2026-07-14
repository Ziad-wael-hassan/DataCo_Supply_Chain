# Demo Recording Guide

6 recordings for README, GitHub repo, and presentations. Every command is copy-paste ready.

---

## Recording 1 — Full Pipeline Demo

**Purpose:** Show the complete end-to-end workflow in one GIF.  
**Duration:** 30–40 seconds (speed up DAG execution)  
**Playback speed:** 2× for DAG execution sections

### Browser Pages

| Tab | URL |
|-----|-----|
| 1 | `http://localhost:8080` — Airflow DAGs |
| 2 | Telegram Desktop — Notifications chat |
| 3 | Metabase — `http://localhost:3000` |
| 4 | Neon SQL Console — Dashboard |

### Recording Sequence

1. **Airflow DAGs page** — show `supply_chain_pipeline` is ready
2. **Click play** → **Trigger DAG**
3. **Show tasks turning green** (speed up 2× during execution)
4. **Switch to Telegram** — show Pipeline Started notification
5. **Switch to Telegram** — show Pipeline Success notification
6. **Switch to Neon SQL Console** — run:
   ```sql
   SELECT COUNT(*) AS total_orders
   FROM warehouse.fact_order_items;
   ```
   Show result: 180,519
7. **Switch to Metabase** — click refresh, show dashboard loads

### Expected Output

- Airflow: 7/7 tasks green (all succeeded)
- Telegram: Started + Success notifications
- Neon: `total_orders = 180519`
- Metabase: Dashboard refreshed from Neon

---

## Recording 2 — Model Training

**Purpose:** Show ML model training pipeline.  
**Duration:** 15–20 seconds  
**Playback speed:** 2× during training

### Browser Pages

| Tab | URL |
|-----|-----|
| 1 | Telegram Desktop — Notifications chat |
| 2 | File explorer — `ml/reports/` |

### Commands

```bash
cd ml
python train.py
```

### Recording Sequence

1. **Terminal** — run `python train.py` (speed up 2×)
2. **Switch to Telegram** — show Model Retrained notification
3. **Open `ml/reports/metrics.json`** — show ROC-AUC: 0.9497
4. **Open `ml/reports/confusion_matrix.png`**
5. **Open `ml/reports/roc_curve.png`**

### Expected Output

- Telegram: Model Retrained notification with v1.1.0
- metrics.json: `{"roc_auc": 0.9497, "precision": 0.4013, "recall": 0.3727, "f1": 0.3865}`
- Two plots saved to `ml/reports/`

---

## Recording 3 — Prediction Demo

**Purpose:** Show ML predictions on single orders and batch.  
**Duration:** 15–20 seconds  
**Playback speed:** 1×

### Browser Pages

| Tab | URL |
|-----|-----|
| 1 | pgAdmin — `localhost:5050` |
| 2 | Neon SQL Console |

### Commands

```bash
cd ml

# Single order prediction
python predict.py --order-id 42

# Batch prediction (all unscored orders)
python predict.py --all-new
```

### Recording Sequence

1. **Terminal** — run `python predict.py --order-id 42`
2. **Terminal** — run `python predict.py --all-new` (skip if all already scored)
3. **pgAdmin** — run:
   ```sql
   SELECT order_id,
          fraud_probability,
          predicted_label,
          model_version
   FROM warehouse.predictions
   ORDER BY prediction_id DESC
   LIMIT 10;
   ```
4. **Neon SQL Console** — run same query, show identical results

### Expected Output

- Single prediction: `order_id=42, fraud_probability=X.XXXX, predicted_label=true/false`
- Batch: `N unscored orders processed`
- SQL results: 10 rows with model_version = "1.1.0"

---

## Recording 4 — Neon Sync

**Purpose:** Show data publishing from local PostgreSQL to Neon Cloud.  
**Duration:** 15–20 seconds  
**Playback speed:** 2× during sync

### Browser Pages

| Tab | URL |
|-----|-----|
| 1 | Neon SQL Console |
| 2 | pgAdmin |

### Commands

```bash
python scripts/publish_to_neon.py
```

### Recording Sequence

1. **Terminal** — run `python scripts/publish_to_neon.py` (speed up 2×)
2. **pgAdmin** — run local counts:
   ```sql
   SELECT COUNT(*) FROM warehouse.fact_order_items;
   SELECT COUNT(*) FROM warehouse.predictions;
   ```
3. **Neon SQL Console** — run same queries:
   ```sql
   SELECT COUNT(*) FROM warehouse.fact_order_items;
   SELECT COUNT(*) FROM warehouse.predictions;
   ```
4. **Show matching counts**

### Expected Output

- Local: `fact_order_items = 180,519` | `predictions = 65,752`
- Neon: `fact_order_items = 180,719` | `predictions = 65,852`
- Counts match or Neon is slightly higher (incremental sync)

---

## Recording 5 — Metabase Dashboard

**Purpose:** Show BI dashboards powered by Neon.  
**Duration:** 15–20 seconds  
**Playback speed:** 1×

### Browser Pages

| Tab | URL |
|-----|-----|
| 1 | Metabase — `http://localhost:3000` |

### Recording Sequence

1. **Open Metabase** — show dashboard home
2. **Click into a dashboard** — show charts loading
3. **Click refresh** — show data updates from Neon
4. **Interact with a filter** — change date range or segment
5. **Show data source** — Admin → Databases → show Neon connection

### Expected Output

- Dashboard loads from Neon
- Charts render with 180,519 orders
- Filter interaction changes results
- Data source shows Neon PostgreSQL connection

---

## Recording 6 — Telegram Notifications

**Purpose:** Show all 4 notification types.  
**Duration:** 20–30 seconds  
**Playback speed:** 1×

### Browser Pages

| Tab | URL |
|-----|-----|
| 1 | Telegram Desktop — Notifications chat |
| 2 | Airflow DAGs |

### Commands

```bash
# Trigger a successful run
docker compose exec airflow airflow dags trigger supply_chain_pipeline

# To trigger a failure notification (optional):
# Temporarily set wrong Neon password in .env, trigger, then restore
```

### Recording Sequence

1. **Telegram** — show existing notifications:
   - 🚀 Pipeline Started
   - ✅ Pipeline Success
   - ❌ Pipeline Failed
   - 🤖 Model Retrained
2. **If missing a type:** trigger a new run to generate it
3. **For failure:** temporarily break Neon password in `.env`, trigger DAG, wait for failure notification, restore correct password

### Expected Output

4 notification types visible in Telegram:

| Type | Content |
|------|---------|
| Started | dag_id, run_id, trigger time |
| Success | Duration, rows loaded, predictions, Neon status |
| Failed | Failed task, exception message, retry count |
| Model Retrained | Model name, version, ROC-AUC, precision, recall, F1 |

---

## Tips

- **Screen resolution:** 1920×1080 recommended
- **Terminal font:** 14–16pt for readability
- **Clean desktop:** Close unnecessary apps before recording
- **GIF conversion:** Use [LICEcap](https://www.cockos.com/licecap/) or [peek](https://github.com/phw/peek)
- **Speed up:** 2× for loading/training, 1× for interactions
- **File naming:** `docs/gifs/<recording-name>.gif`
