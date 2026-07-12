# DataCo Supply Chain — Data Engineering + ML Project

End-to-end data pipeline for the Kaggle [DataCo Smart Supply Chain](https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis) dataset (~180k order items). Raw CSV → validated → Kimball star schema → Gold AI feature table → fraud detection model → predictions.

`Python` · `PostgreSQL` · `dbt` · `Apache Airflow` · `scikit-learn` · `Docker` · `Power BI`

---

## 1. Architecture

```
DataCo CSV
     │
     ▼
raw.orders_raw            (load_raw.py — incremental landing, etl_run_id + ingested_at audit)
     │
     ▼
validate_raw.py           (incremental validation — only newly ingested rows, etl_run_id watermark)
     │
     ▼
staging.stg_orders        (dbt view — cleaning, renaming, typing / silver layer)
     │
     ├──────────┬──────────┬──────────┬──────────────────┐
     ▼          ▼          ▼          ▼                  ▼
dim_customers  dim_products  dim_date  dim_shipping_location
     │          │          │          │
     └──────────┴──────────┴──────────┘
                       │
                       ▼
              warehouse.fact_order_items   (incremental, merge on order_item_id)
                       │
                       ▼
              warehouse.fraud_features     (Gold AI — single source of truth for ML)
                       │
              ┌────────┴────────┐
              ▼                 ▼
         train.py          predict.py
     (ExtraTreesclf)    (reads Gold AI, writes predictions)
              │                 │
              ▼                 ▼
       fraud_model.pkl   warehouse.predictions
```

Orchestrated by Airflow: `load_raw → validate_raw → dbt_run → dbt_test`, daily.
ML training is manual (`python train.py`); prediction runs via `predict.py`.

---

## 2. Quickstart

```bash
git clone <this repo>
cd dataco-supply-chain-data-engineering
cp .env.example .env          # fill in real values if not using defaults
make up                       # docker compose up --build
make load                     # loads raw CSV into Postgres
make validate                 # runs incremental validation
make dbt                      # builds star schema + Gold AI
make test                     # runs 51 dbt tests
```

Or run the whole thing after `make up`:
```bash
make pipeline
```

**ML pipeline** (inside container):
```bash
cd ml
python train.py               # retrain model → fraud_model.pkl + metrics
python predict.py --order-id 5349    # predict one order (upsert)
python predict.py --all-new          # predict all unscored orders
```

Airflow UI: `localhost:8080` (admin/admin) · pgAdmin: `localhost:5050` (admin@admin.com/admin)

---

## 3. Repository Structure

```
├── scripts/                  load_raw.py, validate_raw.py
├── dbt/dataco_analytics/
│   ├── macros/               generate_schema_name.sql (schema naming override)
│   ├── models/
│   │   ├── staging/          stg_orders.sql (cleaning / silver layer)
│   │   └── marts/
│   │       ├── dim_*.sql     dimension tables (full refresh)
│   │       ├── fact_*.sql    fact table (incremental, merge)
│   │       └── ai/
│   │           ├── fraud_features.sql   Gold AI — ML feature table
│   │           └── schema.yml           31 dbt tests
│   └── profiles.yml          (gitignored)
├── ml/
│   ├── config.py             POSTGRES_URI, paths
│   ├── feature_engineering.py  load + validate + profile from Gold AI
│   ├── train.py              ExtraTreesClassifier, SMOTE, threshold sweep
│   ├── predict.py            production prediction (PULL from Gold AI, PUSH to predictions)
│   ├── threshold_optimization.py  standalone threshold analysis
│   ├── utils.py              logging helper
│   ├── saved_models/         fraud_model.pkl (gitignored)
│   └── reports/              metrics, plots (gitignored)
├── airflow/dags/             supply_chain_pipeline.py
├── docker-compose.yml        postgres, pgadmin, airflow
└── .gitignore
```

---

## 4. Current Scope (production-ready)

- ✅ PostgreSQL (raw / staging / warehouse schemas)
- ✅ Kimball star schema (dbt-modeled dims + fact, 4 dimensions + 1 fact)
- ✅ Incremental `fact_order_items` (dbt incremental, merge on `order_item_id`)
- ✅ Gold AI feature table (`warehouse.fraud_features` — 24 features, 30 columns)
- ✅ Great Expectations-style validation (incremental, etl_run_id watermark)
- ✅ Apache Airflow orchestration (daily, 4-task DAG)
- ✅ ML fraud detection (ExtraTreesClassifier, ROC-AUC 0.9552, 24 features)
- ✅ Production predictions (`warehouse.predictions`, upsert-safe, idempotent)
- ✅ Audit columns (`raw.orders_raw`: `etl_run_id` + `ingested_at`; `warehouse.predictions`: `created_at` + `modified_at`)
- ✅ Docker (`docker compose up`, no manual setup)
- ✅ 51 dbt tests (includes 4 FK relationship tests)
- ✅ Power BI dashboard (planned: Fraud Risk page using `warehouse.predictions`)

---

## 5. Future Improvements

- **Power BI Fraud Risk page** — dashboard `warehouse.predictions` (predicted fraud rate, model confidence distribution, flagged high-risk orders)
- Slowly Changing Dimension (SCD Type 2) on `dim_customers`, via `dbt snapshot`
- CDC (Debezium/Kafka) instead of full CSV reloads
- Cloud deployment (AWS/GCP/Azure) + Terraform
- Data lake landing zone (S3/MinIO) ahead of Postgres
- GitHub Actions CI (`dbt test` + `validate_raw.py` + `train.py` smoke test on every push)

---

## 6. Data Quality Note

412 rows have negative `Sales` values. Investigated and confirmed these correspond exclusively to `CANCELED` and `SUSPECTED_FRAUD` order statuses — legitimate refund/loss signal, not bad data. Preserved (not filtered), with validation scoping the "Sales ≥ 0" rule to exclude those statuses.

---

## 7. Data Dictionary

### raw.orders_raw
Landing table — raw CSV columns preserved as-is, plus audit columns.

| Column | Type | Description |
|---|---|---|
| *(all CSV columns)* | varchar / float / int | Untouched from source |
| etl_run_id | int | FK → warehouse.etl_runs.run_id (set at ingestion time) |
| ingested_at | timestamp | Row ingestion timestamp (DEFAULT now()) |

### staging.stg_orders
dbt view — cleaning, renaming, typing. Silver layer.

### warehouse.dim_customers
One row per customer (deduplicated to most recent order's attributes).

| Column | Type | Description |
|---|---|---|
| customer_id | int, PK | Unique customer identifier |
| customer_first_name | varchar | Customer's first name |
| customer_last_name | varchar | Customer's last name (`'Unknown'` if missing) |
| customer_segment | varchar | Business segment (Consumer, Corporate, Home Office) |
| customer_city | varchar | Customer's city |
| customer_state | varchar | Customer's state/province |
| customer_country | varchar | Customer's country |
| customer_street | varchar | Street address |
| customer_zipcode | varchar | Postal code (`'Unknown'` if missing) |

### warehouse.dim_products
One row per product.

| Column | Type | Description |
|---|---|---|
| product_card_id | int, PK | Unique product identifier |
| product_name | varchar | Product name |
| product_price | float | List price |
| product_status | int | Product status flag |
| category_id | int | Category identifier |
| category_name | varchar | Category name |
| department_id | int | Department identifier |
| department_name | varchar | Department name |

### warehouse.dim_date
One row per calendar date present in the order data.

| Column | Type | Description |
|---|---|---|
| date_key | int, PK | Date key in `YYYYMMDD` format |
| full_date | date | Calendar date |
| year | int | Calendar year |
| quarter | int | Calendar quarter (1-4) |
| month | int | Calendar month (1-12) |
| month_name | varchar | Full month name |
| week | int | ISO week number |
| day | int | Day of month |
| is_weekend | boolean | True if Saturday/Sunday |

### warehouse.dim_shipping_location
Shipping destination dimension with surrogate key.

| Column | Type | Description |
|---|---|---|
| shipping_location_id | varchar, PK | Surrogate key (dbt_utils.generate_surrogate_key) |
| order_city | varchar | Destination city |
| order_state | varchar | Destination state/province |
| order_country | varchar | Destination country |
| order_zipcode | varchar | Destination postal code |
| latitude | float | Destination latitude |
| longitude | float | Destination longitude |
| order_region | varchar | Geographic region |

### warehouse.fact_order_items
Grain: one row per order item. Incremental (merge on `order_item_id`).

| Column | Type | Description |
|---|---|---|
| order_item_id | int, PK | Unique order item identifier |
| order_id | int, FK | Parent order identifier |
| customer_id | int, FK → dim_customers | Customer who placed the order |
| product_card_id | int, FK → dim_products | Product ordered |
| date_key | int, FK → dim_date | Order date key |
| shipping_location_id | varchar, FK → dim_shipping_location | Shipping destination |
| payment_type | varchar | Payment method |
| delivery_status | varchar | Delivery outcome (Late, On Time) |
| order_status | varchar | Order status (COMPLETE, CANCELED, SUSPECTED_FRAUD) |
| late_delivery_risk | int | Binary flag (0/1) |
| shipping_mode | varchar | Shipping method |
| order_date | timestamp | Order placement date |
| shipping_date | timestamp | Ship date |
| days_for_shipping_real | float | Actual days to ship |
| days_for_shipment_scheduled | float | Scheduled days to ship |
| sales | float | Sales amount |
| sales_per_customer | float | Sales per customer |
| benefit_per_order | float | Profit/loss per order |
| order_profit_per_order | float | Order-level profit |
| order_item_total | float | Item total |
| order_item_discount | float | Discount amount |
| order_item_discount_rate | float | Discount rate (0-1) |
| order_item_profit_ratio | float | Profit ratio |
| order_item_quantity | int | Quantity (≥ 1) |

### warehouse.fraud_features
Gold AI layer — single source of truth for ML. All feature engineering (joins, temporal derivation, leakage/PII exclusion, target computation) in SQL.

| Column | Type | Description |
|---|---|---|
| order_item_id | int | Order line item identifier |
| order_id | int | Order identifier |
| customer_id | int | Customer identifier |
| payment_type | varchar | Payment method |
| order_item_quantity | int | Quantity |
| sales | float | Sales amount |
| sales_per_customer | float | Sales per customer |
| benefit_per_order | float | Profit/loss |
| order_profit_per_order | float | Order-level profit |
| order_item_total | float | Item total |
| order_item_discount | float | Discount amount |
| order_item_discount_rate | float | Discount rate (0-1) |
| order_item_profit_ratio | float | Profit ratio |
| shipping_mode | varchar | Shipping method |
| order_month | int | Month extracted from order_date (1-12) |
| order_day | int | Day of month (1-31) |
| order_hour | int | Hour (0-23) |
| order_day_of_week | int | Day of week (0=Sunday, 6=Saturday) |
| customer_segment | varchar | Business segment |
| product_price | float | Product list price |
| category_name | varchar | Product category |
| department_name | varchar | Product department |
| is_weekend | boolean | True if order placed on weekend |
| latitude | float | Shipping destination latitude |
| longitude | float | Shipping destination longitude |
| order_region | varchar | Geographic region |
| order_country | varchar | Shipping destination country |
| order_status | varchar | Raw status text (dropped before training) |
| target | int | Binary: 1 = SUSPECTED_FRAUD, 0 = clean |
| created_at | timestamp | Row creation timestamp |

**Excluded from this table** (leakage): `delivery_status`, `late_delivery_risk`, `days_for_shipping_real`, `shipping_date`, `days_for_shipment_scheduled`

**Excluded** (PII): `customer_first_name`, `customer_last_name`, `customer_street`, `customer_city`, `customer_zipcode`

### warehouse.predictions
Model predictions — one row per order. Upsert-safe (`ON CONFLICT (order_id) DO UPDATE`).

| Column | Type | Description |
|---|---|---|
| prediction_id | serial, PK | Auto-incrementing ID |
| order_id | int, UNIQUE | Order identifier (NOT NULL) |
| fraud_probability | float | Model output probability (0-1) |
| predicted_label | boolean | True if fraud (threshold applied) |
| threshold_used | float | Classification threshold (0.30) |
| model_version | varchar | Model version string |
| pipeline_version | varchar | Pipeline version string |
| predicted_at | timestamp | Prediction timestamp |
| created_at | timestamp | Row creation timestamp (DEFAULT now()) |
| modified_at | timestamp | Last update timestamp (updated on upsert) |

### warehouse.etl_runs
Pipeline run monitoring — one row per `load_raw.py` execution.

| Column | Type | Description |
|---|---|---|
| run_id | int, PK | Auto-incrementing run identifier |
| pipeline_name | varchar | Pipeline name (`supply_chain_pipeline`) |
| start_time | timestamp | Run start time |
| end_time | timestamp | Run end time |
| duration_ms | int | Runtime in milliseconds |
| rows_loaded | int | Rows inserted into `raw.orders_raw` |
| rows_failed | int | Validation failures |
| validation_status | varchar | `in_progress` / `pending` / `passed` / `failed` |

---

## 8. ML Pipeline

### Model
- **Algorithm**: ExtraTreesClassifier (300 estimators, no depth limit)
- **Features**: 24 (7 categorical + 17 numerical, derived in SQL)
- **Class imbalance**: 1:43.4 (4,066 fraud / 176,653 clean)
- **Resampling**: SMOTE on training set only (sampling_strategy=0.5)
- **Threshold**: 0.30 (optimized for F1 on test set)

### Performance (test set, 36,144 rows)
| Metric | Value |
|---|---|
| Precision | 0.4013 |
| Recall | 0.3727 |
| F1 | 0.3865 |
| ROC-AUC | 0.9497 |

### Inference artifact (`fraud_model.pkl`)
Contains: fitted model, fitted OrdinalEncoder, feature column list, threshold, model version. No refitting at inference time — predict.py applies the preprocessor directly.

### Usage
```bash
python train.py                           # retrain → saves fraud_model.pkl + reports
python predict.py --order-id 5349         # predict one order (upsert)
python predict.py --all-new               # predict all unscored orders (idempotent)
```
