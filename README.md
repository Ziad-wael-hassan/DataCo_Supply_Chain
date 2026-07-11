# DataCo Supply Chain — Data Engineering Project Documentation

End-to-end data pipeline for the Kaggle [DataCo Smart Supply Chain](https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis) dataset (~180k order items). Raw CSV → validated → modeled into a Kimball star schema → orchestrated → ready for BI.

`Python` · `PostgreSQL` · `dbt` · `Apache Airflow` · `Great Expectations` · `Docker` · `Power BI`

---

## 1. Architecture

```
DataCo CSV
     │
     ▼
raw.orders_raw            (load_raw.py — untouched landing table)
     │
     ▼
Great Expectations         (validate_raw.py — gatekeeper, halts pipeline on failure)
     │
     ▼
staging.stg_orders         (dbt view — cleaning, renaming, typing)
     │
     ├──────────┬──────────┬──────────┐
     ▼          ▼          ▼          ▼
dim_customers  dim_products  dim_date  (dbt tables)
     │          │          │
     └──────────┴──────────┴──────────┐
                                        ▼
                          warehouse.fact_order_items
                                        │
                                        ▼
                              Power BI Dashboard
```

Orchestrated end-to-end by Airflow: `load_raw → validate_raw → dbt_run → dbt_test`, daily.

---

## 2. Quickstart

```bash
git clone <this repo>
cd dataco-supply-chain-data-engineering
cp .env.example .env          # fill in real values if not using defaults
make up                       # docker compose up --build
make load                     # loads raw CSV into Postgres
make validate                 # runs Great Expectations
make dbt                      # builds the star schema
make test                     # runs dbt tests
```

Or run the whole thing after `make up`:
```bash
make pipeline
```

Airflow UI: `localhost:8080` (admin/admin) · pgAdmin: `localhost:5050` (admin@admin.com/admin)

---

## 3. Repository Structure

```
├── scripts/              load_raw.py, validate_raw.py
├── sql/                  create_tables.sql
├── dbt/dataco_analytics/ staging + marts models, tests, docs
├── airflow/dags/         supply_chain_pipeline.py
├── dashboards/           Power BI file + screenshots
├── docs/                 architecture, ERD, DAG, lineage, GX screenshots
├── docker-compose.yml    postgres, pgadmin, airflow
├── Makefile              make up / load / validate / dbt / test / pipeline
└── data_quality_notes.md documented business decisions (e.g. negative Sales)
```

---

## 4. Current Scope (production-ready)

- ✅ PostgreSQL (raw / staging / warehouse schemas)
- ✅ Kimball star schema (dbt-modeled dims + fact)
- ✅ Great Expectations data quality gate
- ✅ Apache Airflow orchestration
- ✅ Docker (`docker compose up`, no manual setup)
- ✅ Logging + pipeline run monitoring (`warehouse.etl_runs`)
- ✅ Power BI dashboard

## 5. Future Improvements

- Incremental `fact_order_items` (dbt incremental materialization)
- Slowly Changing Dimension (SCD Type 2) on `dim_customers`, via `dbt snapshot`
- CDC (Debezium/Kafka) instead of full CSV reloads
- Cloud deployment (AWS/GCP/Azure) + Terraform
- Data lake landing zone (S3/MinIO) ahead of Postgres
- Spark for larger-than-memory processing
- GitHub Actions CI (`dbt test` + `validate_raw.py` on every push)

---

## 6. Data Quality Note

412 rows have negative `Sales` values. Investigated and confirmed these correspond exclusively to `CANCELED` and `SUSPECTED_FRAUD` order statuses — legitimate refund/loss signal, not bad data. Preserved (not filtered), with a Great Expectations `row_condition` scoping the "Sales ≥ 0" rule to exclude those statuses.

---

## 7. Data Dictionary

### warehouse.dim_customers
One row per customer (deduplicated to most recent order's attributes).

| Column | Type | Description |
|---|---|---|
| customer_id | int, PK | Unique customer identifier |
| customer_first_name | varchar | Customer's first name |
| customer_last_name | varchar | Customer's last name (`'Unknown'` if missing) |
| customer_segment | varchar | Business segment (e.g. Consumer, Corporate, Home Office) |
| customer_city | varchar | Customer's city |
| customer_state | varchar | Customer's state/province |
| customer_country | varchar | Customer's country |
| customer_street | varchar | Street address |
| customer_zipcode | varchar | Postal code (`'Unknown'` if missing) |

### warehouse.dim_products
One row per product (deduplicated to most recent order's attributes).

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

### warehouse.fact_order_items
Grain: one row per order item. Central fact table.

| Column | Type | Description |
|---|---|---|
| order_item_id | int, PK | Unique order item identifier |
| order_id | int, FK | Parent order identifier |
| customer_id | int, FK → dim_customers | Customer who placed the order |
| product_card_id | int, FK → dim_products | Product ordered |
| date_key | int, FK → dim_date | Order date key |
| payment_type | varchar | Payment/order type |
| delivery_status | varchar | Delivery outcome (e.g. Late, On Time) |
| order_status | varchar | Order status (e.g. COMPLETE, CANCELED, SUSPECTED_FRAUD) |
| late_delivery_risk | int | Binary flag (0/1) for late delivery risk |
| order_date | timestamp | Date/time order was placed |
| shipping_date | timestamp | Date/time order shipped |
| days_for_shipping_real | float | Actual days to ship |
| days_for_shipment_scheduled | float | Scheduled days to ship |
| sales | float | Sales amount (can be negative for CANCELED/SUSPECTED_FRAUD — see §6) |
| sales_per_customer | float | Sales attributed per customer |
| benefit_per_order | float | Profit/loss per order |
| order_profit_per_order | float | Order-level profit |
| order_item_total | float | Total for this order item |
| order_item_discount | float | Discount amount |
| order_item_discount_rate | float | Discount rate (0-1) |
| order_item_profit_ratio | float | Profit ratio |
| order_item_quantity | int | Quantity ordered (≥ 1) |

### warehouse.etl_runs
Pipeline run monitoring — one row per `load_raw.py` execution.

| Column | Type | Description |
|---|---|---|
| run_id | int, PK | Auto-incrementing run identifier |
| pipeline_name | varchar | Name of the pipeline (`supply_chain_pipeline`) |
| start_time | timestamp | Run start time |
| end_time | timestamp | Run end time |
| duration_ms | int | Total runtime in milliseconds |
| rows_loaded | int | Rows loaded into `raw.orders_raw` |
| rows_failed | int | Count of failed GX expectations, if any |
| validation_status | varchar | `pending` / `passed` / `failed` |
