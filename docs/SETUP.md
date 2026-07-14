# Setup

## Prerequisites

- Docker and Docker Compose
- Git
- Python 3.8+

## Quick Start

```bash
# 1. Clone
git clone https://github.com/Ziad-wael-hassan/DataCo_Supply_Chain.git
cd DataCo_Supply_Chain

# 2. Configure
cp .env.example .env
# Edit .env with your Neon and Telegram credentials

# 3. Start
docker compose up -d

# 4. Open Airflow → http://localhost:8080 (admin/admin)
# Trigger supply_chain_pipeline DAG
```

## Environment Variables

| Variable | Purpose | Required |
|----------|---------|----------|
| `DATABASE_URL` | PostgreSQL connection string | Yes |
| `NEON_HOST` | Neon hostname | Yes |
| `NEON_PORT` | Neon port (5432) | Yes |
| `NEON_DATABASE` | Neon database name | Yes |
| `NEON_USER` | Neon username | Yes |
| `NEON_PASSWORD` | Neon password | Yes |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token | Yes |
| `TELEGRAM_CHAT_ID` | Telegram chat ID | Yes |

## Service Ports

| Service | URL | Credentials |
|---------|-----|-------------|
| PostgreSQL | `localhost:5432` | postgres / postgres |
| pgAdmin | `localhost:5050` | admin@admin.com / admin |
| Airflow | `localhost:8080` | admin / admin |
| Metabase | `localhost:3000` | Create on first login |

## Running the Pipeline

### Via Airflow UI
1. Open `http://localhost:8080`
2. Find `supply_chain_pipeline` DAG
3. Click the play button → **Trigger DAG**

### Via CLI
```bash
docker compose exec airflow airflow dags trigger supply_chain_pipeline
```

### Manually (without Airflow)
```bash
python scripts/load_raw.py
python scripts/validate_raw.py
cd dbt/dataco_analytics && dbt run && dbt test
python ml/predict.py --all-new
python scripts/publish_to_neon.py
```

## Running ML

### Train the model
```bash
cd ml
python train.py
```

Saves `saved_models/fraud_model.pkl` and `reports/metrics.json`.

### Predict fraud

```bash
# All unscored orders
python ml/predict.py --all-new

# Single order by ID
python ml/predict.py --order-id 42
```

## Neon Setup

```bash
# Create schema in Neon
psql "postgresql://neondb_owner:<password>@ep-xxx-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require" -f scripts/create_neon_schema.sql

# Sync data
python scripts/publish_to_neon.py
```

## Metabase Setup

1. Open `http://localhost:3000`
2. **Admin** → **Databases** → **Add database**
3. Select PostgreSQL
4. Enter Neon credentials from `.env`
5. Save and explore

## Tableau Setup

1. Open Tableau Desktop or Tableau Public
2. **Connect** → **PostgreSQL**
3. Enter Neon credentials from `.env`
4. Select the `warehouse` schema
5. Build dashboards from the 6 synced tables
