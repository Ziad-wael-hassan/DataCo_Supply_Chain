FROM apache/airflow:2.7.1

COPY requirements.txt /requirements.txt

# Install ML/ETL packages (no conflict with Airflow)
RUN pip install --no-cache-dir \
    pandas sqlalchemy psycopg2-binary python-dotenv \
    scikit-learn>=1.3.0 imbalanced-learn>=0.11.0 matplotlib>=3.7.0 pyyaml>=6.0

# Install dbt separately --no-deps to avoid breaking Airflow's pinned deps
RUN pip install --no-cache-dir --no-deps \
    dbt-postgres==1.8.2 dbt-core==1.8.2

# Reinstall airflow to fix any collateral damage
RUN pip install --no-cache-dir apache-airflow==2.7.1 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.8.txt" || true
