-- create_neon_schema.sql
-- Run once against Neon to initialize the serving database schema.
-- psql "postgresql://user:pass@host/db?sslmode=require" -f scripts/create_neon_schema.sql

CREATE SCHEMA IF NOT EXISTS warehouse;

-- Dimensions (full-synced on each pipeline run)

CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
    customer_id         INT PRIMARY KEY,
    customer_first_name VARCHAR,
    customer_last_name  VARCHAR,
    customer_segment    VARCHAR,
    customer_city       VARCHAR,
    customer_state      VARCHAR,
    customer_country    VARCHAR,
    customer_street     VARCHAR,
    customer_zipcode    VARCHAR
);

CREATE TABLE IF NOT EXISTS warehouse.dim_products (
    product_card_id  INT PRIMARY KEY,
    product_name     VARCHAR,
    product_price    DOUBLE PRECISION,
    product_status   VARCHAR,
    category_id      INT,
    category_name    VARCHAR,
    department_id    INT,
    department_name  VARCHAR
);

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key    INT PRIMARY KEY,
    full_date   DATE,
    year        INT,
    quarter     INT,
    month       INT,
    month_name  VARCHAR,
    week        INT,
    day         INT,
    is_weekend  BOOLEAN
);

CREATE TABLE IF NOT EXISTS warehouse.dim_shipping_location (
    shipping_location_id VARCHAR PRIMARY KEY,
    order_city           VARCHAR,
    order_state          VARCHAR,
    order_country        VARCHAR,
    order_zipcode        VARCHAR,
    latitude             DOUBLE PRECISION,
    longitude            DOUBLE PRECISION,
    order_region         VARCHAR
);

-- Fact (upserted — only new order_item_ids inserted)

CREATE TABLE IF NOT EXISTS warehouse.fact_order_items (
    order_item_id                  INT PRIMARY KEY,
    order_id                       INT,
    customer_id                    INT,
    product_card_id                INT,
    date_key                       INT,
    shipping_location_id           VARCHAR,
    payment_type                   VARCHAR,
    delivery_status                VARCHAR,
    order_status                   VARCHAR,
    late_delivery_risk             INT,
    shipping_mode                  VARCHAR,
    order_date                     TIMESTAMP,
    shipping_date                  TIMESTAMP,
    days_for_shipping_real         DOUBLE PRECISION,
    days_for_shipment_scheduled    DOUBLE PRECISION,
    sales                          DOUBLE PRECISION,
    sales_per_customer             DOUBLE PRECISION,
    benefit_per_order              DOUBLE PRECISION,
    order_profit_per_order         DOUBLE PRECISION,
    order_item_total               DOUBLE PRECISION,
    order_item_discount            DOUBLE PRECISION,
    order_item_discount_rate       DOUBLE PRECISION,
    order_item_profit_ratio        DOUBLE PRECISION,
    order_item_quantity            INT
);

-- Predictions (incremental — new + updated via modified_at)

CREATE TABLE IF NOT EXISTS warehouse.predictions (
    prediction_id      SERIAL PRIMARY KEY,
    order_id           INT NOT NULL UNIQUE,
    fraud_probability  DOUBLE PRECISION,
    predicted_label    BOOLEAN,
    threshold_used     DOUBLE PRECISION,
    model_version      VARCHAR,
    pipeline_version   VARCHAR,
    predicted_at       TIMESTAMP,
    created_at         TIMESTAMP DEFAULT now(),
    modified_at        TIMESTAMP DEFAULT now()
);
