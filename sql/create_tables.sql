CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

DROP TABLE IF EXISTS warehouse.fact_orders;
DROP TABLE IF EXISTS warehouse.dim_shipping_location;
DROP TABLE IF EXISTS warehouse.dim_customer;
DROP TABLE IF EXISTS warehouse.dim_product;
DROP TABLE IF EXISTS warehouse.dim_date;

CREATE TABLE warehouse.dim_date (
    "DateKey" INT PRIMARY KEY,
    "order date (DateOrders)" TIMESTAMP,
    "Year" INT,
    "Quarter" INT,
    "Month" INT,
    "MonthName" VARCHAR,
    "Week" INT,
    "Day" INT,
    "Weekend" BOOLEAN
);

CREATE TABLE warehouse.dim_customer (
    "Customer Id" INT PRIMARY KEY,
    "Customer Fname" VARCHAR,
    "Customer Lname" VARCHAR,
    "Customer Segment" VARCHAR,
    "Customer City" VARCHAR,
    "Customer State" VARCHAR,
    "Customer Country" VARCHAR,
    "Customer Street" VARCHAR,
    "Customer Zipcode" VARCHAR
);

CREATE TABLE warehouse.dim_product (
    "Product Card Id" INT PRIMARY KEY,
    "Product Name" VARCHAR,
    "Product Price" FLOAT,
    "Product Status" VARCHAR,
    "Category Id" INT,
    "Category Name" VARCHAR,
    "Department Id" INT,
    "Department Name" VARCHAR
);

CREATE TABLE warehouse.dim_shipping_location (
    "Order Id" INT PRIMARY KEY,
    "Market" VARCHAR,
    "Order City" VARCHAR,
    "Order State" VARCHAR,
    "Order Country" VARCHAR,
    "Order Region" VARCHAR,
    "Order Zipcode" VARCHAR,
    "Shipping Mode" VARCHAR
);

CREATE TABLE warehouse.fact_orders (
    "Order Item Id" INT PRIMARY KEY,
    "Order Id" INT REFERENCES warehouse.dim_shipping_location("Order Id"),
    "Customer Id" INT REFERENCES warehouse.dim_customer("Customer Id"),
    "Product Card Id" INT REFERENCES warehouse.dim_product("Product Card Id"),
    "DateKey" INT REFERENCES warehouse.dim_date("DateKey"),
    "order date (DateOrders)" TIMESTAMP,
    "shipping date (DateOrders)" TIMESTAMP,
    "Type" VARCHAR,
    "Days for shipping (real)" FLOAT,
    "Days for shipment (scheduled)" FLOAT,
    "Delivery Status" VARCHAR,
    "Late_delivery_risk" INT,
    "Sales" FLOAT,
    "Sales per customer" FLOAT,
    "Benefit per order" FLOAT,
    "Order Profit Per Order" FLOAT,
    "Order Item Total" FLOAT,
    "Order Item Discount" FLOAT,
    "Order Item Discount Rate" FLOAT,
    "Order Item Profit Ratio" FLOAT,
    "Order Item Quantity" INT,
    "Order Status" VARCHAR
);
