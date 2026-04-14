CREATE SCHEMA ecommerce;

CREATE TABLE ecommerce.ecommerce_data (
    item_id BIGINT,
    status TEXT,
    created_at TIMESTAMP,
    sku TEXT,
    price NUMERIC,
    qty_ordered NUMERIC,
    grand_total NUMERIC,
    increment_id TEXT,
    category_name_1 TEXT,
    sales_commission_code TEXT,
    discount_amount NUMERIC,
    payment_method TEXT,
    working_date TIMESTAMP,
    bi_status TEXT,
    mv NUMERIC,
    year INT,
    month INT,
    customer_since TEXT,
    m_y TEXT,
    fy TEXT,
    customer_id BIGINT
);

SET search_path TO ecommerce;

SELECT * FROM ecommerce_data;

COPY ecommerce_data
FROM 'Downloads/ecommerce_clean.csv'
DELIMITER ','
CSV HEADER;


CREATE SCHEMA staging;
CREATE SCHEMA analytics;
CREATE SCHEMA reporting;

ALTER TABLE ecommerce.ecommerce_data SET SCHEMA staging;

-- fact table
DROP TABLE IF EXISTS analytics.fact_sales;
CREATE TABLE analytics.fact_sales AS
SELECT
    increment_id,
    item_id,
    customer_id,
    TRIM(LOWER(sku)) AS sku,
    created_at,
    DATE(created_at) AS order_date,
    price,
    qty_ordered,
    grand_total,
    discount_amount,
    (grand_total - discount_amount) AS net_sales,
    status,
    mv
FROM staging.ecommerce_data
WHERE status = 'complete';


-- dim_customer
DROP TABLE IF EXISTS analytics.dim_customer;
CREATE TABLE analytics.dim_customer AS
SELECT customer_id,
       MIN(customer_since) AS customer_since,
       MIN(fy) AS fy,
       MIN(created_at) AS first_purchase_date
FROM staging.ecommerce_data
GROUP BY customer_id;

-- Check for duplicates in dimension tables
-- Customers
SELECT customer_id, COUNT(*) 
FROM analytics.dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- dim_product
DROP TABLE IF EXISTS analytics.dim_product;
-- Rebuild dim_product with clean SKUs
CREATE TABLE analytics.dim_product AS
SELECT
    TRIM(LOWER(sku)) AS sku,
    MIN(category_name_1) AS category_name_1,
    MIN(sales_commission_code) AS sales_commission_code
FROM staging.ecommerce_data
WHERE sku IS NOT NULL AND TRIM(sku) <> ''
GROUP BY TRIM(LOWER(sku));

-- Now all SKUs in fact_sales match dim_product_clean exactly.
ALTER TABLE analytics.fact_sales
ALTER COLUMN sku TYPE TEXT;

UPDATE analytics.fact_sales
SET sku = TRIM(LOWER(sku))
WHERE sku IS NOT NULL;

-- Confirm uniqueness
SELECT sku, COUNT(*) 
FROM analytics.dim_product
GROUP BY sku
HAVING COUNT(*) > 1;

-- dim date
DROP TABLE IF EXISTS analytics.dim_date;
CREATE TABLE analytics.dim_date AS
SELECT DISTINCT
       created_at::date AS full_date,
       EXTRACT(YEAR FROM created_at) AS year,
       EXTRACT(MONTH FROM created_at) AS month,
       DATE_TRUNC('month', created_at) AS month_start
FROM staging.ecommerce_data;

-- Cohort Analysis
DROP TABLE IF EXISTS analytics.cohort_analysis;
CREATE TABLE analytics.cohort_analysis AS
WITH first_purchase AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', MIN(created_at)) AS cohort_month
    FROM analytics.fact_sales
    GROUP BY customer_id
),
all_orders AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', created_at) AS order_month
    FROM analytics.fact_sales
)
SELECT 
    a.customer_id,
    f.cohort_month,
    a.order_month
FROM all_orders a
JOIN first_purchase f
ON a.customer_id = f.customer_id;

-- 
SELECT 
    cohort_month,
    order_month,
    COUNT(*)
FROM analytics.cohort_analysis
GROUP BY cohort_month, order_month
ORDER BY cohort_month, order_month;

-- RFM Segmentation
CREATE TABLE analytics.rfm AS
SELECT
    customer_id,
    MAX(created_at) AS last_purchase,
    COUNT(increment_id) AS frequency,
    SUM(net_sales) AS monetary
FROM analytics.fact_sales
GROUP BY customer_id;

-- Customer Lifetime Value (LTV)
CREATE TABLE analytics.ltv AS
SELECT
    customer_id,
    SUM(net_sales) AS lifetime_value
FROM analytics.fact_sales
GROUP BY customer_id;

-- Pareto Analysis
SELECT
    sku,
    SUM(net_sales) AS revenue
FROM analytics.fact_sales
GROUP BY sku
ORDER BY revenue DESC;


-- Monthly Revenue View
CREATE VIEW reporting.monthly_revenue AS
SELECT
    DATE_TRUNC('month', created_at) AS month,
    SUM(net_sales) AS revenue,
    COUNT(DISTINCT increment_id) AS total_orders,
    ROUND(SUM(net_sales) / COUNT(DISTINCT increment_id), 2) AS avg_order_value
FROM analytics.fact_sales
GROUP BY 1;


-- Customer Retention View
CREATE VIEW reporting.customer_retention AS
SELECT
    COUNT(DISTINCT customer_id) FILTER (WHERE order_count > 1) * 100.0 /
    COUNT(DISTINCT customer_id) AS retention_rate
FROM (
    SELECT customer_id, COUNT(*) AS order_count
    FROM analytics.fact_sales
    GROUP BY customer_id
) t;


-- Add Indexes
CREATE INDEX idx_fact_sales_date ON analytics.fact_sales(created_at);
CREATE INDEX idx_fact_sales_customer ON analytics.fact_sales(customer_id);
CREATE INDEX idx_fact_sales_sku ON analytics.fact_sales(sku);
CREATE INDEX idx_fact_sales_composite ON analytics.fact_sales(customer_id, created_at);