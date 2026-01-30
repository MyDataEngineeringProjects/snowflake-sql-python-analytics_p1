CREATE OR REPLACE TABLE analytics.orders AS
SELECT
    invoice_no AS order_id,
    customer_id,
    MIN(invoice_date) AS order_date,
    SUM(line_total) AS order_total
FROM analytics.stg_retail
GROUP BY invoice_no, customer_id;

CREATE OR REPLACE TABLE analytics.customers AS
SELECT
    customer_id,
    country,
    COUNT(DISTINCT invoice_no) AS total_orders,
    SUM(line_total) AS lifetime_value
FROM analytics.stg_retail
GROUP BY customer_id, country;
