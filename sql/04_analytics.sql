-- Top 10 customers by lifetime value
SELECT
    customer_id,
    lifetime_value
FROM analytics.customers
ORDER BY lifetime_value DESC
LIMIT 10;

-- Monthly revenue trend
SELECT
    DATE_TRUNC('month', order_date) AS month,
    SUM(order_total) AS revenue
FROM analytics.orders
GROUP BY month
ORDER BY month;
