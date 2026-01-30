CREATE OR REPLACE VIEW analytics.stg_retail AS
SELECT
    invoice_no,
    stock_code,
    description,
    quantity,
    invoice_date,
    unit_price,
    customer_id,
    country,
    quantity * unit_price AS line_total
FROM raw.retail_raw
WHERE
    quantity > 0
    AND unit_price > 0
    AND customer_id IS NOT NULL;
