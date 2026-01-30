CREATE OR REPLACE TABLE raw.retail_raw (
    invoice_no STRING,
    stock_code STRING,
    description STRING,
    quantity INTEGER,
    invoice_date TIMESTAMP,
    unit_price NUMBER(10,2),
    customer_id STRING,
    country STRING
);
