# snowflake-sql-python-analytics_p1
Project overview

Dataset description:
https://www.kaggle.com/datasets/tunguz/online-retail
This is a transnational data set which contains all the transactions occurring between 01/12/2010 and 09/12/2011 for a UK-based and registered non-store online retail.The company mainly sells unique all-occasion gifts. Many customers of the company are wholesalers.

Architecture diagram

How to run locally

SQL highlights:

CREATE DATABASE retail_db;
CREATE SCHEMA raw;
CREATE SCHEMA analytics;
CREATE WAREHOUSE retail_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60;

Example insights:
## Key Insights
- 18% of customers generate over 60% of total revenue
- UK accounts for the majority of transactions
- Repeat customers have significantly higher AOV


Teaching notes (optional section!)
