# End-to-End Analytics Pipeline with SQL, Python, and Snowflake

## 📌 Overview
This project demonstrates an end-to-end data analytics pipeline using
**Snowflake**, **SQL**, and **Python** on a real-world retail dataset.

It is designed for:
- Teaching beginner–intermediate data engineering concepts
- Showcasing portfolio-ready skills on GitHub

---

## 🧱 Architecture
Raw CSV → Python ingestion → Snowflake RAW tables → SQL transformations → Analytics queries → Python visualization

---

## 📊 Dataset
**Online Retail II Dataset**
- Real transactional e-commerce data
- Orders, customers, products, revenue
- Source: UCI / Kaggle

---

## 🛠️ Tech Stack
- Snowflake (data warehouse)
- SQL (ELT, analytics)
- Python (pandas, Snowflake connector)
- GitHub

---

## Data Load Validation
After loading data into Snowflake, the following checks are performed:
- Row count comparison between pandas DataFrame and Snowflake table
- Date range sanity check on invoice_date
- NULL handling verification
- Aggregate value comparison for unit_price

- ----
## Lessons Learned

- Real-world CSV files often use non-UTF8 encodings
- Datetime normalization is critical before warehouse loading
- Pandas NaN values must be converted to NULL for databases
- Row-by-row inserts do not scale; chunked bulk loading is required
- ELT (SQL-based transformations) improves transparency and reproducibility


These checks ensure the data load completed correctly and without silent corruption.
