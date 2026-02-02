import os
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from sqlalchemy import create_engine

# ----------------------------------------------------
# Load environment variables
# ----------------------------------------------------
load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

assert SNOWFLAKE_USER is not None, "SNOWFLAKE_USER not set"
assert SNOWFLAKE_PASSWORD is not None, "SNOWFLAKE_PASSWORD not set"
assert SNOWFLAKE_ACCOUNT is not None, "SNOWFLAKE_ACCOUNT not set"

# ----------------------------------------------------
# Create SQLAlchemy engine
# ----------------------------------------------------
engine = create_engine(
    f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@"
    f"{SNOWFLAKE_ACCOUNT}/retail_db/analytics"
    "?warehouse=retail_wh"
)

# ----------------------------------------------------
# Query with explicit aliases
# ----------------------------------------------------
query = """
SELECT
  DATE_TRUNC('month', order_date) AS MONTH,
  SUM(order_total) AS REVENUE
FROM analytics.orders
GROUP BY MONTH
ORDER BY MONTH;
"""

df = pd.read_sql(query, engine)

# ----------------------------------------------------
# Normalize column names (IMPORTANT)
# SQLAlchemy lowercases column names by default
# ----------------------------------------------------
df.columns = [c.upper() for c in df.columns]

# Debug print (can remove later)
print("Columns returned from Snowflake:", df.columns)
print(df.head())

# ----------------------------------------------------
# Visualization
# ----------------------------------------------------
plt.figure(figsize=(10, 5))
plt.plot(df["MONTH"], df["REVENUE"], marker="o")
plt.title("Monthly Revenue Trend")
plt.xlabel("Month")
plt.ylabel("Revenue")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
