import os
import pandas as pd
import numpy as np
import snowflake.connector
from dotenv import load_dotenv

# ----------------------------------------------------
# Load environment variables (.env lives in repo root)
# ----------------------------------------------------
load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

# ----------------------------------------------------
# Connect to Snowflake
# ----------------------------------------------------
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse="retail_wh",
    database="retail_db",
    schema="raw"
)

cursor = conn.cursor()

# truncate data
cursor.execute("TRUNCATE TABLE raw.retail_raw")
print("raw.retail_raw truncated before load")

# ----------------------------------------------------
# Read CSV (real-world dataset uses latin1 encoding)
# ----------------------------------------------------
df = pd.read_csv(
    "data/online_retail.csv",
    encoding="latin1"
)

# ----------------------------------------------------
# Normalize InvoiceDate (CRITICAL)
# - Explicit format
# - Convert to ISO string for Snowflake
# ----------------------------------------------------
df["InvoiceDate"] = pd.to_datetime(
    df["InvoiceDate"],
    format="%m/%d/%y %H:%M",
    errors="coerce"
)

# Drop rows with invalid dates
df = df.dropna(subset=["InvoiceDate"])

# Convert datetime → ISO string (Snowflake-safe)
df["InvoiceDate"] = df["InvoiceDate"].dt.strftime("%Y-%m-%d %H:%M:%S")

# ----------------------------------------------------
# Enforce column order to match Snowflake table
# ----------------------------------------------------
df = df[
    [
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country"
    ]
]

# ----------------------------------------------------
# CRITICAL: Convert to object dtype so None is preserved
# ----------------------------------------------------
df = df.astype(object)

# Replace NaN with None (Snowflake-safe NULL)
df = df.where(pd.notnull(df), None)

# ----------------------------------------------------
# Insert data into Snowflake
# ----------------------------------------------------
# Bulk insert using executemany() in batches (FAST + SAFE)
# ----------------------------------------------------
insert_sql = """
INSERT INTO retail_raw
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

rows = list(df.itertuples(index=False, name=None))
total_rows = len(rows)
BATCH_SIZE = 10000

for start in range(0, total_rows, BATCH_SIZE):
    end = start + BATCH_SIZE
    batch = rows[start:end]
    cursor.executemany(insert_sql, batch)
    print(f"Inserted rows {start + 1} to {min(end, total_rows)}")


# ----------------------------------------------------
# Cleanup
# ----------------------------------------------------
cursor.close()
conn.close()
print("Done!");
# print(f"✅ Loaded {row_count} rows into raw.retail_raw")
