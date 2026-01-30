import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="retail_wh",
    database="retail_db",
    schema="analytics"
)

query = """
SELECT country, SUM(lifetime_value) AS revenue
FROM customers
GROUP BY country
ORDER BY revenue DESC;
"""

df = pd.read_sql(query, conn)
print(df.head())

conn.close()
