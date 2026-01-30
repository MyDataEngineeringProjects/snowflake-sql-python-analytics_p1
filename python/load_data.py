import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="retail_wh",
    database="retail_db",
    schema="raw"
)

df = pd.read_csv("data/online_retail.csv")

cursor = conn.cursor()
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO retail_raw VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, tuple(row))

cursor.close()
conn.close()