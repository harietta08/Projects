import duckdb
import pandas as pd
import numpy as np
import requests
import datetime
import os
import glob

# --- CONFIGURATION ---
API_URL = "https://api.mockaroo.com/api/0222dac0?key=88235c70&count=1000&format=csv"
DATA_DIR = 'C:/Users/harip/Documents/Projects/Data_Warehousing/Medallion_Duckdb/data'
DB_PATH = 'sales.duckdb'

# --- Ensure the data directory exists ---
os.makedirs(DATA_DIR, exist_ok=True)

# --- Generate a unique filename with timestamp for each run ---
timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
filename = f"{DATA_DIR}/sales_{timestamp}.csv"

# --- Fetch data from Mockaroo ---
response = requests.get(API_URL)
if response.status_code == 200:
    with open(filename, 'wb') as f:
        f.write(response.content)
    print(f"Data saved to: {filename}")
else:
    print(f"Failed to fetch data. Status: {response.status_code}")
    exit(1)

# --- Load data ---
df = pd.read_csv(filename)
print(f"Total rows loaded from CSV: {len(df)}")

# --- Rename 'id' to 'cust_id' ---
df.rename(columns={"id": "cust_id"}, inplace=True)

# --- Parse purchase_date ---
df["purchase_date"] = pd.to_datetime(df["purchase_date"], format="%m/%d/%Y", errors="coerce").dt.date

# --- Log invalid delivery_period values (but do not clean) ---
invalid_delivery = df[df["delivery_period"].isnull() | (df["delivery_period"] == "") | (df["delivery_period"] == " ")]
if not invalid_delivery.empty:
    print(f"Found {len(invalid_delivery)} rows with invalid or missing delivery_period:")
    print(invalid_delivery[["cust_id", "purchase_date", "delivery_period"]])

# --- Connect to DuckDB ---
con = duckdb.connect(DB_PATH)

# --- Get the current max order_id from bronze_sales ---
try:
    max_id = con.execute("SELECT MAX(order_id) FROM bronze_sales").fetchone()[0]
    max_id = max_id if max_id is not None else 0
except:
    max_id = 0

# --- Assign new order_id incrementally ---
df["order_id"] = range(max_id + 1, max_id + 1 + len(df))

# --- Drop unnamed/index columns ---
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

# --- Reorder columns ---
df = df[[ 
    'order_id', 'cust_id', 'first_name', 'last_name', 'gender', 'email', 'product_id',
    'product_name', 'product_category', 'product_price',
    'quantity_sold', 'purchase_date', 'delivery_period',
    'payment_method', 'shipping_address', 'total_price'
]]

# --- Validation Checks ---
missing_counts = df.isnull().sum()
if missing_counts.any():
    print("Missing values found:\n", missing_counts[missing_counts > 0])
else:
    print("No missing values.")

duplicate_orders = df[df.duplicated(subset="order_id", keep=False)]
if not duplicate_orders.empty:
    print(f"Found {len(duplicate_orders)} duplicate rows based on 'order_id'")
    print(duplicate_orders[["order_id", "email", "product_name"]])
else:
    print("All order IDs are unique.")

for col in ["product_price", "total_price"]:
    non_numeric = pd.to_numeric(df[col], errors="coerce").isnull()
    if non_numeric.any():
        print(f"Found {non_numeric.sum()} rows with non-numeric {col}:")
        print(df.loc[non_numeric, [col, "product_name", "cust_id"]])

# --- Create bronze_sales table if not exists ---
con.execute("""
CREATE TABLE IF NOT EXISTS bronze_sales (
    order_id INTEGER,
    cust_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    email VARCHAR,
    product_id VARCHAR,
    product_name VARCHAR,
    product_category VARCHAR,
    product_price VARCHAR,
    quantity_sold INTEGER,
    purchase_date DATE,
    delivery_period VARCHAR,
    payment_method VARCHAR,
    shipping_address VARCHAR,
    total_price VARCHAR
);
""")

# --- Append new data ---
con.execute("INSERT INTO bronze_sales SELECT * FROM df")
row_count = con.execute("SELECT COUNT(*) FROM bronze_sales").fetchone()[0]
print(f"Inserted {len(df)} new rows into bronze_sales. Total rows in bronze_sales: {row_count}")
