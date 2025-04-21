import duckdb  # DuckDB for local database
import pandas as pd  # Pandas for data handling
import numpy as np  # Numpy for numerical operations
import requests  # For API data fetching
import datetime  # To handle timestamps
import os  # For file and directory operations
import glob  # For working with file paths

# --- CONFIGURATION ---
API_URL = "https://api.mockaroo.com/api/0222dac0?key=88235c70&count=1000&format=csv"  # Mockaroo schema URL
DATA_DIR = 'C:/Users/harip/Documents/Projects/Data_Warehousing/Medallion_Duckdb/data'  # Directory to store raw CSVs
DB_PATH = 'sales.duckdb'  # Path to DuckDB database file

# --- Ensure the data directory exists ---
os.makedirs(DATA_DIR, exist_ok=True)  # Create the data directory if it doesn't exist

# --- Generate a unique filename with timestamp for each run ---
timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")  # Format current timestamp
filename = f"{DATA_DIR}/sales_{timestamp}.csv"  # Construct CSV filename

# --- Fetch data from Mockaroo ---
response = requests.get(API_URL)  # Call the Mockaroo API
if response.status_code == 200:
    with open(filename, 'wb') as f:
        f.write(response.content)  # Save the CSV content to file
    print(f"Data saved to: {filename}")
else:
    print(f"Failed to fetch data. Status: {response.status_code}")
    exit(1)  # Stop execution if fetch fails

# --- Load data ---
df = pd.read_csv(filename)  # Load the CSV into pandas DataFrame
print(f"Total rows loaded from CSV: {len(df)}")

# --- Rename 'id' to 'cust_id' ---
df.rename(columns={"id": "cust_id"}, inplace=True)  # Rename for clarity

# --- Parse purchase_date ---
df["purchase_date"] = pd.to_datetime(df["purchase_date"], format="%m/%d/%Y", errors="coerce").dt.date  # Convert to datetime.date

# --- Log invalid delivery_period values ---
invalid_delivery = df[df["delivery_period"].isnull() | (df["delivery_period"] == "") | (df["delivery_period"] == " ")]
if not invalid_delivery.empty:
    print(f"Found {len(invalid_delivery)} rows with invalid or missing delivery_period:")
    print(invalid_delivery[["cust_id", "purchase_date", "delivery_period"]])

# --- Connect to DuckDB ---
con = duckdb.connect(DB_PATH)  # Connect to DuckDB instance

# --- Get the current max order_id from bronze_sales ---
try:
    max_id = con.execute("SELECT MAX(order_id) FROM bronze_sales").fetchone()[0]  # Get highest existing order_id
    max_id = max_id if max_id is not None else 0  # Handle None
except:
    max_id = 0  # If table doesn't exist yet

# --- Assign new order_id incrementally ---
df["order_id"] = range(max_id + 1, max_id + 1 + len(df))  # Create sequential unique order_ids

# --- Drop unnamed/index columns ---
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]  # Remove index columns if present

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
    print("Missing values found:\n", missing_counts[missing_counts > 0])  # Log columns with missing data
else:
    print("No missing values.")

duplicate_orders = df[df.duplicated(subset="order_id", keep=False)]  # Check for duplicate order_ids
if not duplicate_orders.empty:
    print(f"Found {len(duplicate_orders)} duplicate rows based on 'order_id'")
    print(duplicate_orders[["order_id", "email", "product_name"]])
else:
    print("All order IDs are unique.")

# Check for non-numeric values in price columns
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
con.execute("INSERT INTO bronze_sales SELECT * FROM df")  # Insert into DuckDB table
row_count = con.execute("SELECT COUNT(*) FROM bronze_sales").fetchone()[0]  # Count total rows now
print(f"Inserted {len(df)} new rows into bronze_sales. Total rows in bronze_sales: {row_count}")
