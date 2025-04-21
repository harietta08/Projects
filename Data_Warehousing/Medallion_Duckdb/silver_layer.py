import duckdb  
import pandas as pd  

# --- Connect to DuckDB and read from Bronze Layer ---
con = duckdb.connect("sales.duckdb")  # Open connection to DuckDB

df = con.execute("SELECT * FROM bronze_sales").fetchdf()  # Fetch all rows from bronze_sales table into pandas DataFrame

# ---  Clean and Transform ---

# 1. Cast product_price and total_price to float for numeric operations
for col in ["product_price", "total_price"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")  # Convert to numeric, set errors as NaN

# 2. Convert delivery_period to numeric type
df["delivery_period"] = pd.to_numeric(df["delivery_period"], errors="coerce")

# 3. Fill missing prices and delivery periods using product-level averages
price_avg = df.groupby("product_name")["product_price"].transform("mean")
df["product_price"] = df["product_price"].fillna(price_avg)

period_avg = df.groupby("product_name")["delivery_period"].transform("mean")
df["delivery_period"] = df["delivery_period"].fillna(period_avg)
df = df.dropna(subset=["delivery_period"])  # Remove if delivery period is still missing
df["delivery_period"] = df["delivery_period"].astype(int)  # Ensure integer type

# 4. Recalculate total_price where missing (price * quantity)
missing_total_price = df["total_price"].isnull()
df.loc[missing_total_price, "total_price"] = df.loc[missing_total_price, "product_price"] * df.loc[missing_total_price, "quantity_sold"]

# 5. Fill missing customer fields using cust_id group-based forward/backward fill
df = df.sort_values(by="order_id")  # Sort to preserve order for ffill/bfill
customer_cols = ["first_name", "last_name", "gender", "email"]
for col in customer_cols:
    df[col] = df.groupby("cust_id")[col].transform(lambda x: x.ffill().bfill())

# 6. Fill missing product fields using product_id group-based forward/backward fill
product_cols = ["product_name", "product_category"]
for col in product_cols:
    df[col] = df.groupby("product_id")[col].transform(lambda x: x.ffill().bfill())

# 7. Drop rows with any remaining missing values in critical columns
df_clean = df.dropna(subset=[
    "cust_id", "first_name", "last_name", "gender", "email", "product_id",
    "product_name", "product_category", "product_price", "quantity_sold",
    "purchase_date", "delivery_period", "payment_method", "shipping_address",
    "total_price"
]).copy()

# 8. Derive delivery_date from purchase_date + delivery_period
df_clean["delivery_date"] = pd.to_datetime(df_clean["purchase_date"]) + pd.to_timedelta(df_clean["delivery_period"], unit='D')
df_clean["delivery_date"] = df_clean["delivery_date"].dt.date  # Extract date only

# 9. Normalize payment_method field
payment_map = {
    "credit_card": "credit",
    "cash ": "cash",
    "cod": "cash",
    "cash": "cash",
    "paypal": "wallet",
    "apple pay": "wallet",
    "credit": "credit"
}
df_clean["payment_method"] = df_clean["payment_method"].str.strip().str.lower().replace(payment_map)

# 10. Normalize product_category field
category_map = {
    "food & snacks": "snacks",
    "food - snacks": "snacks",
    "electrnics": "electronics",
    "electronics ": "electronics",
    "clothing ": "clothing"
}
df_clean["product_category"] = df_clean["product_category"].str.strip().str.lower().replace(category_map)

# 11. Drop duplicate orders by order_id
df_clean = df_clean.drop_duplicates(subset=["order_id"])

# ---  Save to Silver Layer ---
con.execute("DROP TABLE IF EXISTS silver_sales;")  # Drop if silver_sales table exists
con.execute("CREATE TABLE silver_sales AS SELECT * FROM df_clean")  # Save cleaned DataFrame to DuckDB

# ---  Print total row count after cleaning ---
row_count = con.execute("SELECT COUNT(*) FROM silver_sales").fetchone()[0]  # Count rows in silver layer
print(f"Silver Layer transformation complete. Total rows after cleaning and transformation: {row_count}\n")
print("Sample of cleaned data:")
print(df_clean.head())  # Show sample output
