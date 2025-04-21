import duckdb
import pandas as pd

# --- Connect to DuckDB and read from Bronze Layer ---
con = duckdb.connect("sales.duckdb")
df = con.execute("SELECT * FROM bronze_sales").fetchdf()

# --- 🧹 Clean and Transform ---

# 1. Cast product_price and total_price to float
for col in ["product_price", "total_price"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# 2. Cast delivery_period to numeric (initially)
df["delivery_period"] = pd.to_numeric(df["delivery_period"], errors="coerce")

# 3. Impute missing product_price and delivery_period using product-level average
price_avg = df.groupby("product_name")["product_price"].transform("mean")
df["product_price"] = df["product_price"].fillna(price_avg)

period_avg = df.groupby("product_name")["delivery_period"].transform("mean")
df["delivery_period"] = df["delivery_period"].fillna(period_avg)
df = df.dropna(subset=["delivery_period"])
df["delivery_period"] = df["delivery_period"].astype(int)

# 4. Recalculate total_price if still missing
# Also handles cases where quantity_sold is missing but product_price is present
missing_total_price = df["total_price"].isnull()
df.loc[missing_total_price, "total_price"] = df.loc[missing_total_price, "product_price"] * df.loc[missing_total_price, "quantity_sold"]

# 5. Impute customer fields using cust_id
df = df.sort_values(by="order_id")
customer_cols = ["first_name", "last_name", "gender", "email"]
for col in customer_cols:
    df[col] = df.groupby("cust_id")[col].transform(lambda x: x.ffill().bfill())

# 6. Impute product fields using product_id
product_cols = ["product_name", "product_category"]
for col in product_cols:
    df[col] = df.groupby("product_id")[col].transform(lambda x: x.ffill().bfill())

# 7. Drop rows with any remaining missing critical values
df_clean = df.dropna(subset=[
    "cust_id", "first_name", "last_name", "gender", "email", "product_id",
    "product_name", "product_category", "product_price", "quantity_sold",
    "purchase_date", "delivery_period", "payment_method", "shipping_address",
    "total_price"
]).copy()

# 8. Derive delivery_date = purchase_date + delivery_period
df_clean["delivery_date"] = pd.to_datetime(df_clean["purchase_date"]) + pd.to_timedelta(df_clean["delivery_period"], unit='D')
df_clean["delivery_date"] = df_clean["delivery_date"].dt.date

# 9. Normalize payment_method with advanced mapping
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

# 10. Normalize product_category with mapping
category_map = {
    "food & snacks": "snacks",
    "food - snacks": "snacks",
    "electrnics": "electronics",
    "electronics ": "electronics",
    "clothing ": "clothing"
}
df_clean["product_category"] = df_clean["product_category"].str.strip().str.lower().replace(category_map)

# 11. Remove duplicates by order_id
df_clean = df_clean.drop_duplicates(subset=["order_id"])

# --- 📆 Save to Silver Layer ---
con.execute("DROP TABLE IF EXISTS silver_sales;")
con.execute("CREATE TABLE silver_sales AS SELECT * FROM df_clean")

# --- 📊 Print total row count after cleaning ---
row_count = con.execute("SELECT COUNT(*) FROM silver_sales").fetchone()[0]
print(f"\n✅ Silver Layer transformation complete. Total rows after cleaning and transformation: {row_count}\n")
print("Sample of cleaned data:")
print(df_clean.head())
