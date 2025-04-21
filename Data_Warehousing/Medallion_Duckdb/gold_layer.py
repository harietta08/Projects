import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

# --- Connect to DuckDB and read from Silver Layer ---
con = duckdb.connect("sales.duckdb")
silver_count = con.execute("SELECT COUNT(*) FROM silver_sales").fetchone()[0]
print(f"\n📊 Silver Layer currently contains: {silver_count} rows\n")

# --- Read cleaned Silver Layer data ---
df = con.execute("SELECT * FROM silver_sales").fetchdf()

# --- GOLD LAYER AGGREGATIONS ---

# 1. Total sales per product category
sales_by_category = df.groupby("product_category")["total_price"].sum().reset_index()

# 2. Top 5 products by quantity sold
top_products = df.groupby(["product_id", "product_name"])["quantity_sold"].sum().reset_index()
top_products = top_products.sort_values(by="quantity_sold", ascending=False).head(5)

# 3. Total sales by payment method
sales_by_payment = df.groupby("payment_method")["total_price"].sum().reset_index()

# 4. Top 5 customers by total spend
top_customers = df.groupby("cust_id")["total_price"].sum().reset_index()
top_customers = top_customers.sort_values(by="total_price", ascending=False).head(5)

# 5. Monthly sales trend
monthly_sales = df.copy()
monthly_sales["purchase_month"] = pd.to_datetime(monthly_sales["purchase_date"]).dt.to_period("M").astype(str)
monthly_sales_trend = monthly_sales.groupby("purchase_month")["total_price"].sum().reset_index()

# --- Save to DuckDB Tables ---
con.execute("DROP TABLE IF EXISTS gold_sales_by_category;")
con.execute("CREATE TABLE gold_sales_by_category AS SELECT * FROM sales_by_category")

con.execute("DROP TABLE IF EXISTS gold_top_products;")
con.execute("CREATE TABLE gold_top_products AS SELECT * FROM top_products")

con.execute("DROP TABLE IF EXISTS gold_sales_by_payment;")
con.execute("CREATE TABLE gold_sales_by_payment AS SELECT * FROM sales_by_payment")

con.execute("DROP TABLE IF EXISTS gold_top_customers;")
con.execute("CREATE TABLE gold_top_customers AS SELECT * FROM top_customers")

con.execute("DROP TABLE IF EXISTS gold_monthly_sales_trend;")
con.execute("CREATE TABLE gold_monthly_sales_trend AS SELECT * FROM monthly_sales_trend")

# ---Visual Analytics ---
with PdfPages("gold_layer_charts.pdf") as pdf:
    # Total Sales by Product Category
    plt.figure(figsize=(10, 6))
    plt.bar(sales_by_category["product_category"], sales_by_category["total_price"], color=plt.cm.Set3.colors[:len(sales_by_category)])
    plt.title("Total Sales by Product Category")
    plt.xlabel("Product Category")
    plt.ylabel("Total Sales")
    plt.xticks(rotation=45)
    plt.tight_layout()
    pdf.savefig()
    plt.close()

    # Top 5 Products by Quantity Sold
    plt.figure(figsize=(10, 6))
    plt.bar(top_products["product_name"], top_products["quantity_sold"], color=plt.cm.Pastel2.colors[:len(top_products)])
    plt.title("Top 5 Products by Quantity Sold")
    plt.xlabel("Product Name")
    plt.ylabel("Quantity Sold")
    plt.xticks(rotation=45)
    plt.tight_layout()
    pdf.savefig()
    plt.close()

    # Total Sales by Payment Method
    plt.figure(figsize=(8, 5))
    plt.bar(sales_by_payment["payment_method"], sales_by_payment["total_price"], color=plt.cm.Accent.colors[:len(sales_by_payment)])
    plt.title("Total Sales by Payment Method")
    plt.xlabel("Payment Method")
    plt.ylabel("Total Sales")
    plt.tight_layout()
    pdf.savefig()
    plt.close()

    # Top 5 Customers by Total Spend
    plt.figure(figsize=(8, 5))
    plt.bar(top_customers["cust_id"], top_customers["total_price"], color=plt.cm.Set2.colors[:len(top_customers)])
    plt.title("Top 5 Customers by Total Spend")
    plt.xlabel("Customer ID")
    plt.ylabel("Total Spend")
    plt.tight_layout()
    pdf.savefig()
    plt.close()

    # Monthly Sales Trend
    plt.figure(figsize=(10, 5))
    plt.plot(monthly_sales_trend["purchase_month"].astype(str), monthly_sales_trend["total_price"], marker='o', color='dodgerblue')
    plt.title("Monthly Sales Trend")
    plt.xlabel("Month")
    plt.ylabel("Total Sales")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    pdf.savefig()
    plt.close()

# ---Summary ---
print("Gold Layer Aggregations Complete\n")
print("Total Sales by Product Category:\n", sales_by_category)
print("\nTop 5 Products by Quantity Sold:\n", top_products)
print("\nTotal Sales by Payment Method:\n", sales_by_payment)
print("\nTop 5 Customers by Total Spend:\n", top_customers)
print("\nMonthly Sales Trend:\n", monthly_sales_trend)
print("\n📄 Charts exported to gold_layer_charts.pdf")
