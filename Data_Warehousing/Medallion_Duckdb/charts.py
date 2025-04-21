import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

# Connect to DuckDB
con = duckdb.connect("sales.duckdb")

# Load Gold Layer tables
sales_by_category = con.execute("SELECT * FROM gold_sales_by_category").fetchdf()
delivery_time_by_payment = con.execute("SELECT * FROM gold_delivery_time_by_payment").fetchdf()
top_products = con.execute("SELECT * FROM gold_top_products").fetchdf()

# Create a multi-page PDF
with PdfPages("gold_layer_charts.pdf") as pdf:

    # 1. Total Sales by Product Category
    plt.figure(figsize=(10, 6))
    colors = plt.cm.tab20.colors[:len(sales_by_category)]
    plt.bar(sales_by_category["product_category"], sales_by_category["total_price"], color=colors)
    plt.title("Total Sales by Product Category")
    plt.xlabel("Product Category")
    plt.ylabel("Total Sales")
    plt.xticks(rotation=45)
    plt.tight_layout()
    pdf.savefig()
    plt.close()

    # 2. Average Delivery Time by Payment Method
    plt.figure(figsize=(8, 5))
    colors = plt.cm.Set2.colors[:len(delivery_time_by_payment)]
    plt.barh(delivery_time_by_payment["payment_method"], delivery_time_by_payment["delivery_period"], color=colors)
    plt.title("Average Delivery Time by Payment Method")
    plt.xlabel("Average Delivery Period (Days)")
    plt.ylabel("Payment Method")
    plt.tight_layout()
    pdf.savefig()
    plt.close()

    # 3. Top 5 Products by Quantity Sold
    plt.figure(figsize=(10, 6))
    colors = plt.cm.Pastel1.colors[:len(top_products)]
    plt.bar(top_products["product_name"], top_products["quantity_sold"], color=colors)
    plt.title("Top 5 Products by Quantity Sold")
    plt.xlabel("Product Name")
    plt.ylabel("Quantity Sold")
    plt.xticks(rotation=45)
    plt.tight_layout()
    pdf.savefig()
    plt.close()

print("✅ Charts saved as PNGs and compiled into gold_layer_charts.pdf.")
