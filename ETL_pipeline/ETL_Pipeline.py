import pandas as pd
import sqlite3
import os
from utils.cleaning_functions import clean_customers, clean_products, clean_orders



customers = pd.read_json("./dataset/raw_data/customers_messy_data.json")

orders = pd.read_csv("./dataset/raw_data/orders_unstructured_data.csv")

products = pd.read_json("./dataset/raw_data/products_inconsistent_data.json")

# Cleaning data using cleaning function
clean_customers_df = clean_customers(customers)
clean_products_df = clean_products(products)
clean_orders_df = clean_orders(orders)

print(f" Cleaned customers: {len(clean_customers_df)} rows")
print(f" Cleaned products: {len(clean_products_df)} rows")
print(f" Cleaned orders (before FK filter): {len(clean_orders_df)} rows")

# FK integrity filter
clean_orders_df = clean_orders_df[
    
    clean_orders_df["customer_id"].isin(clean_customers_df["customer_id"]) &
    clean_orders_df["product_id"].isin(clean_products_df["product_id"])
]

print(f"Cleaned orders (after FK filter): {len(clean_orders_df)} rows")

# Convert datetime columns to strings for SQLite
for col in ["registered_on"]:
    if col in clean_customers_df.columns:
        clean_customers_df[col] = clean_customers_df[col].astype(str)

for col in ["created_date", "last_updated"]:
    if col in clean_products_df.columns:
        clean_products_df[col] = clean_products_df[col].astype(str)

if "order_timestamp" in clean_orders_df.columns:
    clean_orders_df["order_timestamp"] = clean_orders_df["order_timestamp"].astype(str)

# Save to CSV
os.makedirs("../dataset/cleaned", exist_ok=True)
clean_customers_df.to_csv("../dataset/cleaned/customers_cleaned.csv", index=False)
clean_products_df.to_csv("../dataset/cleaned/products_cleaned.csv", index=False)
clean_orders_df.to_csv("../dataset/cleaned/orders_cleaned.csv", index=False)

# Load into SQLite
conn = sqlite3.connect("./streamlite_app/techcorp_cleaned.db")
clean_customers_df.to_sql("customers", conn, if_exists="replace", index=False)
clean_products_df.to_sql("products", conn, if_exists="replace", index=False)
clean_orders_df.to_sql("orders", conn, if_exists="replace", index=False)


conn.execute(
    "CREATE INDEX IF NOT EXISTS idx_customer_id ON customers(customer_id);"
    )
conn.execute(
    "CREATE INDEX IF NOT EXISTS idx_product_id ON products(product_id);"
    )
conn.execute(
    "CREATE INDEX IF NOT EXISTS idx_order_customer ON orders(customer_id);"
    )
conn.execute(
    "CREATE INDEX IF NOT EXISTS idx_order_product ON orders(product_id);"
    )

conn.commit()
conn.close()

print("✅ Final ETL pipeline completed successfully.")
