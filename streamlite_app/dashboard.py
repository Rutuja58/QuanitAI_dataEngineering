import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px

# Page config
st.set_page_config(page_title="TechCorp Dashboard", layout="wide")

col1, col2 = st.columns([1, 6])  

with col1:
    st.image("./logo2.jpeg", width=150)

with col2:
    st.title("TechCorp Dashboard")



# Connect to SQLite DB
conn = sqlite3.connect("../streamlite_app/techcorp_cleaned.db")

# Load tables
customers = pd.read_sql("SELECT * FROM customers", conn)
products = pd.read_sql("SELECT * FROM products", conn)
orders = pd.read_sql("SELECT * FROM orders", conn)

# Sidebar Filters
st.sidebar.header("🔎 Filters")
status_options = orders["status"].dropna().unique()
category_options = products["product_category"].dropna().unique()

selected_status = st.sidebar.multiselect("Filter by Order Status", status_options, default=status_options)
selected_category = st.sidebar.multiselect("Filter by Product Category", category_options, default=category_options)

# Data Quality Metrics
st.subheader(" Data Quality Overview")
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Customers", len(customers))
    st.metric("Missing Emails", customers["email"].isnull().sum())

with col2:
    st.metric("Total Products", len(products))
    st.metric("Missing Brands", products["brand"].isnull().sum())

with col3:
    st.metric("Total Orders", len(orders))
    st.metric("Missing Shipping Address", orders["shipping_address"].isnull().sum())

# Business KPIs
st.subheader("📈 Key Metrics")
total_revenue = orders["order_value"].sum()
most_common_category = products["product_category"].mode()[0]
top_customer_id = orders.groupby("customer_id")["order_value"].sum().idxmax()

kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric("Total Revenue", f"${total_revenue:,.2f}")
kpi2.metric("Top Product Category", most_common_category.title())
kpi3.metric("Top Customer ID", top_customer_id)

# Sales Trend
st.subheader("📊 Sales Trend Over Time")
orders["order_timestamp"] = pd.to_datetime(orders["order_timestamp"], errors="coerce")
filtered_orders = orders[orders["status"].isin(selected_status)]
monthly_sales = filtered_orders.groupby(pd.Grouper(key="order_timestamp", freq="ME"))["order_value"].sum().reset_index()

fig_sales = px.line(monthly_sales, x="order_timestamp", y="order_value", title="Monthly Revenue")
st.plotly_chart(fig_sales, use_container_width=True)

# Top Products
st.subheader("🏆 Top Products")
top_products = filtered_orders.groupby("product_id")["order_value"].sum().nlargest(10).reset_index()
top_products = top_products.merge(products[["product_id", "product_name"]], on="product_id", how="left")
st.dataframe(top_products.rename(columns={"order_value": "Total Sales"}))

# Customer Segments
st.subheader("🧑 Customer Segmentation")
segment_counts = customers["segment"].value_counts().reset_index()
segment_counts.columns = ["Segment", "Count"]

fig_segments = px.pie(segment_counts, names="Segment", values="Count", title="Customer Segments")
st.plotly_chart(fig_segments, use_container_width=True)

st.subheader("📋 Product Explorer")

# Apply category filter
filtered_products = products[products["product_category"].isin(selected_category)]

# Search bar for product name
product_search = st.text_input("🔍 Search Product by Name")
if product_search:
    filtered_products = filtered_products[
        filtered_products["product_name"].str.contains(product_search, case=False, na=False)
    ]

st.dataframe(filtered_products)


conn.close()
