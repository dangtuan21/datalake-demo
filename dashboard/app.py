#!/usr/bin/env python3
"""
Retail Data Lake Dashboard
Streamlit dashboard for visualizing retail analytics
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
from datetime import datetime
import json

# Configuration constants
BATCH_SIZE = 200

# Page config
st.set_page_config(
    page_title="Retail Data Lake Dashboard",
    page_icon="🏪",
    layout="wide",
    initial_sidebar_state="expanded"
)

# API base URL
API_BASE_URL = "http://localhost:8000"

# Helper functions
@st.cache_data(ttl=60)  # Cache for 1 minute
def fetch_api_data(endpoint: str):
    """Fetch data from API with caching"""
    try:
        response = requests.get(f"{API_BASE_URL}{endpoint}")
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"API Error: {response.status_code}")
            return None
    except requests.exceptions.ConnectionError:
        st.error("🔌 Cannot connect to API. Please ensure the API server is running on port 8000.")
        return None
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None

def format_currency(value):
    """Format value as currency"""
    return f"${value:,.2f}"

def format_number(value):
    """Format number with commas"""
    return f"{value:,}"

# Main dashboard
def main():
    # Header
    st.title("🏪 Retail Data Lake Dashboard")
    st.markdown("Real-time analytics from your Snowflake data lake")
    
    # Sidebar
    st.sidebar.title("📊 Navigation")
    
    # Check API health
    health_data = fetch_api_data("/health")
    if health_data:
        if health_data.get("status") == "healthy":
            st.sidebar.success("✅ API Connected")
        else:
            st.sidebar.error("❌ API Unhealthy")
    else:
        st.sidebar.error("🔌 API Disconnected")
        st.stop()
    
    # Navigation
    page = st.sidebar.selectbox(
        "Select Page",
        ["📈 Overview", "📦 Products", "👥 Customers", "🌍 Countries", "🔄 ETL Status"]
    )
    
    # Auto-refresh option
    auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh (30s)")
    if auto_refresh:
        time.sleep(30)
        st.rerun()
    
    # Manual refresh button
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    # Page routing
    if page == "📈 Overview":
        show_overview()
    elif page == "📦 Products":
        show_products()
    elif page == "👥 Customers":
        show_customers()
    elif page == "🌍 Countries":
        show_countries()
    elif page == "🔄 ETL Status":
        show_etl_status()

def show_overview():
    """Overview dashboard page"""
    st.header("📈 Data Lake Overview")
    
    # Get summary data
    summary_data = fetch_api_data("/api/summary")
    sales_metrics = fetch_api_data("/api/sales/metrics")
    
    if not summary_data or not sales_metrics:
        st.error("Failed to load overview data")
        return
    
    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "📊 Total Records",
            format_number(summary_data["staging_records"]),
            help="Total records in staging table"
        )
    
    with col2:
        st.metric(
            "💰 Total Sales",
            format_currency(sales_metrics["total_sales"]),
            help="Total revenue from all transactions"
        )
    
    with col3:
        st.metric(
            "🛒 Total Orders",
            format_number(sales_metrics["total_orders"]),
            help="Total number of orders"
        )
    
    with col4:
        st.metric(
            "👥 Total Customers",
            format_number(sales_metrics["total_customers"]),
            help="Unique customers"
        )
    
    # Second row of metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "📦 Products",
            format_number(summary_data["products"]),
            help="Unique products in catalog"
        )
    
    with col2:
        st.metric(
            "🌍 Countries",
            format_number(summary_data["countries"]),
            help="Countries with sales"
        )
    
    with col3:
        st.metric(
            "💵 Avg Order Value",
            format_currency(sales_metrics["average_order_value"]),
            help="Average order value"
        )
    
    with col4:
        st.metric(
            "🔄 Last Updated",
            summary_data["last_updated"][:19],
            help="Data freshness"
        )
    
    # Charts row
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📅 Daily Sales Trend")
        daily_sales = fetch_api_data("/api/sales/daily?limit=30")
        if daily_sales:
            df_daily = pd.DataFrame(daily_sales)
            fig = px.line(
                df_daily, 
                x='sale_date', 
                y='total_revenue',
                title="Daily Revenue (Last 30 Days)",
                labels={'total_revenue': 'Revenue ($)', 'sale_date': 'Date'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🏆 Top Products")
        top_products = fetch_api_data("/api/products/top?limit=5")
        if top_products:
            df_products = pd.DataFrame(top_products)
            fig = px.bar(
                df_products,
                x='total_revenue',
                y='stock_code',
                orientation='h',
                title="Top 5 Products by Revenue",
                labels={'total_revenue': 'Revenue ($)', 'stock_code': 'Product Code'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

def show_products():
    """Products analysis page"""
    st.header("📦 Products Analysis")
    
    # Controls
    col1, col2 = st.columns(2)
    with col1:
        limit = st.selectbox("Number of products", [10, 20, 50, 100], index=0)
    with col2:
        sort_by = st.selectbox("Sort by", ["total_revenue", "total_quantity_sold", "unique_customers"])
    
    # Get products data
    products_data = fetch_api_data(f"/api/products?limit={limit}&sort_by={sort_by}")
    
    if not products_data:
        st.error("Failed to load products data")
        return
    
    df_products = pd.DataFrame(products_data)
    
    # Products table
    st.subheader(f"📊 Top {len(df_products)} Products")
    
    # Format the dataframe for display
    display_df = df_products.copy()
    display_df['total_revenue'] = display_df['total_revenue'].apply(lambda x: f"${x:,.2f}")
    display_df['average_unit_price'] = display_df['average_unit_price'].apply(lambda x: f"${x:.2f}")
    display_df['total_quantity_sold'] = display_df['total_quantity_sold'].apply(lambda x: f"{x:,}")
    
    st.dataframe(
        display_df,
        column_config={
            "stock_code": "Product Code",
            "description": "Description",
            "total_quantity_sold": "Qty Sold",
            "total_revenue": "Revenue",
            "average_unit_price": "Avg Price",
            "unique_customers": "Customers"
        },
        use_container_width=True
    )
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💰 Revenue Distribution")
        fig = px.pie(
            df_products.head(10),
            values='total_revenue',
            names='stock_code',
            title="Revenue by Product (Top 10)"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📊 Quantity vs Revenue")
        fig = px.scatter(
            df_products,
            x='total_quantity_sold',
            y='total_revenue',
            hover_data=['stock_code', 'description'],
            title="Quantity Sold vs Revenue"
        )
        st.plotly_chart(fig, use_container_width=True)

def show_customers():
    """Customers analysis page"""
    st.header("👥 Customer Analysis")
    
    # Customer segments
    segments_data = fetch_api_data("/api/customers/segments")
    if segments_data:
        st.subheader("🎯 Customer Segments")
        df_segments = pd.DataFrame(segments_data)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.pie(
                df_segments,
                values='customer_count',
                names='customer_segment',
                title="Customer Count by Segment"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(
                df_segments,
                x='customer_segment',
                y='total_spent',
                title="Total Spending by Segment"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Customer details
    col1, col2 = st.columns(2)
    with col1:
        limit = st.selectbox("Number of customers", [10, 20, 50], index=0)
    with col2:
        segment_filter = st.selectbox(
            "Filter by segment", 
            ["All", "VIP", "HIGH_VALUE", "MEDIUM_VALUE", "LOW_VALUE", "NEW"]
        )
    
    # Build API endpoint
    endpoint = f"/api/customers?limit={limit}"
    if segment_filter != "All":
        endpoint += f"&segment={segment_filter}"
    
    customers_data = fetch_api_data(endpoint)
    
    if customers_data:
        df_customers = pd.DataFrame(customers_data)
        
        # Format for display
        display_df = df_customers.copy()
        display_df['total_amount_spent'] = display_df['total_amount_spent'].apply(lambda x: f"${x:,.2f}")
        
        st.subheader(f"👥 Customer Details ({segment_filter})")
        st.dataframe(
            display_df,
            column_config={
                "customer_id": "Customer ID",
                "country": "Country",
                "total_orders": "Orders",
                "total_amount_spent": "Total Spent",
                "customer_segment": "Segment",
                "days_since_last_purchase": "Days Since Last Purchase"
            },
            use_container_width=True
        )

def show_countries():
    """Countries analysis page"""
    st.header("🌍 Geographic Analysis")
    
    # Get countries data
    countries_data = fetch_api_data("/api/countries?limit=20")
    
    if not countries_data:
        st.error("Failed to load countries data")
        return
    
    df_countries = pd.DataFrame(countries_data)
    
    # Key metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            "🌍 Total Countries",
            len(df_countries),
            help="Countries with sales"
        )
    
    with col2:
        st.metric(
            "💰 Top Country Revenue",
            format_currency(df_countries['total_revenue'].max()),
            help="Highest revenue country"
        )
    
    with col3:
        top_country = df_countries.loc[df_countries['total_revenue'].idxmax(), 'country']
        st.metric(
            "🏆 Top Country",
            top_country,
            help="Country with highest revenue"
        )
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💰 Revenue by Country")
        fig = px.bar(
            df_countries.head(10),
            x='total_revenue',
            y='country',
            orientation='h',
            title="Top 10 Countries by Revenue"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("👥 Customers by Country")
        fig = px.bar(
            df_countries.head(10),
            x='total_customers',
            y='country',
            orientation='h',
            title="Top 10 Countries by Customer Count"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Countries table
    st.subheader("🌍 All Countries")
    display_df = df_countries.copy()
    display_df['total_revenue'] = display_df['total_revenue'].apply(lambda x: f"${x:,.2f}")
    
    st.dataframe(
        display_df,
        column_config={
            "country": "Country",
            "total_customers": "Customers",
            "total_orders": "Orders",
            "total_revenue": "Revenue"
        },
        use_container_width=True
    )

def show_etl_status():
    """ETL pipeline status page"""
    st.header("🔄 ETL Pipeline Status")
    
    # Show current data lake status instead of batch info
    summary_data = fetch_api_data("/api/summary")
    if summary_data:
        st.subheader("📊 Current Data Lake Status")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "📈 Total Records",
                f"{summary_data['staging_records']:,}",
                help="Records in staging table"
            )
        
        with col2:
            st.metric(
                "💰 Transactions",
                f"{summary_data['transactions']:,}",
                help="Processed transactions"
            )
        
        with col3:
            st.metric(
                "📦 Products",
                f"{summary_data['products']:,}",
                help="Unique products"
            )
        
        with col4:
            st.metric(
                "👥 Customers",
                f"{summary_data['customers']:,}",
                help="Unique customers"
            )
    
    # ETL Progress
    st.subheader("🔄 ETL Progress")
    
    # Calculate progress
    total_possible = 541909  # Total rows in CSV
    current_records = summary_data['staging_records'] if summary_data else 0
    progress_pct = (current_records / total_possible) * 100
    
    st.progress(progress_pct / 100)
    st.write(f"**Progress**: {current_records:,} / {total_possible:,} records ({progress_pct:.1f}%)")
    
    # Next batch info
    next_batch = (current_records // BATCH_SIZE) + 1
    next_start = current_records + 1
    next_end = min(next_start + BATCH_SIZE - 1, total_possible)
    
    st.info(f"**Next Batch**: Batch {next_batch} (rows {next_start:,} - {next_end:,})")
    
    # Manual ETL controls
    st.subheader("🎮 ETL Controls")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("▶️ Load Next Batch", help=f"Load batch {next_batch}"):
            st.info(f"To load the next batch, run: `python3 etl/incremental_etl_pipeline.py --next`")
    
    with col2:
        if st.button("📊 Check Status", help="Check current ETL status"):
            st.info("To check status, run: `python3 etl/incremental_etl_pipeline.py --status`")
    
    # Latest batch info (if available)
    latest_batch = fetch_api_data("/api/batches/latest")
    if latest_batch and latest_batch.get('execution_type') != 'No batches yet':
        st.subheader("📊 Latest Batch")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            status_color = "🟢" if latest_batch.get("status") == "COMPLETED" else "🔴"
            st.metric(
                "Status",
                f"{status_color} {latest_batch.get('status', 'N/A')}",
                help="Latest batch status"
            )
        
        with col2:
            st.metric(
                "Batch Type",
                latest_batch.get("execution_type", "N/A"),
                help="Batch identifier"
            )
        
        with col3:
            st.metric(
                "Records Processed",
                format_number(latest_batch.get("rows_processed", 0) or 0),
                help="Rows processed in latest batch"
            )
        
        with col4:
            duration = latest_batch.get("duration_seconds", 0) or 0
            st.metric(
                "Duration",
                f"{duration}s",
                help="Processing time"
            )
    
    # Instructions for running ETL
    st.subheader("📖 ETL Instructions")
    
    st.code("""
# Check current status
python3 etl/incremental_etl_pipeline.py --status

# Load next batch
python3 etl/incremental_etl_pipeline.py --next

# Load specific batch
python3 etl/incremental_etl_pipeline.py --batch 3
    """, language="bash")
    
    st.info("💡 **Tip**: Run these commands in your terminal to manage the ETL pipeline.")

if __name__ == "__main__":
    main()
