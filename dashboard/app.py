# WHAT THIS FILE DOES?
# This is the main dashboard application for visualizing Olist e-commerce data
# It connects to ClickHouse to fetch data and uses Streamlit and Plotly for interactive visualizations
# The dashboard provides insights into sales performance, customer behavior, and market trends
# It allows filtering by year and displays key performance indicators (KPIs), charts, and finance insights
import os
import sys
import pandas as pd
import clickhouse_connect
import streamlit as st
import plotly.express as px

# Streamlit page configuration
st.set_page_config(page_title="Olist E-commerce Dashboard",
                   page_icon="🛒",
                   layout="wide")

@st.cache_data # Cache the data fetching function
def get_data(query_sql):
# Input: SQL query string
# Output: DataFrame with query results
    try: # Establish ClickHouse connection
        client = clickhouse_connect.get_client(
            host='warehouse_db',
            port=8123,
            username=os.getenv('CLICKHOUSE_USER'),
            password=os.getenv('CLICKHOUSE_PASSWORD'),
            database='olist_dw'
        )
        data = client.query_df(query_sql)
        return data
    except Exception as e:
        st.error(f"Error connecting to ClickHouse: {e}")
        return pd.DataFrame()

# Sidebar for year filter
year_list = get_data("SELECT DISTINCT year FROM dim_date WHERE year <= '2018' ORDER BY year DESC")['year'].tolist()
if not year_list:
    st.warning("No data available for the selected year.")
    st.stop()
selected_year = st.sidebar.selectbox("Select Year", year_list, index=0)

# Main Dashboard Title
st.title(f"Olist E-commerce Dashboard - {selected_year} Performance")
st.markdown("---")

# Display KPIs
st.markdown("### Key Performance Indicators (KPIs)")
kpi_query = f"""
    SELECT
        SUM(f.price + f.freight_value) AS total_revenue,
        COUNT(DISTINCT f.order_id) AS total_orders,
        COUNT(DISTINCT f.customer_id) AS total_customers
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.year = '{selected_year}'
"""
kpi_df = get_data(kpi_query)
if kpi_df.empty:
    st.warning("No KPI data available for the selected year.")
    st.stop()
else:
    col1, col2, col3 = st.columns(3)
    with col1:
        revenue = kpi_df['total_revenue'][0]
        st.metric("Total Revenue", f"${revenue:,.2f}")
    with col2:
        order = kpi_df['total_orders'][0]
        st.metric("Total Orders", f"{order:,}")
    with col3:
        customer = kpi_df['total_customers'][0]
        st.metric("Total Customers", f"{customer:,}")

# Sales Performance Visualizations
st.markdown("---")
st.markdown("### Monthly Revenue Trend")
monthly_revenue_query = f"""
    SELECT
        d.month,
        SUM(f.price + f.freight_value) AS monthly_revenue
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        WHERE d.year = '{selected_year}'
        GROUP BY d.month
        ORDER BY d.month
"""
monthly_revenue_df = get_data(monthly_revenue_query)
if monthly_revenue_df.empty:
    st.warning("No monthly revenue data available for the selected year.")
else:
    fig_revenue = px.line(monthly_revenue_df, x='month', y='monthly_revenue',
                          labels={'month': 'Month', 'monthly_revenue': 'Revenue'})
    fig_revenue.update_xaxes(tickmode='linear', tick0=1, dtick=1, range=[0.5, 12.5])
    st.plotly_chart(fig_revenue, use_container_width=True)

st.markdown("---")
st.markdown("### Top 10 Products by Sales")
top_products_query = f"""
    SELECT
        p.category_name,
        COUNT(f.product_id) AS total_sales
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.year = '{selected_year}'
    GROUP BY p.category_name
    ORDER BY total_sales DESC
    LIMIT 10
"""
top_products_df = get_data(top_products_query)
if top_products_df.empty:
    st.warning("No product sales data available for the selected year.")
else:
    fig_products = px.bar(top_products_df, x='category_name', y='total_sales',
                          labels={'category_name': 'Product Category', 'total_sales': 'Total Sales'})
    st.plotly_chart(fig_products, use_container_width=True)

# Finance & Market Insights
st.markdown("---")
st.header("Finance & Market Insights")
col_finance1, col_finance2 = st.columns(2)

with col_finance1:
    st.subheader("Sunburst Chart for Payment Methods")
    payment_methods_query = f"""
        SELECT
            p.payment_type,
            SUM(p.payment_value) AS total_payment
        FROM fact_payments p
        JOIN dim_date d ON p.date_key = d.date_key
        WHERE d.year = '{selected_year}'
        GROUP BY p.payment_type
    """
    payment_methods_df = get_data(payment_methods_query)
    if payment_methods_df.empty:
        st.info("No data for payment methods.")
    else:
        fig_payments = px.pie(payment_methods_df, values='total_payment', names='payment_type',
                              hole=0.4)
        st.plotly_chart(fig_payments, use_container_width=True)

with col_finance2:
    st.subheader("Installment Analysis")
    installment_query = f"""
        SELECT
            p.payment_installments,
            COUNT(DISTINCT p.order_id) AS num_orders,
            SUM(p.payment_value) AS total_value
        FROM fact_payments p
        JOIN dim_date d ON p.date_key = d.date_key
        WHERE d.year = '{selected_year}'
        AND p.payment_installments > 0
        GROUP BY p.payment_installments
        ORDER BY p.payment_installments
    """
    installment_df = get_data(installment_query)
    if installment_df.empty:
        st.info("No installment data available.")
    else:
        fig_installments = px.bar(installment_df, x='payment_installments', y='num_orders',
                                labels={'payment_installments': 'Installment Months', 'num_orders': 'Number of Orders'},
                                color_discrete_sequence=['#3dd56d'])
        st.plotly_chart(fig_installments, use_container_width=True)

# Geospatial Sales Visualization
st.markdown("---")
st.subheader("Revenue by State (Brazil)")
geo_query = f"""
    SELECT
        c.state,
        SUM(s.price + s.freight_value) AS total_sales
    FROM fact_sales s
    JOIN dim_customer c ON s.customer_id = c.customer_id
    JOIN dim_date d ON s.date_key = d.date_key
    WHERE d.year = '{selected_year}'
    GROUP BY c.state
"""
geo_df = get_data(geo_query)
if geo_df.empty:
    st.warning("No geospatial sales data available for the selected year.")
else:
    fig_geo = px.treemap(geo_df, 
                            path=['state'], 
                            values='total_sales',
                            color='total_sales',
                            color_continuous_scale='Blues')
    st.plotly_chart(fig_geo, use_container_width=True)