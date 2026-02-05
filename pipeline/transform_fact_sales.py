# WHAT THIS FILE DOES?
# It generates a fact table for sales from raw sales data suitable for ClickHouse
# Sales data is essential for product performance and sales trend analysis
import os
import sys
import pandas as pd
import clickhouse_connect

# Establish ClickHouse connection
try:
    client = clickhouse_connect.get_client(
        host='warehouse_db',
        port=8123,
        username=os.getenv('CLICKHOUSE_USER'),
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        database='olist_dw'
    )
except Exception as e:
    print(f"Error connecting to ClickHouse: {e}")
    sys.exit(1)

# Extract raw data
ordered_items_df = pd.read_csv('data/raw/olist_order_items_dataset.csv')
orders_df = pd.read_csv('data/raw/olist_orders_dataset.csv')
customers_df = pd.read_csv('data/raw/olist_customers_dataset.csv')
customers_df = customers_df[['customer_id', 'customer_unique_id']]

# Filter, merge, and transform data
orders_df = orders_df[orders_df['order_status'] == 'delivered']
sales_df = ordered_items_df.merge(orders_df, how='inner', on='order_id')
sales_df = sales_df.merge(customers_df, how='inner', on='customer_id')
sales_df.drop(columns=['customer_id',
                       'shipping_limit_date',
                       'order_status',
                       'order_approved_at', 
                       'order_delivered_carrier_date', 
                       'order_delivered_customer_date', 
                       'order_estimated_delivery_date'], axis=1, inplace=True)
sales_df['order_purchase_timestamp'] = pd.to_datetime(sales_df['order_purchase_timestamp']).dt.date # Format date (YYYY-MM-DD)
sales_df.rename(columns={'customer_unique_id': 'customer_id',
                         'order_item_id': 'line_item_id',
                         'order_purchase_timestamp': 'date_key'}, inplace=True)

# Load fact sales data into ClickHouse
client.command('DROP TABLE IF EXISTS fact_sales')
client.command('CREATE TABLE fact_sales (' \
               'order_id String, ' \
               'line_item_id UInt8, ' \
               'customer_id String, ' \
               'product_id String, ' \
               'seller_id String, ' \
               'date_key Date, ' \
               'price Float64, ' \
               'freight_value Float64' \
               ') ENGINE = MergeTree() ORDER BY (date_key, order_id)')
client.insert_df('fact_sales', sales_df)
print(f"Loaded {len(sales_df)} records into fact_sales")