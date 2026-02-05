# WHAT THIS FILE DOES?
# It generates a fact table for payments from raw payment data suitable for ClickHouse
# Payment data is essential for revenue analysis
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
order_payments_df = pd.read_csv('data/raw/olist_order_payments_dataset.csv')
orders_df = pd.read_csv('data/raw/olist_orders_dataset.csv')
customers_df = pd.read_csv('data/raw/olist_customers_dataset.csv')

# Filter, merge, and transform data
orders_df = orders_df[orders_df['order_status'] == 'delivered']
payments_df = order_payments_df.merge(orders_df, how='inner', on='order_id')
payments_df = payments_df.merge(customers_df, how='inner', on='customer_id')
payments_df['order_purchase_timestamp'] = pd.to_datetime(payments_df['order_purchase_timestamp']).dt.date # Format date (YYYY-MM-DD)
payments_df.rename(columns={'order_purchase_timestamp': 'date_key',
                            'customer_unique_id': 'customer_id'}, inplace=True)
payments_df.drop(columns=['customer_id',
                          'order_status',
                          'order_approved_at',
                          'order_delivered_carrier_date',
                          'order_delivered_customer_date',
                          'order_estimated_delivery_date',
                          'customer_zip_code_prefix',
                          'customer_city',
                          'customer_state'], axis=1, inplace=True)

# Load fact payments data into ClickHouse
client.command('DROP TABLE IF EXISTS fact_payments')
client.command('CREATE TABLE fact_payments (' \
                'order_id String, ' \
                'customer_id String, ' \
                'payment_sequential UInt8, ' \
                'payment_type String, ' \
                'payment_installments UInt8, ' \
                'payment_value Float64, ' \
                'date_key Date ' \
                ') ENGINE = MergeTree() ORDER BY (date_key, order_id, payment_sequential)')
client.insert_df('fact_payments', payments_df)
print(f"Loaded {len(payments_df)} records into fact_payments")