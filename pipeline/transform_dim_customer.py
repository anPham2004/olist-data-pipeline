# WHAT THIS FILE DOES?
# It transforms raw customer data into a dimension table format suitable for ClickHouse
# It reads raw data from a CSV file, processes it using pandas, and loads it into ClickHouse
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

# Extract and transform customer data
customers_df = pd.read_csv('./data/raw/olist_customers_dataset.csv', dtype=str).fillna('')
customers_df.drop(columns=['customer_id'], axis=1, inplace=True)
customers_df.rename(columns={"customer_unique_id": "customer_id", 
                             "customer_zip_code_prefix": "zip_code", 
                             "customer_city": "city", 
                             "customer_state": "state"}, inplace=True)
customers_df.drop_duplicates(subset=['customer_id'], 
                             keep='last', 
                             inplace=True) # Ensure unique constraint as ClickHouse does not have primary keys
customers_df['city'] = customers_df['city'].str.title()

# Load transformed data into ClickHouse
client.command('DROP TABLE IF EXISTS dim_customer')
client.command('CREATE TABLE dim_customer (' \
               'customer_id String, ' \
               'zip_code String, ' \
               'city String, ' \
               'state String' \
               ') ENGINE = MergeTree() ORDER BY customer_id')
client.insert_df('dim_customer', customers_df)
print(f"Loaded {len(customers_df)} records into dim_customer")