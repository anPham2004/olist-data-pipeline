# WHAT THIS FILE DOES?
# It transforms raw seller data into a dimension table format suitable for ClickHouse
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

# Extract and transform seller data
sellers_df = pd.read_csv('./data/raw/olist_sellers_dataset.csv', dtype=str).fillna('')
sellers_df.rename(columns={"seller_zip_code_prefix": "zip_code",
                           "seller_city": "city",
                           "seller_state": "state"}, inplace=True)
sellers_df.drop_duplicates(subset=['seller_id'], keep='last', inplace=True) # Ensure unique constraint
sellers_df['city'] = sellers_df['city'].str.title()

# Load transformed data into ClickHouse
client.command('DROP TABLE IF EXISTS dim_seller')
client.command('CREATE TABLE dim_seller (' \
               'seller_id String, ' \
               'zip_code String, ' \
               'city String, ' \
               'state String' \
               ') ENGINE = MergeTree() ORDER BY seller_id')
client.insert_df('dim_seller', sellers_df)
print(f"Loaded {len(sellers_df)} records into dim_seller")