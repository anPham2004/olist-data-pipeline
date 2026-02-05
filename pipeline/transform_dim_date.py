# WHAT THIS FILE DOES?
# It creates the time dimension data into a table for ClickHouse
# It generates date records from 2016-01-01 to 2021-12-31 and processes them using pandas
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

# Generate and feature engineer date data
dates_df = pd.DataFrame(pd.date_range(start='2016-01-01', end='2021-12-31'), columns=['date_key'])
dates_df['year'] = dates_df['date_key'].dt.year.astype(str)
dates_df['quarter'] = dates_df['date_key'].dt.quarter
dates_df['month'] = dates_df['date_key'].dt.month
dates_df['day'] = dates_df['date_key'].dt.day
dates_df['day_name'] = dates_df['date_key'].dt.day_name()
dates_df['is_weekend'] = dates_df['day_name'].isin(['Saturday', 'Sunday'])

# Load transformed data into ClickHouse
client.command('DROP TABLE IF EXISTS dim_date')
client.command('CREATE TABLE dim_date (' \
               'date_key Date, ' \
               'year String, ' \
               'quarter UInt8, ' \
               'month UInt8, ' \
               'day UInt8, ' \
               'day_name String, ' \
               'is_weekend UInt8' \
               ') ENGINE = MergeTree() ORDER BY date_key')
client.insert_df('dim_date', dates_df)
print(f"Loaded {len(dates_df)} records into dim_date")