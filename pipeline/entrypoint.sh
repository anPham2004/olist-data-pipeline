#!/bin/bash

# Stop executing script on any error
set -e

echo "Waiting for ClickHouse to be initialized..."

# Loop to test connection to ClickHouse
# It will try to connect every 2 seconds until successful
python - <<END
import time
import clickhouse_connect
import os
import sys

host = 'warehouse_db'
port = 8123
user = os.getenv('CLICKHOUSE_USER')
password = os.getenv('CLICKHOUSE_PASSWORD')

while True:
    try:
        client = clickhouse_connect.get_client(host=host, port=port, username=user, password=password)
        print("Successfully connected to ClickHouse.")

        # Create the database if it doesn't exist
        client.command('CREATE DATABASE IF NOT EXISTS olist_dw')
        print("Database 'olist_dw' is ready.")
        break
    except Exception as e:
        print(f"ClickHouse is not ready yet. Retrying in 2 seconds... Error: {e}")
        time.sleep(2)
END

echo "Starting ETL pipeline..."

# 1. Generate dimension tables
echo "--- Loading dimenions ---"
python pipeline/transform_dim_date.py
python pipeline/transform_dim_customer.py
python pipeline/transform_dim_product.py
python pipeline/transform_dim_seller.py

# 2. Generate fact tables
echo "--- Loading facts ---"
python pipeline/transform_fact_sales.py
python pipeline/transform_fact_payments.py

echo "ETL pipeline completed successfully. Data is ready in ClickHouse."

echo "Initializing Streamlit dashboard..."
# Run Streamlit 
streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0