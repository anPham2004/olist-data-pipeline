# WHAT THIS FILE DOES?
# It transforms raw product data into a dimension table format suitable for ClickHouse
import os
import sys
import pandas as pd
import numpy as np
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

# Extract and transform product data
products_df = pd.read_csv('./data/raw/olist_products_dataset.csv', dtype=str).fillna('')
products_name_df = pd.read_csv('./data/raw/product_category_name_translation.csv', dtype=str).fillna('')
products_df = products_df.merge(products_name_df, how='left', on='product_category_name') # Merge to get English category names
products_df.drop(columns=['product_category_name', 
                          'product_name_lenght', 
                          'product_description_lenght', 
                          'product_photos_qty'], axis=1, inplace=True)
products_df.rename(columns={"product_category_name_english": "category_name",
                            "product_weight_g": "weight_g",
                            "product_length_cm": "length_cm",
                            "product_height_cm": "height_cm",
                            "product_width_cm": "width_cm"}, inplace=True)
products_df['category_name'] = products_df['category_name'].replace(r'^\s*$', np.nan, regex=True) # Handle empty strings
products_df['category_name'] = products_df['category_name'].str.capitalize().fillna('Unknown')
products_df[['product_id', 'category_name']] = products_df[['product_id', 'category_name']].astype(str)
# Clean numeric columns: convert to float and fill NaNs with 0
cols_to_numeric = ['weight_g', 'length_cm', 'height_cm', 'width_cm']
for col in cols_to_numeric:
    products_df[col] = pd.to_numeric(products_df[col], errors='coerce').fillna(0.0)
products_df.drop_duplicates(subset=['product_id'], keep='last', inplace=True) # Ensure unique constraint

# Load transformed data into ClickHouse
client.command('DROP TABLE IF EXISTS dim_product')
client.command('CREATE TABLE dim_product (' \
               'product_id String, ' \
               'category_name String, ' \
               'weight_g Float64, ' \
               'length_cm Float64, ' \
               'height_cm Float64, ' \
               'width_cm Float64, ' \
               ') ENGINE = MergeTree() ORDER BY product_id')
client.insert_df('dim_product', products_df)
print(f"Loaded {len(products_df)} records into dim_product")