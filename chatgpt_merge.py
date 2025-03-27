import pandas as pd

# Load datasets
customers = pd.read_csv("./data/olist_customers_dataset.csv")
geolocation = pd.read_csv("./data/olist_geolocation_dataset.csv")
orders = pd.read_csv("./data/olist_orders_dataset.csv")
order_items = pd.read_csv("./data/olist_order_items_dataset.csv")
products = pd.read_csv("./data/olist_products_dataset.csv")
sellers = pd.read_csv("./data/olist_sellers_dataset.csv")
category_translation = pd.read_csv("./data/product_category_name_translation.csv")

# Merge datasets step by step
merged_df = orders.merge(order_items, on='order_id', how='left')
merged_df = merged_df.merge(products, on='product_id', how='left')
merged_df = merged_df.merge(category_translation, on='product_category_name', how='left')
merged_df = merged_df.merge(sellers, on='seller_id', how='left')
merged_df = merged_df.merge(customers, on='customer_id', how='left')
merged_df = merged_df.merge(geolocation, left_on='customer_zip_code_prefix', right_on='geolocation_zip_code_prefix', how='left')

# Convert date columns to datetime format
for col in ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']:
    merged_df[col] = pd.to_datetime(merged_df[col])

# Calculate actual delivery time
merged_df['actual_delivery_time'] = (merged_df['order_delivered_customer_date'] - merged_df['order_purchase_timestamp']).dt.days

# Drop unnecessary columns and handle missing values
merged_df = merged_df.drop(columns=['order_approved_at', 'order_delivered_carrier_date'])
merged_df = merged_df.dropna()

# Save the merged dataset
merged_df.to_csv("./data/chatgpt_merged_dataset.csv", index=False)

print("Data merging complete. Saved as merged_dataset.csv.")
