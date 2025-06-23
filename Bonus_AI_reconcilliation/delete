import pandas as pd
import os

## documented Approach in pdf document file


recon_raw = pd.read_csv('./dataset/raw_data/reconciliation_challenge_data.csv')

# Step 2: Define a mapping from raw fields to our standardized schema
column_mapping = {
    "client_reference": "customer_id",
    "full_customer_name": "full_name",
    "contact_email": "email",
    "transaction_ref": "order_id",
    "item_reference": "product_id",
    "transaction_date": "order_timestamp",
    "amount_paid": "total_spent",
    "payment_status": "payment_status",
    "delivery_status": "delivery_status",
    "customer_segment": "segment",
    "region": "state",  # Assumes "region" maps to cleaned "state"
    "product_line": "category",
    "quantity_ordered": "quantity",
    "unit_cost": "unit_cost",
    "total_value": "order_total",
    "discount_applied": "discount",
    "shipping_fee": "shipping_cost",
    "tax_amount": "tax",
    "notes_comments": "notes",
    "last_modified_timestamp": "last_updated"
}

# Step 3: Apply the schema transformation
recon_cleaned = recon_raw.rename(columns=column_mapping)

# Step 4: Save the cleaned reconciliation data
output_path = './dataset/cleaned/reconciliation_cleaned.csv'
os.makedirs(os.path.dirname(output_path), exist_ok=True)
recon_cleaned.to_csv(output_path, index=False)

print("Reconciliation data has been successfully saved.")
print("File saved at:", os.path.abspath(output_path))

# Step 5: Compare with the cleaned customer dataset, if available
main_customers_path = './dataset/cleaned/customers_cleaned.csv'

if os.path.exists(main_customers_path):
    main_customers = pd.read_csv(main_customers_path)

    # Step 6: Identify new customer IDs present in reconciliation but not in the main dataset
    new_customers = recon_cleaned[~recon_cleaned['customer_id'].isin(main_customers['customer_id'])]

    print(f"\nNew customers found in reconciliation file: {len(new_customers)}")

    if not new_customers.empty:
        print("Sample of new customer IDs:")
        print(new_customers['customer_id'].drop_duplicates().head())
else:
    print("\nCleaned customer dataset not found for comparison.")
    print("Expected path:", main_customers_path)
