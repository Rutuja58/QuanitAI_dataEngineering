import pandas as pd
import numpy as np
import re
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)

def validate_email(email):
    if pd.isnull(email): return None
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return email if re.match(pattern, str(email)) else None

def unify_status(val):
    if pd.isnull(val): return None
    val = str(val).strip().lower()
    return {
        'active': 'active', 'yes': 'active', '1': 'active',
        'inactive': 'inactive', 'no': 'inactive', '0': 'inactive',
        'suspended': 'suspended', 'pending': 'pending',
        'false': 'inactive', 'true': 'active'
    }.get(val, 'unknown')

def clean_customers(df):
    df = df.copy()

    if 'cust_id' in df.columns and not df['cust_id'].isnull().all():
        df['customer_id'] = df['customer_id'].astype(str)
        df['cust_id'] = df['cust_id'].astype(str)
        df.loc[:, 'customer_id'] = df['customer_id'].combine_first(df['cust_id'])

    if 'customer_name' in df.columns and not df['customer_name'].isnull().all():
        df['full_name'] = df['full_name'].combine_first(df['customer_name'])

    # Ensure email field exists before combine_first
    if 'email' not in df.columns:
        df['email'] = pd.NA
    if 'email_address' in df.columns and not df['email_address'].isnull().all():
        df['email'] = df['email'].combine_first(df['email_address'])
    df['email'] = df['email'].apply(validate_email)

    if 'phone_number' in df.columns and not df['phone_number'].isnull().all():
        df['phone'] = df['phone'].combine_first(df['phone_number'])

    if 'postal_code' in df.columns and not df['postal_code'].isnull().all():
        df['zip_code'] = df['zip_code'].astype(str)
        df['postal_code'] = df['postal_code'].astype(str)
        df.loc[:, 'zip_code'] = df['zip_code'].combine_first(df['postal_code'])

    if 'addr' in df.columns and not df['addr'].isnull().all():
        df['address'] = df['address'].combine_first(df['addr'])

    if 'province' in df.columns and not df['province'].isnull().all():
        df['state'] = df['state'].combine_first(df['province'])

    # Safe handling of registration_date and reg_date
    if 'registration_date' in df.columns:
        reg1 = pd.to_datetime(df['registration_date'], errors='coerce')
    else:
        reg1 = pd.Series(pd.NaT, index=df.index)

    if 'reg_date' in df.columns:
       reg2 = pd.to_datetime(df['reg_date'], errors='coerce')
    else:
       reg2 = pd.Series(pd.NaT, index=df.index)
  
    df['registered_on'] = reg1.combine_first(reg2)

    if 'customer_status' in df.columns and not df['customer_status'].isnull().all():
        df['status'] = df['status'].combine_first(df['customer_status'])
    df['final_status'] = df['status'].apply(unify_status)

    df['gender'] = df['gender'].str.lower().str.strip()

    for col in ['total_spent', 'total_orders', 'loyalty_points', 'age']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'preferred_payment' in df.columns and not df['preferred_payment'].dropna().empty:
        df['preferred_payment'] = df['preferred_payment'].fillna(df['preferred_payment'].mode().iloc[0])
    if 'zip_code' in df.columns and not df['zip_code'].dropna().empty:
        df['zip_code'] = df['zip_code'].fillna(df['zip_code'].mode().iloc[0])

    df = df[~(df['email'].isnull() & df['phone'].isnull())]
    df = df.drop_duplicates(subset=['customer_id'])
    df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce')

    df.replace({'': pd.NA, 'none': pd.NA, 'null': pd.NA, 'NaN': pd.NA}, inplace=True)
    optional_fills = {
        'segment': 'general',
        'preferred_payment': 'unknown',
        'address': 'not provided'
    }
    for col, val in optional_fills.items():
        if col in df.columns:
            df[col] = df[col].fillna(val)

    essential_cols = ['customer_id', 'full_name', 'email']
    df = df.dropna(subset=[col for col in essential_cols if col in df.columns])

    keep_cols = [
        'customer_id', 'full_name', 'email', 'phone', 'address',
        'city', 'state', 'zip_code', 'total_orders', 'total_spent',
        'loyalty_points', 'preferred_payment', 'age', 'birth_date',
        'gender', 'segment', 'registered_on', 'final_status'
    ]
    return df[keep_cols]


def clean_products(df):
    df = df.copy()

    df['product_id'] = df['product_id'].astype(str)
    df['product_name'] = df['product_name'].astype(str)

    for col in ['price', 'list_price', 'cost', 'weight', 'rating']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df['rating'] = df['rating'].fillna(df['rating'].median())
    df['weight'] = df['weight'].fillna(df['weight'].median())

    df['is_active'] = df['is_active'].astype(str).str.lower().map({
        'true': True, '1': True, 'yes': True,
        'false': False, '0': False, 'no': False
    })

    df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce')
    df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce')
    df['last_updated'] = df['last_updated'].fillna(df['created_date'])

    df = df[df['created_date'].notna()]

    for col in ['brand', 'manufacturer', 'category', 'product_category', 'color', 'size']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().str.strip()

    df['brand'] = df['brand'].combine_first(df['manufacturer'])
    df['product_category'] = df['product_category'].combine_first(df['category'])

    if 'supplier_id' in df.columns:
        df['supplier_id'] = df['supplier_id'].astype(str).fillna('unknown')
    else:
        df['supplier_id'] = 'unknown'

    df = df.drop_duplicates(subset=['product_id'])

    df.replace({'': pd.NA, 'none': pd.NA, 'null': pd.NA, 'NaN': pd.NA}, inplace=True)
    optional_fills = {
        'description': 'not provided',
        'color': 'unspecified',
        'size': 'unspecified',
        'dimensions': 'not specified',
        'brand': 'unknown',
        'product_category': 'misc',
        'supplier_id': 'unknown'
    }
    for col, val in optional_fills.items():
        if col in df.columns:
            df[col] = df[col].fillna(val)

    essential_cols = ['product_id', 'product_name', 'price', 'brand']
    df = df.dropna(subset=essential_cols)

    keep = [
        'product_id', 'product_name', 'description', 'product_category', 'brand',
        'price', 'list_price', 'cost', 'weight', 'dimensions', 'color', 'size',
        'stock_quantity', 'stock_level', 'reorder_level', 'supplier_id',
        'created_date', 'last_updated', 'is_active', 'rating'
    ]
    return df[keep]



def clean_orders(df):
    df = df.copy()

    df['order_id'] = df['order_id'].astype(str)
    df['customer_id'] = df['customer_id'].astype(str)
    if 'cust_id' in df.columns:
        df['cust_id'] = df['cust_id'].astype(str)
        df['customer_id'] = df['customer_id'].combine_first(df['cust_id'])
    df['product_id'] = df['product_id'].astype(str)

    df.drop(columns=['ord_id', 'cust_id'], errors='ignore', inplace=True)

    df['order_date'] = pd.to_datetime(df.get('order_date'), errors='coerce')
    df['order_datetime'] = pd.to_datetime(df.get('order_datetime'), errors='coerce')
    df['order_timestamp'] = df['order_date'].combine_first(df['order_datetime'])

    df['quantity'] = pd.to_numeric(df['quantity'].combine_first(df.get('qty')), errors='coerce')
    df.drop(columns=['qty'], errors='ignore', inplace=True)

    for col in ['unit_price', 'price', 'total_amount', 'order_total', 'shipping_cost', 'tax', 'discount']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'order_value' not in df.columns or df['order_value'].isnull().all():
        df['order_value'] = df['quantity'] * df['unit_price']
    else:
        df['order_value'] = pd.to_numeric(df['order_value'], errors='coerce')

    df['status'] = df['status'].combine_first(df.get('order_status'))
    df['status'] = df['status'].str.lower().str.strip()
    df.drop(columns=['order_status'], errors='ignore', inplace=True)

    df['tracking_number'] = df.get('tracking_number', 'NOT_AVAILABLE')
    df['tracking_number'] = df['tracking_number'].fillna('NOT_AVAILABLE')
    df['payment_method'] = df['payment_method'].str.lower().str.strip()
    df['shipping_address'] = df['shipping_address'].astype(str).str.strip()
    df['notes'] = df['notes'].fillna('').astype(str)

    df.replace({'': pd.NA, 'none': pd.NA, 'null': pd.NA, 'NaN': pd.NA}, inplace=True)

    optional_fills = {
        'shipping_address': 'not provided',
        'notes': '',
        'tracking_number': 'not available'
    }
    for col, val in optional_fills.items():
        if col in df.columns:
            df[col] = df[col].fillna(val)

    essential_cols = ['order_id', 'customer_id', 'product_id', 'quantity', 'unit_price']
    df = df.dropna(subset=essential_cols)

    keep_columns = [
        'order_id', 'customer_id', 'product_id', 'quantity', 'unit_price',
        'total_amount', 'order_total', 'shipping_cost', 'tax', 'discount',
        'status', 'payment_method', 'shipping_address', 'notes',
        'tracking_number', 'order_value', 'order_timestamp'
    ]
    return df[keep_columns]

