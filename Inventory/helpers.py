import re

from numpy import nan

from typing import Generator, Union
import pandas as pd

def get_OC_row_data(df_OC: pd.DataFrame) -> Generator[pd.Series, None, None]:
    print(df_OC.columns)
    for _, row in df_OC.iterrows():
        
        stock_available = row.get("In-Transit") + row.get("Available")
        sales_in_30_days = row.get("During the 30 days")
        
        data = {
            "Parent SKU": None,
            "SKU Name": row.get("SKU Name"),
            "Seller SKUID": row.get("Seller SKUID"),
            "OC/FBA": "OC",
            "Stock Available": stock_available,
            "Sales in Last 30 Days (During the 30 days)": sales_in_30_days,
            # Estimate number of months left if the same number of products are sold in the coming months (Equation by Zevi)
            "Months Of Stock": get_months_of_stock(stock_available, sales_in_30_days),
            "Parent Months of Stock": None
        }
        
        yield data
        

def get_new_OC_df(df_OC: pd.DataFrame):
    return pd.DataFrame([row for row in get_OC_row_data(df_OC)])


def get_FBA_row_data(df_FBA: pd.DataFrame) -> Generator[pd.Series, None, None]:
    
    # Fill all the nan cells with 0 to make calculations easier to handle
    # df_FBA["available"] = df_FBA["available"].fillna(0)
    # df_FBA["units-shipped-last-30-days"] = df_FBA["units-shipped-last-30-days"].fillna(0)
    for _, row in df_FBA.iterrows():
        
        stock_available = row.get("available")
        sales_in_30_days = row.get("units-shipped-last-30-days")
        
        data = {
            "Parent SKU": None,
            "SKU Name": row.get("product-name"),
            "Seller SKUID": row.get("sku"),
            "OC/FBA": "FBA",
            "Stock Available": stock_available,
            "Sales in Last 30 Days (During the 30 days)": sales_in_30_days,
            # Estimate number of months left if the same number of products are sold in the coming months (Equation by Zevi)
            "Months Of Stock": get_months_of_stock(stock_available, sales_in_30_days),
            "Parent Months of Stock": None
        }
        
        yield data
        
def get_new_FBA_df(df_FBA: pd.DataFrame):
    return pd.DataFrame([row for row in get_FBA_row_data(df_FBA)])

def get_months_of_stock(stock_available, sales_in_30_days):
    if sales_in_30_days != 0:
        return round((stock_available / sales_in_30_days), ndigits=1)
    elif sales_in_30_days == 0:
        return "No Sales"
    else:
        return 0
    
def skus_to_dict(skus_df: pd.DataFrame):
    prefixes = {}
    for _, row in skus_df.iterrows():
        prefixes[row.get("Full SKU")] = row.get("Parent SKU")
    return prefixes

def add_parent_skus(full_inventory_df: pd.DataFrame, df_prefixes: pd.DataFrame, other_skus: dict):
    for index, row in full_inventory_df.iterrows():
        seller_sku = row.get("Seller SKUID")
        if seller_sku in other_skus:
            full_inventory_df.loc[index, "Parent SKU"] = other_skus.get(seller_sku)
        else:
            sku_parent = re.search("^(\w)*", seller_sku)[0]
            full_inventory_df.loc[index, "Parent SKU"] = sku_parent
    
    return full_inventory_df

def calc_avg_months_of_sale_remaining(df: pd.DataFrame, avgs: dict):
    for index, row in df.iterrows():
        df.loc[index, "Parent Months of Stock"] = avgs[row.get("Parent SKU")]
    return df