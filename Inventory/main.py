import pandas as pd

from helpers import get_new_OC_df, get_new_FBA_df, skus_to_dict, add_parent_skus, calc_avg_months_of_sale_remaining

def main():

    FILES_URL = "./files/"
    USER_FILES_URL = FILES_URL + "user_files/"

    OC_FILENAME = USER_FILES_URL + "Export inventory status list0113154802.xlsx"
    # OC_FILENAME = FILES_URL + "Copy of Inventory 13_01_23 - OC.csv"
    FBA_FILENAME = USER_FILES_URL + "71719019370.csv"
    SKU_PREFIXES_FILENAME = FILES_URL + "Schema for PyScript - SKU Prefixes.csv"
    OTHER_SKUS_FILENAME = FILES_URL + "Schema for PyScript - Other SKU's.csv"
    COMBINE_SKUS_FILENAME = FILES_URL + "Schema for PyScript - Combine SKU's.csv"


    df_OC = pd.read_excel(OC_FILENAME)
    # df_OC = pd.read_csv(OC_FILENAME)
    df_OC.columns = df_OC.columns.str.strip()
    df_FBA = pd.read_csv(FBA_FILENAME)

    df_prefixes = pd.read_csv(SKU_PREFIXES_FILENAME)
    df_other_skus = pd.read_csv(OTHER_SKUS_FILENAME)

    other_skus = skus_to_dict(df_other_skus)

    df_combined = pd.DataFrame(
        columns=[
            "Parent SKU", 
            "SKU Name", 
            "Seller SKUID", 
            "OC/FBA",	
            "Stock Available", 
            "Sales in Last 30 Days (During the 30 days)", 
            "Months Of Stock",	
            "Parent Months of Stock"
            ])


    df_combined = pd.concat([df_combined, get_new_FBA_df(df_FBA), get_new_OC_df(df_OC)], ignore_index=True)

    df_combined = add_parent_skus(df_combined, df_prefixes, other_skus)
    avg_df = df_combined.groupby(["Parent SKU"]).sum(numeric_only=False)
    avg_dict ={}
    for sku, row in avg_df.iterrows():
        try:
            avg_dict[sku] = round(row.get("Stock Available") / row.get("Sales in Last 30 Days (During the 30 days)"), ndigits=2)
        except ZeroDivisionError:
            avg_dict[sku] = "No Sales"
            
    df_combined = calc_avg_months_of_sale_remaining(df_combined, avg_dict)
    
    print(df_combined)
   
    df_combined.to_excel("Inventory.xlsx", index=False)

if __name__ == "__main__":
    main()
