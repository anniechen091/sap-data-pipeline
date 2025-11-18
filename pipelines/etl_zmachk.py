import os
import re
import shutil
import pandas as pd
from math import ceil
from pathlib import Path
from sqlalchemy import text
from collections import defaultdict
from ETL_SAP.common.loader import upload_to_sql, upsert_batch
from ETL_SAP.common.config import get_sql_engine
from datetime import datetime
from sqlalchemy.types import VARCHAR, NVARCHAR, DECIMAL, INTEGER, Date, DateTime
from ETL_SAP.sap_scripts.downloader_zmachk import download_zmachk
from dotenv import load_dotenv
from ETL_SAP.pipelines.etl_utils import *

load_dotenv()

def run_etl_zmachk(folder_path):

    processed_dir = Path(folder_path, "processed")
    processed_dir.mkdir(exist_ok=True)

    dfs = []
    files = sorted(Path(folder_path).glob("ZMACHK_*.xlsx"))
    if not files:
        print(f"❗ 沒有找到任何 ZMACHK 檔案在 {folder_path}")
        return
    for fp in files:
        print(f"Processing file: {fp}")
        df = pd.read_excel(fp, dtype=str)
        df = df[df['Status'] == 'Y']
        df.drop(columns=['Status'], inplace=True)
        dfs.append(df)

    batch_df = pd.concat(dfs, ignore_index=True)
    print("Length:", len(batch_df), " \nContent: \n", batch_df)

    batch_df.rename(columns={
            "Article Description": "Article_Description",
            "Chinese Desc.":  "Chinese_Desc",
            "Merchandise Category": "MCH",
            "Valid-From Date": "Valid_From_Date",
            "Size/dimensions": "Size_dimensions",
            "BUn Conv.": "BUn_Conv",
            "D/I": "DI",
            "D/I Conv.": "DI_Conv",
            "SUn Conv.": "SUn_Conv",
            "Oun to Bun Conv": "Oun_to_Bun_Conv",
            "FI Wtunit": "FI_Wtunit",
            "FIWtconv.": "FIWt_Conv",
            "Brand Name": "Brand_Name",
            "Country of origin of the article": "Origin_Country",
            "Minimum Remaining Shelf Life": "Min_Remaining_Shelf_Life",
            "Total shelf life": "Total_Shelf_Life",
            "Source of Supply": "Source_of_Supply",
            "Product Type": "Product_Type",
            "DOH Target": "DOH_Target",
            "Lead Time": "Lead_Time",
            "Stock Plan Frequency": "Stock_Plan_Frequency",
            "Supplier Channel": "Supplier_Channel",
            "Item Status": "Item_Status",
            "Status WS E": "Status_WS_E",
            "Status WS W": "Status_WS_W",
            "Status SCA": "Status_SCA",
            "Status NCA": "Status_NCA",
            "Status TX": "Status_TX",
            "Status EC": "Status_EC",
            "Retail Channel": "Retail_Channel",
            "Status Online": "Status_Online",
            "WholeSale Channel": "WholeSale_Channel",
            "Wacine Ordering": "Wachine_Ordering",

        }, inplace=True)

    # 數字清洗
    batch_df['Valid_From_Date'] = pd.to_datetime(batch_df['Valid_From_Date']).dt.strftime('%Y-%m-%d')
    batch_df['DOH_Target'] = pd.to_numeric(batch_df['DOH_Target'].replace('-', None), errors='coerce')
    batch_df['Lead_Time'] = pd.to_numeric(batch_df['Lead_Time'].replace('-', None), errors='coerce')

    print(f"清洗後資料：\n{batch_df.head(2)}\n"
        f"清洗後資料筆數：{len(batch_df)}\n")
    
    # for col in batch_df.columns:
    #     if batch_df[col].dtype == 'object':
    #         max_len = batch_df[col].astype(str).map(len).max()
    #         print(f"{col}: 最大長度 = {max_len}")

    # numeric_cols = ['BUn_Conv', 'DI_Conv', 'SUn_Conv', 'Oun_to_Bun_Conv', 'FIWt_Conv', 'Min_Remaining_Shelf_Life', 'Total_Shelf_Life', 'DOH_Target', 'Lead_Time']

    # for col in numeric_cols:
    #     batch_df[col] = pd.to_numeric(batch_df[col], errors='coerce')  # 將無法轉成數字的變成 NaN
    #     invalid_rows = batch_df[batch_df[col].isna()]
    #     if not invalid_rows.empty:
    #         print(f"❗ 無效數字資料在欄位 {col}：")
    #         print(invalid_rows[[col, 'Article']].head())

    # for col in ['DOH_Target', 'Lead_Time']:
    #     # 找出那些不能轉為數字的原始值
    #     invalid_mask = pd.to_numeric(batch_df[col], errors='coerce').isna() & batch_df[col].notna()
    #     invalid_rows = batch_df.loc[invalid_mask, [col, 'Article']]
        
    #     if not invalid_rows.empty:
    #         print(f"❗ {col} 欄位中無法轉成數字的原始值：")
    #         print(invalid_rows.head(10))


    # 上傳 ZMACHK 至 SQL Server
    print(f"🔹 開始上傳 Article_MasterData 資料到 {os.getenv('SQL_DB')}...")

    engine = get_sql_engine()
    with engine.connect() as conn:
        # 確保目標資料表存在
        existing_articles = pd.read_sql(text("SELECT Article FROM dbo.dim_Article"), conn)
    existing_set = set(existing_articles['Article'])
    new_rows = batch_df[~batch_df['Article'].isin(existing_set)]

    column_types = {
        "Article": NVARCHAR(20),
        "Article_Description": NVARCHAR(255),
        "Chinese_Desc": NVARCHAR(255),
        "MCH": NVARCHAR(20),
        "Valid_From_Date": Date(),
        "Size_dimensions": NVARCHAR(50),
        "BUn": NVARCHAR(10),
        "BUn_Conv": DECIMAL(10, 4),
        "DI": NVARCHAR(10),
        "DI_Conv": DECIMAL(10, 4),
        "SUn": NVARCHAR(10),
        "SUn_Conv": DECIMAL(10, 4),
        "OUn": NVARCHAR(10),
        "Oun_to_Bun_Conv": DECIMAL(10, 4),
        "FI_Wtunit": NVARCHAR(10),
        "FIWt_Conv": DECIMAL(18, 6),
        "Brand_Name": NVARCHAR(100),
        "Origin_Country": NVARCHAR(50),
        "Min_Remaining_Shelf_Life": INTEGER(),
        "Total_Shelf_Life": INTEGER(),
        "Source_of_Supply": INTEGER(),
        "Assortment": NVARCHAR(30),
        "Ethnicity": NVARCHAR(20),
        "Product_Type": NVARCHAR(30),
        "DOH_Target": DECIMAL(10, 4),
        "Lead_Time": DECIMAL(10, 4),
        "Stock_Plan_Frequency": NVARCHAR(50),
        "Supplier_Channel": NVARCHAR(50),
        "Seasonal": NVARCHAR(20),
        "Item_Status": NVARCHAR(5),
        "Status_WS_E": NVARCHAR(5),
        "Status_WS_W": NVARCHAR(5),
        "Status_SCA": NVARCHAR(5),
        "Status_NCA": NVARCHAR(5),
        "Status_TX": NVARCHAR(5),
        "Status_EC": NVARCHAR(5),
        "Retail_Channel": NVARCHAR(5),
        "Status_Online": NVARCHAR(5),
        "WholeSale_Channel": NVARCHAR(5),
        "Wachine_Ordering": NVARCHAR(5),
    }

    upsert_batch(
        df=batch_df,
        target_table=os.getenv("TABLE_Article_MasterData"),
        unique_keys=["Article"],
        column_types=column_types
        )
    
    batch_df.to_csv(r"C:\Users\anniec\Documents\TAWA\AutoScript\DC Forecast - Seasonality\TawaWalong\ZMACHK_ALL.csv", index=False, encoding='utf-8-sig')
    batch_df.to_excel(r"C:\Users\anniec\Documents\TAWA\AutoScript\DC Forecast - Seasonality\TawaWalong\ZMACHK_ALL.xlsx", index=False)
    print(f"✅  已匯入 {os.getenv("TABLE_Article_MasterData")} {len(batch_df):,} 列\n")

    output_path = Path(folder_path) / "new_articles" / f"New_Article_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    new_rows.to_excel(output_path, index=False)
    print(f"已將新增的 {len(new_rows)} 筆資料匯出到 {output_path}")


    # ---------- 移動到 processed ----------
    txt_files = sorted(Path(folder_path).glob("ZMACHK_*.xlsx"))
    for fp in txt_files:
        dest = processed_dir / fp.name
        # 若同名檔已存在就加時間戳避免覆寫
        if dest.exists():
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            dest = processed_dir / f"{fp.stem}_{timestamp}{fp.suffix}"
        shutil.move(fp, dest)
        print(f"✅ 已移動檔案：{fp} 到 {dest}")
    print(f"ZMACHK 檔案已移至 {processed_dir}\n")

    print("🎉 全部批次處理結束")



if __name__ == "__main__":

    # download_zmachk(os.getenv("EXPORT_DIR_ZMACHK"))
    run_etl_zmachk(os.getenv("EXPORT_DIR_ZMACHK")) 
