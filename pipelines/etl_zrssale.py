import os
import re
import shutil
import pandas as pd
from math import ceil
from pathlib import Path
from collections import defaultdict
from ETL_SAP.common.loader import upload_to_sql, upsert_batch
from ETL_SAP.common.config import get_sql_engine
from datetime import datetime
from sqlalchemy.types import VARCHAR, NVARCHAR, DECIMAL, INTEGER, Date, DateTime
from ETL_SAP.sap_scripts.downloader_zrssale import download_zrssale
from dotenv import load_dotenv
from ETL_SAP.pipelines.etl_utils import *

load_dotenv()


def run_etl_zrssale_D2(folder_path):

    BATCH_SIZE = 1
    txt_files = sorted(Path(folder_path).glob("ZRSSALE_D2*.txt"))
    # txt_files = sorted(Path(folder_path).glob("zrssale_D2.txt"))
    n_batches = ceil(len(txt_files) / BATCH_SIZE)
    processed_dir = Path(folder_path, "processed")
    processed_dir.mkdir(exist_ok=True)

    print(f"Total {len(txt_files)} files, {n_batches} batches.")

    for b in range(n_batches):
        batch_files = txt_files[b*BATCH_SIZE : (b+1)*BATCH_SIZE]
        if not batch_files:
            break

        print(f"🚚 處理批次 {b+1}/{n_batches}，檔案數 {len(batch_files)} …")

        # -------- 讀 + 清洗 --------

        print("🔹 開始清理 ZRSSALE_D2 檔案...")
        
        dfs = []
        for fp in batch_files:
            df = (
                pd.read_csv(fp, sep="\t", skiprows=2, dtype=str, low_memory=False,)
                    .iloc[:, 1:]                            # 去掉空白首欄
            )

            # df = (
            #     pd.read_csv(fp, sep="\t", dtype=str, low_memory=False,)
            # )

            df.columns = df.columns.str.strip().str.replace(r"\s+", " ", regex=True)
            df['Bill. Date'] = pd.to_datetime(df['Bill. Date'], format='%m/%d/%Y')
            df.dropna(subset=['Article'], inplace=True)
            df = df.rename(columns={
                "Descript.": "Art.type descr.",
            })
            print(df.columns.tolist())
            print(df.head(3))
            dfs.append(df)

        batch_df = pd.concat(dfs, ignore_index=True)
        batch_df = batch_df.drop_duplicates(subset=['Article'])
        batch_df['Article'] = batch_df['Article'].astype(str).str.strip()

        for col in batch_df.columns:
            if batch_df[col].dtype == object:
                max_len = batch_df[col].astype(str).map(len).max()
                print(f"{col}: {max_len}")


        # 欄位正名
        batch_df = batch_df.rename(columns={
            "SOrg.": "SOrg",
            "Sold-to": "Sold_To",
            "Ship-to": "Ship_To",
            "Name 1":  "Name",
            "Bill.Doc.": "Bill_Doc",
            "Bill. Date": "Date",
            "Mdse Cat.":  "MCH",
            "Bill.qty":  "Quantity_SUn",
            "SU":  "SUn",
            "BillQtySKU":  "Quantity",
            "Sales Amou":  "Amt",
            "Curr.":  "Curr",
            "SAP Tax":  "SAP_Tax",
            "Sales Doc.":  "Sales_Doc",
            "Ship-to st":  "Ship_To_State",
            "Ship-to Ci":  "Ship_To_City",
            "TaxRate %":  "Tax_Rate",
            'Net Value': 'Net',
            "Reg":  "DlvState",
            "Search Ter":  "Search_Ter",
            "Postal Cod":  "Postal_Code",
            "N Weight":  "N_Weight",
            "Inco. 2":  "Inco_2",
            "MTyp":  "Article_Type",
            "Art.type descr.":  "Article_Type_Description",
            "POS Tax":  "POS_Tax",
            "Net Sale":  "Net_Sale",
        })

        # 數字清洗
        batch_df[["Quantity_SUn", "Quantity", "Amt", "Cost", "SAP_Tax", "ArtTax", "Tax_Rate", 
                  "CRVRate", "Net", "N_Weight", "Discount", "WSale", "POS_Tax", "Net_Sale"]] = batch_df[["Quantity_SUn", "Quantity", "Amt", "Cost", "SAP_Tax", "ArtTax", "Tax_Rate", 
                  "CRVRate", "Net", "N_Weight", "Discount", "WSale", "POS_Tax", "Net_Sale"]].apply(fast_numeric)
        batch_df = batch_df[batch_df['Article_Type'] == 'ZTTG'] 

        # groupby_df = batch_df.groupby(['Article', 'Date']).agg({
        #     "SOrg": 'first',
        #     "Sold_To": 'first',
        #     "Ship_To": 'first',
        #     "Payer": 'first',
        #     "Name": 'first',
        #     "Bill_Doc": 'first',
        #     "Item": 'first',
        #     "Description": 'first',
        #     "MCH": 'first',
        #     "Quantity_SUn": 'sum',
        #     "SUn": 'first',
        #     "Quantity": 'sum',
        #     "Amt": 'sum',
        #     "Curr": 'first',
        #     "SAP_Tax": 'first',
        #     "Cost": 'sum',
        #     "AAGM": 'first',
        #     "Sales_Doc": 'first',
        #     "SOType": 'first',
        #     "ArtTax": 'first',
        #     "ArtCRV": 'first',
        #     "CRVDesc": 'first',
        #     "Site": 'first',
        #     "Ship_To_State": 'first',
        #     "Ship_To_City": 'first',
        #     "DChl": 'first',
        #     "ItCa": 'first',
        #     "PsSt": 'first',
        #     "Tax_Rate": 'first',
        #     "CRVRate": 'first',
        #     "Net": 'sum',
        #     "DlvState": 'first',
        #     "Search_Ter": 'first',
        #     "Postal_Code": 'first',
        #     "N_Weight": 'first',
        #     "IncoT": 'first',
        #     "Inco_2": 'first',
        #     "Article_Type": 'first',
        #     "Article_Type_Description": 'first',
        #     "Discount": 'sum',
        #     "WSale": 'sum',
        #     "Customer": 'first',
        #     "POS_Tax": 'sum',
        #     "Net_Sale": 'sum',
        #     "Tx": 'first'

        # }).reset_index()    

        print(f"🚚 批次 {b+1} 清洗後資料：\n{batch_df.head(2)}\n"
            f"🚚 批次 {b+1} 清洗後資料筆數：{len(batch_df)}\n")

        # 上傳至 SQL Server
        print(f"🔹 開始上傳 {os.getenv("TABLE_ZRSSALE_D2")} 資料到 {os.getenv("SQL_DB")}...")
        
        engine = get_sql_engine()

        column_types = {
            "SOrg": NVARCHAR(10),
            "Sold_To": NVARCHAR(20),
            "Ship_To": NVARCHAR(20),
            "Payer": NVARCHAR(20),
            "Name": NVARCHAR(100),
            "Bill_Doc": NVARCHAR(20),
            "Date": Date(),
            "Item": NVARCHAR(10),
            "Article": NVARCHAR(20),
            "Description": NVARCHAR(100),
            "MCH": NVARCHAR(20),
            "Quantity_SUn": DECIMAL(18, 6),
            "SUn": NVARCHAR(10),
            "Quantity": DECIMAL(18, 6),
            "Amt": DECIMAL(18, 6),
            "Curr": NVARCHAR(10),
            "SAP_Tax": DECIMAL(18, 6),
            "Cost": DECIMAL(18, 6),
            "AAGM": NVARCHAR(10),
            "Sales_Doc": NVARCHAR(20),
            "SOType": NVARCHAR(10),
            "ArtTax": DECIMAL(18, 6),
            "ArtCRV": NVARCHAR(10),
            "CRVDesc": NVARCHAR(50),
            "Site": NVARCHAR(20),
            "Ship_To_State": NVARCHAR(3),
            "Ship_To_City": NVARCHAR(50),
            "DChl": NVARCHAR(10),
            "ItCa": NVARCHAR(10),
            "PsSt": NVARCHAR(10),
            "Tax_Rate": DECIMAL(18, 6),
            "CRVRate": DECIMAL(18, 6),
            "Net": DECIMAL(18, 6),
            "DlvState": NVARCHAR(10),
            "Search_Ter": NVARCHAR(20),
            "Postal_Code": NVARCHAR(10),
            "N_Weight": DECIMAL(18, 6),
            "IncoT": NVARCHAR(20),
            "Inco_2": NVARCHAR(20),
            "Article_Type": NVARCHAR(50),
            "Article_Type_Description": NVARCHAR(100),
            "Discount": DECIMAL(18, 6),
            "WSale": DECIMAL(18, 6),
            "Customer": NVARCHAR(20),
            "POS_Tax": DECIMAL(18, 6),
            "Net_Sale":DECIMAL(18, 6),
            "Tx": NVARCHAR(10)
        }

        upsert_batch(
            df=batch_df,
            target_table=os.getenv("TABLE_ZRSSALE_D2"),
            unique_keys=["Bill_Doc", "Item"],
            column_types=column_types,
        )
        
        print(f"✅ 批次 {b+1} 已匯入 {os.getenv("TABLE_ZRSSALE_D2")} {len(batch_df):,} 列\n")

        # ---------- 移動到 processed ----------
        for fp in batch_files:
            dest = processed_dir / fp.name
            # 若同名檔已存在就加時間戳避免覆寫
            if dest.exists():
                timestamp = datetime.now().strftime("%Y%m%d%H%M")
                dest = processed_dir / f"{fp.stem}_{timestamp}{fp.suffix}"
            shutil.move(fp, dest)
        print(f"批次 {b+1} 檔案已移至 {processed_dir}\n")

    print("🎉 D2 批次處理結束")


def run_etl_zrssale_D3(folder_path):

    BATCH_SIZE = 1
    txt_files = sorted(Path(folder_path).glob("ZRSSALE_D3*.txt"))
    # txt_files = sorted(Path(folder_path).glob("zrssale_D3.txt"))
    n_batches = ceil(len(txt_files) / BATCH_SIZE)
    processed_dir = Path(folder_path, "processed")
    processed_dir.mkdir(exist_ok=True)

    print(f"Total {len(txt_files)} files, {n_batches} batches.")

    for b in range(n_batches):
        batch_files = txt_files[b*BATCH_SIZE : (b+1)*BATCH_SIZE]
        if not batch_files:
            break

        print(f"🚚 處理批次 {b+1}/{n_batches}，檔案數 {len(batch_files)} …")

        # -------- 讀 + 清洗 --------

        print("🔹 開始清理 ZRSSALE_D3 檔案...")
        
        dfs = []
        for fp in batch_files:
            df = (
                pd.read_csv(fp, sep="\t", skiprows=2, dtype=str, low_memory=False,)
                    .iloc[:, 1:]                            # 去掉空白首欄
            )

            # df = (
            #     pd.read_csv(fp, sep="\t", dtype=str, low_memory=False,)
            # )

            df.columns = df.columns.str.strip().str.replace(r"\s+", " ", regex=True)
            df['Bill. Date'] = pd.to_datetime(df['Bill. Date'], format='%m/%d/%Y')
            df.dropna(subset=['Article'], inplace=True)
            df = df.rename(columns={
                "Descript.": "Art.type descr.",
                "Ship-to City": "Ship-to Ci",
            })
            print(df.columns.tolist())
            print(df.head(3))
            dfs.append(df)

        batch_df = pd.concat(dfs, ignore_index=True)

        # 欄位正名
        batch_df = batch_df.rename(columns={
            "SOrg.": "SOrg",
            "Sold-to": "Sold_To",
            "Ship-to": "Ship_To",
            "Name 1":  "Name",
            "Bill.Doc.": "Bill_Doc",
            "Bill. Date": "Date",
            "Mdse Cat.":  "MCH",
            "Bill.qty":  "Quantity_SUn",
            "SU":  "SUn",
            "BillQtySKU":  "Quantity",
            "Sales Amou":  "Amt",
            "Curr.":  "Curr",
            "SAP Tax":  "SAP_Tax",
            "Sales Doc.":  "Sales_Doc",
            "Ship-to st":  "Ship_To_State",
            "Ship-to Ci":  "Ship_To_City",
            "TaxRate %":  "Tax_Rate",
            "Reg":  "DlvState",
            "Search Ter":  "Search_Ter",
            "Postal Cod":  "Postal_Code",
            "N Weight":  "N_Weight",
            "Inco. 2":  "Inco_2",
            "MTyp":  "Article_Type",
            "Art.type descr.":  "Article_Type_Description",
            "POS Tax":  "POS_Tax",
            "Net Sale":  "Net_Sale",
        })

        # 數字清洗
        batch_df[["Quantity_SUn", "Quantity", "Amt", "Cost", "SAP_Tax", "ArtTax", "Tax_Rate", 
                  "CRVRate", "Net", "N_Weight", "Discount", "WSale", "POS_Tax", "Net_Sale"]] = batch_df[["Quantity_SUn", "Quantity", "Amt", "Cost", "SAP_Tax", "ArtTax", "Tax_Rate", 
                  "CRVRate", "Net", "N_Weight", "Discount", "WSale", "POS_Tax", "Net_Sale"]].apply(fast_numeric)
        batch_df = batch_df[batch_df['Article_Type'] == 'ZTTG'] 

        # groupby_df = batch_df.groupby(['Article', 'Date']).agg({
        #     "SOrg": 'first',
        #     "Sold_To": 'first',
        #     "Ship_To": 'first',
        #     "Payer": 'first',
        #     "Name": 'first',
        #     "Bill_Doc": 'first',
        #     "Item": 'first',
        #     "Description": 'first',
        #     "MCH": 'first',
        #     "Quantity_SUn": 'sum',
        #     "SUn": 'first',
        #     "Quantity": 'sum',
        #     "Amt": 'sum',
        #     "Curr": 'first',
        #     "SAP_Tax": 'first',
        #     "Cost": 'sum',
        #     "AAGM": 'first',
        #     "Sales_Doc": 'first',
        #     "SOType": 'first',
        #     "ArtTax": 'first',
        #     "ArtCRV": 'first',
        #     "CRVDesc": 'first',
        #     "Site": 'first',
        #     "Ship_To_State": 'first',
        #     "Ship_To_City": 'first',
        #     "DChl": 'first',
        #     "ItCa": 'first',
        #     "PsSt": 'first',
        #     "Tax_Rate": 'first',
        #     "CRVRate": 'first',
        #     "Net": 'sum',
        #     "DlvState": 'first',
        #     "Search_Ter": 'first',
        #     "Postal_Code": 'first',
        #     "N_Weight": 'first',
        #     "IncoT": 'first',
        #     "Inco_2": 'first',
        #     "Article_Type": 'first',
        #     "Article_Type_Description": 'first',
        #     "Discount": 'sum',
        #     "WSale": 'sum',
        #     "Customer": 'first',
        #     "POS_Tax": 'sum',
        #     "Net_Sale": 'sum',
        #     "Tx": 'first'

        # }).reset_index()    

        print(f"🚚 批次 {b+1} 清洗後資料：\n{batch_df.head(2)}\n"
            f"🚚 批次 {b+1} 清洗後資料筆數：{len(batch_df)}\n")

        # 上傳至 SQL Server
        print(f"🔹 開始上傳 {os.getenv("TABLE_ZRSSALE_D3")} 資料到 {os.getenv("SQL_DB")}...")
        
        engine = get_sql_engine()

        column_types = {
            "SOrg": NVARCHAR(10),
            "Sold_To": NVARCHAR(20),
            "Ship_To": NVARCHAR(20),
            "Payer": NVARCHAR(20),
            "Name": NVARCHAR(100),
            "Bill_Doc": NVARCHAR(20),
            "Date": Date(),
            "Item": NVARCHAR(10),
            "Article": NVARCHAR(20),
            "Description": NVARCHAR(100),
            "MCH": NVARCHAR(20),
            "Quantity_SUn": DECIMAL(18, 6),
            "SUn": NVARCHAR(10),
            "Quantity": DECIMAL(18, 6),
            "Amt": DECIMAL(18, 6),
            "Curr": NVARCHAR(10),
            "SAP_Tax": DECIMAL(18, 6),
            "Cost": DECIMAL(18, 6),
            "AAGM": NVARCHAR(10),
            "Sales_Doc": NVARCHAR(20),
            "SOType": NVARCHAR(10),
            "ArtTax": DECIMAL(18, 6),
            "ArtCRV": NVARCHAR(10),
            "CRVDesc": NVARCHAR(50),
            "Site": NVARCHAR(20),
            "Ship_To_State": NVARCHAR(3),
            "Ship_To_City": NVARCHAR(50),
            "DChl": NVARCHAR(10),
            "ItCa": NVARCHAR(10),
            "PsSt": NVARCHAR(10),
            "Tax_Rate": DECIMAL(18, 6),
            "CRVRate": DECIMAL(18, 6),
            "Net": DECIMAL(18, 6),
            "DlvState": NVARCHAR(10),
            "Search_Ter": NVARCHAR(20),
            "Postal_Code": NVARCHAR(10),
            "N_Weight": DECIMAL(18, 6),
            "IncoT": NVARCHAR(10),
            "Inco_2": NVARCHAR(10),
            "Article_Type": NVARCHAR(50),
            "Article_Type_Description": NVARCHAR(100),
            "Discount": DECIMAL(18, 6),
            "WSale": DECIMAL(18, 6),
            "Customer": NVARCHAR(20),
            "POS_Tax": DECIMAL(18, 6),
            "Net_Sale":DECIMAL(18, 6),
            "Tx": NVARCHAR(10)
        }

        upsert_batch(
            df=batch_df,
            target_table=os.getenv("TABLE_ZRSSALE_D3"),
            unique_keys=["Bill_Doc", "Item"],
            column_types=column_types,
        )

        print(f"✅ 批次 {b+1} 已匯入 {os.getenv("TABLE_ZRSSALE_D3")} {len(batch_df):,} 列\n")

        # ---------- 移動到 processed ----------
        for fp in batch_files:
            dest = processed_dir / fp.name
            # 若同名檔已存在就加時間戳避免覆寫
            if dest.exists():
                timestamp = datetime.now().strftime("%Y%m%d%H%M")
                dest = processed_dir / f"{fp.stem}_{timestamp}{fp.suffix}"
            shutil.move(fp, dest)
        print(f"批次 {b+1} 檔案已移至 {processed_dir}\n")

    print("🎉 D3 批次處理結束")


if __name__ == "__main__":

    download_zrssale(os.getenv("DATE_FILE_ZRSSALE"), os.getenv("EXPORT_DIR_ZRSSALE"))
    run_etl_zrssale_D2(os.getenv("EXPORT_DIR_ZRSSALE"))
    run_etl_zrssale_D3(os.getenv("EXPORT_DIR_ZRSSALE"))
    # run_etl_zrssale(r"C:\Users\anniec\Documents\TAWA\AutoScript\ETL_SAP\data")



