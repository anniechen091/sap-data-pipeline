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
from ETL_SAP.sap_scripts.downloader_zstpromo import download_zstpromo
from dotenv import load_dotenv
from ETL_SAP.pipelines.etl_utils import *
from ETL_SAP.pipelines.etl_zmb51 import run_etl_zmb51

load_dotenv()

def run_etl_zstpromo(folder_path):
    BATCH_SIZE = 1
    txt_files = sorted(Path(folder_path).glob("ZSTPROMO_*.txt"))
    n_batches = ceil(len(txt_files) / BATCH_SIZE)
    processed_dir = Path(folder_path, "processed")
    processed_dir.mkdir(exist_ok=True)

    print(f"Total {len(txt_files)} files, {n_batches} batches.")

    for b in range(n_batches):
        batch_files = txt_files[b * BATCH_SIZE : (b + 1) * BATCH_SIZE]
        if not batch_files:
            break

        print(f"🚚 處理批次 {b+1}/{n_batches}，檔案數 {len(batch_files)} …")

        dfs = []
        for fp in batch_files:
            df = pd.read_csv(fp, sep="\t", skiprows=2, dtype=str, low_memory=False).iloc[:, 1:]  
            df.columns = df.columns.str.strip()
            df['Bill. Date'] = pd.to_datetime(df['Bill. Date'], format='%m/%d/%Y')
            df.dropna(subset=['Article', 'Payer', 'Bill. Date'], inplace=True)
            dfs.append(df)

        batch_df = pd.concat(dfs, ignore_index=True)

        # 欄位正名
        batch_df = batch_df.rename(columns={
            "Payer": "Site",
            "Bill.qty":  "Quantity",
            "Bill. Date": "Date",
            "Sales Amou": "Amt",
            "SU": "SUn",
        })

        # 數字清洗
        batch_df[["Quantity", "Amt", "Cost"]] = batch_df[["Quantity", "Amt", "Cost"]].apply(fast_numeric)

        groupby_df = batch_df.groupby(['Article', 'Site', 'Date']).agg({
            "Amt": 'sum',
            'Quantity': 'sum',
            'Cost': 'sum',
            'SUn' : 'first'
            }).reset_index()    

        print(f"🚚 批次 {b+1} 清洗後資料：\n{groupby_df.head(2)}\n"
            f"🚚 批次 {b+1} 清洗後資料筆數：{len(groupby_df)}\n")
        

        # 上傳至 SQL Server
        print(f"🔹 開始上傳 ZSTPROMO 資料到 {os.getenv('SQL_DB')}...")

        engine = get_sql_engine()
        column_types = {
            "Article": NVARCHAR(20),
            "Site":    NVARCHAR(10),
            "Date":    Date(),
            "Amt":     DECIMAL(18,6),
            "Quantity":DECIMAL(18,6),
            "Cost":    DECIMAL(18,6),
            "SUn":     NVARCHAR(10),
        }

        upsert_batch(
            df=groupby_df,
            target_table=os.getenv("TABLE_ZSTPROMO"),
            unique_keys=["Article", "Site", "Date"],
            column_types=column_types
         )
        
        # upload_to_sql(groupby_df, os.getenv("TABLE_ZSTPROMO"), column_types, if_exists="append")
        print(f"✅ 批次 {b+1} 已匯入 {os.getenv("TABLE_ZSTPROMO")} {len(groupby_df):,} 列\n")

        # ---------- 移動到 processed ----------
        for fp in batch_files:
            dest = processed_dir / fp.name
            # 若同名檔已存在就加時間戳避免覆寫
            if dest.exists():
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                dest = processed_dir / f"{fp.stem}_{timestamp}{fp.suffix}"
            shutil.move(fp, dest)
        print(f"批次 {b+1} 檔案已移至 {processed_dir}\n")

    print("🎉 全部批次處理結束")


if __name__ == "__main__":

    download_zstpromo(os.getenv("DATE_FILE_ZSTPROMO"), os.getenv("EXPORT_DIR_ZSTPROMO"))
    run_etl_zstpromo(os.getenv("EXPORT_DIR_ZSTPROMO")) 

    # run_etl_zmb51(os.getenv("EXPORT_DIR_ZMB51"))
    



