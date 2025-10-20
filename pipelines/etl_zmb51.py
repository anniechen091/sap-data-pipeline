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
from ETL_SAP.sap_scripts.downloader_zmb51 import download_zmb51
from dotenv import load_dotenv
from ETL_SAP.pipelines.etl_utils import *

load_dotenv()


def run_etl_zmb51(folder_path):

    BATCH_SIZE = 1  
    txt_files = sorted(Path(folder_path).glob("ZMB51_*.txt"))
    n_batches = ceil(len(txt_files) / BATCH_SIZE)
    # n_batches = 3
    processed_dir = Path(folder_path, "processed")
    processed_dir.mkdir(exist_ok=True)

    print(f"Total {len(txt_files)} files, {n_batches} batches.")

    for b in range(n_batches):
        batch_files = txt_files[b*BATCH_SIZE : (b+1)*BATCH_SIZE]
        if not batch_files:
            break

        print(f"🚚 處理批次 {b+1}/{n_batches}，檔案數 {len(batch_files)} …")

        # -------- 讀 + 清洗 --------

        print("🔹 開始清理 Zmb51 檔案...")

        dtype_dict = {
        'Site': str,
        'Article': str,
        'MvT': str,
        'Cost Ctr': str,
        'Art. Doc.': str,
        }
        
        dfs = []
        for fp in batch_files:
            df = (
                pd.read_csv(fp, sep="\t", skiprows=2, dtype=dtype_dict, low_memory=False,)
                    .iloc[:, 1:]                            # 去掉空白首欄
            )
            df.columns = df.columns.str.strip()
            df['Pstng Date'] = pd.to_datetime(df['Pstng Date'], format='%m/%d/%Y')
            df.dropna(subset=['Article', 'Site', 'Pstng Date'], inplace=True)
            dfs.append(df)

        batch_df = pd.concat(dfs, ignore_index=True)
        
        # 欄位正名
        batch_df = batch_df.rename(columns={
            "Quantity i": "Quantity",
            "Amount LC":  "Cost",
            "Pstng Date": "Date",
            "Amount in LC":  "Cost",
        })

        # 數字清洗
        batch_df[["Quantity", "Cost"]] = batch_df[["Quantity", "Cost"]].apply(fast_numeric)
        
        batch_df['Quantity'] = batch_df['Quantity'] * -1
        batch_df['Cost'] = batch_df['Cost'] * -1

        groupby_df = batch_df.groupby(['Article', 'Site', 'Date']).agg({
        'Quantity': 'sum',
        'Cost': 'sum',
        'BUn' : 'first'
        }).reset_index()    

        print(f"🚚 批次 {b+1} 清洗後資料：\n{groupby_df.head(2)}\n"
            f"🚚 批次 {b+1} 清洗後資料筆數：{len(groupby_df)}\n")

        # 上傳至 SQL Server
        print(f"🔹 開始上傳 ZMB51 資料到 {os.getenv("SQL_DB")}...")
        
        engine = get_sql_engine()
        column_types = {
            "Site": NVARCHAR(10),
            "Article": NVARCHAR(20),
            "BUn": NVARCHAR(10),
            "Date": Date(),
            "Quantity": DECIMAL(18, 6),
            "Cost": DECIMAL(18, 6),
        }
        upsert_batch(
            df=groupby_df,
            target_table=os.getenv("TABLE_ZMB51"),
            unique_keys=["Article", "Site", "Date"],
            column_types=column_types,
        )
        
        # upload_to_sql(groupby_df, os.getenv("TABLE_ZMB51"), column_types, if_exists="append")
        print(f"✅ 批次 {b+1} 已匯入 {os.getenv("TABLE_ZMB51")} {len(groupby_df):,} 列\n")

        # ---------- 移動到 processed ----------
        for fp in batch_files:
            dest = processed_dir / fp.name
            # 若同名檔已存在就加時間戳避免覆寫
            if dest.exists():
                timestamp = datetime.now().strftime("%Y%m%d%H%M")
                dest = processed_dir / f"{fp.stem}_{timestamp}{fp.suffix}"
            shutil.move(fp, dest)
        print(f"批次 {b+1} 檔案已移至 {processed_dir}\n")

    print("🎉 全部批次處理結束")



if __name__ == "__main__":

    # download_zmb51(os.getenv("DATE_FILE_ZMB51"), os.getenv("EXPORT_DIR_ZMB51"))
    run_etl_zmb51(os.getenv("EXPORT_DIR_ZMB51"))




