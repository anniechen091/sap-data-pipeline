import sys
import os

# ⬅️ 把 AutoScript 資料夾加入 sys.path
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

import pandas as pd
from ETL_SAP.common.loader import upload_to_sql
from ETL_SAP.common.config import get_sql_engine
from sqlalchemy.types import NVARCHAR, DECIMAL, INTEGER


def run_etl(path):
    """
    ETL pipeline for Zmmidr_9801 data.
    Reads data from an Excel file, processes it, and uploads it to SQL Server.
    """

    table_name = "Zmmidr_9801"
    print(f"🔹 讀取 {table_name} 檔案...")
    df = pd.read_excel(path)

    string_columns = ['Article No', 'MCH', 'Pack size', 'SCA Assortment', 'Assortment grade']

    for col in string_columns:
        df[col] = df[col].astype(str).str.strip().str.replace('.0', '', regex=False)

    column_types = {
        'Article No': NVARCHAR(50),
        'MCH': NVARCHAR(50),
        'Article Description': NVARCHAR(255),
        'Article Description.1': NVARCHAR(255),
        'Pack size': NVARCHAR(50),
        'D/C MAP': DECIMAL(10, 2),
        'Unrestricted-Use Stock': INTEGER,
        'Allocation Qty': INTEGER,
        'On order Stock': INTEGER,
        'Unrestricted Stock Value': DECIMAL(14, 2),
        'PTD MVMT': INTEGER,
        'YTD MVMT': INTEGER,
        'SCA Assortment': NVARCHAR(50),
        'Assortment grade': NVARCHAR(20),
        'Asrt.Grade Description': NVARCHAR(100)
    }


    engine = get_sql_engine()
    upload_to_sql(df, table_name, column_types, 'replace')

    print(f"Replace {table_name} 完成！")


if __name__ == "__main__":
    test_path = r"C:\Users\anniec\Documents\TAWA\AutoScript\DC Forecast - Seasonality\AllMCH\Zmmidr\Zmmidr_9801.XLSX"
    run_etl(test_path)
