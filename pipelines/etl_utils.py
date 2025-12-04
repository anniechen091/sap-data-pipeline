import pandas as pd
import numpy as np
import re
import os
import time
from datetime import date
from sqlalchemy import types
from sqlalchemy import inspect
from ETL_SAP.common.config import get_sql_engine

from dotenv import load_dotenv



load_dotenv()


def clean_number(val):
    if pd.isna(val):
        return np.nan
    val = str(val).replace(',', '').strip()
    if val.endswith('-') and val[:-1].replace('.', '', 1).isdigit():
        val = '-' + val[:-1]  # 處理尾巴負號
    try:
        return pd.to_numeric(val, errors="coerce")
    except:
        return np.nan
    
def fast_numeric(col: pd.Series) -> pd.Series:

    pat_tail_neg   = re.compile(r"^([\d.]+)-$")        # 1234- → -1234
    s = col.astype(str).str.replace(",", "", regex=False)              # 去逗號
    s = s.str.replace(pat_tail_neg,    r"-\1", regex=True)             # 尾巴負號
    return pd.to_numeric(s, errors="coerce")  

def kill_excel():
    os.system("taskkill /f /im excel.exe >nul 2>&1")

def retry_call(func, args=(), kwargs={}, max_retries=3, delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            print(f"開始執行 {func.__name__} ... ")
            result = func(*args, **kwargs)
            print(f"{func.__name__} 執行完畢! ")
            return result  # 成功執行，返回結果
    
        except Exception as e:
            print(f"⚠️ 第 {attempt} 次執行 {func.__name__} 失敗：{e}")
            if attempt < max_retries:
                time.sleep(delay)
            else:
                print(f"❌ {func.__name__} 全部重試失敗")
                return False  # 表示完全失敗
            
def sql_type_string(col_type):
    if isinstance(col_type, types.NVARCHAR):
        return f"NVARCHAR({col_type.length})"
    elif isinstance(col_type, types.VARCHAR):
        return f"VARCHAR({col_type.length})"
    elif isinstance(col_type, types.INTEGER):
        return "INT"
    elif isinstance(col_type, types.DECIMAL):
        return f"DECIMAL({col_type.precision}, {col_type.scale})"
    elif isinstance(col_type, types.Date):
        return "DATE"
    elif isinstance(col_type, types.DateTime):
        return "DATETIME"
    else:
        raise ValueError(f"Unsupported SQL type for ALTER COLUMN: {col_type}")

def get_acctwk(target_date):
    df_cal = pd.read_excel(r"C:\Users\anniec\Documents\TAWA\AutoScript\ETL_SAP\mapping_tables\maintain\Calendar.xlsx")
    df_cal["Date"] = pd.to_datetime(df_cal["Date"]).dt.date

    date_only = target_date.date()
    acctwk = df_cal.loc[df_cal["Date"] == date_only, "AcctWk"].squeeze()
    print("find acctwk:", acctwk)

    if pd.isna(acctwk):
        raise ValueError(f"No AcctWk found for date: {date_only}")

    return int(acctwk)



def clean_df_by_sql_schema(df, table_name):
    """
    Automatically clean dataframe dtypes based on SQL Server schema.
    If the table does not exist, skip cleaning.
    """

    engine = get_sql_engine()
    insp = inspect(engine)

    # Extract schema + table name (e.g., dbo.dim_Article)

    parts = table_name.strip().split('.')
    if len(parts) == 2:
        schema, tbl = parts
    else:
        schema = "dbo"
        tbl = parts[0]

    # ====== Check if table exists ======
    table_list = insp.get_table_names(schema=schema)

    if tbl not in table_list:
        print(f"⚠️ SQL table `{table_name}` does not exist — skipping dtype cleaning.")
        return df

    # ====== Read schema information ======
    try:
        columns_info = insp.get_columns(tbl, schema=schema)
    except:
        print(f"⚠️ Unable to read schema for `{table_name}` — skipping dtype cleaning.")
        return df

    # ====== Clean df dtypes according to SQL schema ======
    for col in columns_info:
        colname = col["name"]
        coltype = str(col["type"]).lower()

        if colname not in df.columns:
            continue

        # 1) String columns
        if any(t in coltype for t in ["varchar", "char", "text"]):
            df[colname] = df[colname].fillna("").astype(str).replace("nan", "")

        # 2) Numeric columns
        elif any(t in coltype for t in ["decimal", "numeric", "float", "int"]):
            df[colname] = pd.to_numeric(df[colname], errors="coerce")

        # 3) Date columns
        elif "date" in coltype:
            df[colname] = (
                pd.to_datetime(df[colname], errors="coerce")
                .dt.strftime('%Y-%m-%d')
            )

    print(f"🔧 Dtype cleaning completed based on SQL schema for `{table_name}`.")
    return df


