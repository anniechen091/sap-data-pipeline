import os
import pandas as pd
from pathlib import Path
from sqlalchemy import types, inspect
from sqlalchemy import text
from ETL_SAP.common.config import get_sql_engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from ETL_SAP.pipelines.etl_utils import *


# 所有 Excel 設定（Table 名、PK、dtype）
excel_configs = {
    "Calendar.xlsx": {
        "table": "dim_Calendar",
        "pk": ["Date"],
        "dtype": {
            "Date": types.Date(),
            "Wkday": types.NVARCHAR(7),
            "Week": types.NVARCHAR(5),
            "Period": types.NVARCHAR(3),
            "Month" : types.INTEGER(),
            "Quarter": types.NVARCHAR(2),
            "Year": types.INTEGER(),
            "AcctWk": types.NVARCHAR(6),
            "PromWk": types.NVARCHAR(6),
        }
    },
    
    "WeekPeriod.xlsx": {
        "table": "dim_WeekPeriod",
        "pk": ["AcctWk"],
        "dtype": {
            "AcctWk": types.NVARCHAR(6),
            "Period": types.NVARCHAR(3),
            "Quarter": types.NVARCHAR(2),
            "Year": types.INTEGER(),
        }
    },
    "MCH_CM.xlsx": {
        "table": "dim_MCH_CM",
        "pk": ["MCH"],
        "dtype": {
            "MCH": types.NVARCHAR(8),
            "MCH_Description": types.NVARCHAR(100),
            "Class" : types.NVARCHAR(6),
            "Class_Description": types.NVARCHAR(50),
            "Dept": types.NVARCHAR(4),
            "Dept_EN": types.NVARCHAR(25),
            "CM": types.NVARCHAR(25),
        }
    },

    "DC.xlsx": {
        "table": "dim_DC",
        "pk": ["DC"],
        "dtype": {
            "DC": types.NVARCHAR(4),
            "Region": types.NVARCHAR(5),
            "DC_Type": types.NVARCHAR(20),
            "Company": types.NVARCHAR(10),
        }
    },

        "Site.xlsx": {
        "table": "dim_Store",
        "pk": ["Site"],
        "dtype": {
            "Site": types.NVARCHAR(4),
            "Region": types.NVARCHAR(5),
            "Region2" : types.NVARCHAR(12),
            "Region3" : types.NVARCHAR(12),
            "State": types.NVARCHAR(2),
            "Company_Code": types.NVARCHAR(7),
            "Store_Name": types.NVARCHAR(20),
            "Open_Date": types.Date(),
        }
    },

    "Customer.XLSX": {
        "table": "dim_WLChannel",
        "pk": ["Sales_Group", "Sold_To"],
        "dtype": {
            "Sales_Group": types.NVARCHAR(10),
            "Sold_To": types.NVARCHAR(10),
            "Customer_Name": types.NVARCHAR(100),
            "Channel": types.NVARCHAR(20),
            "Sales_District": types.NVARCHAR(20),
            "Sales_Office": types.NVARCHAR(10),
        }
    },

        "Tawa_Fcst.xlsx": {
        "table": "fact_Forecast",
        "pk": ["Article_NoDC"],
        "dtype": {
            "Site": types.NVARCHAR(10),
            "Article": types.NVARCHAR(20),
            "Tawa_Fsct": types.DECIMAL(18, 6),
            "Tawa_Fsct_Inlcude_Promo": types.DECIMAL(18, 6),
            "Tawa_On_Order": types.DECIMAL(18, 6),
            "Qty_Oun": types.NVARCHAR(10),
            "Walong_Fcst": types.DECIMAL(18, 6),
            "Walong_Fcst_Include_promo": types.DECIMAL(18, 6),
            "Lead_Time": types.DECIMAL(18, 6),
            "Order_Interval": types.DECIMAL(18, 6),
            "Safety_Stock": types.DECIMAL(18, 6),
            "Fina_Order_Qty": types.DECIMAL(18, 6),
            "Dept": types.NVARCHAR(50),
            "Article_NoDC": types.NVARCHAR(20),
            "Date": types.DATE()
        }
    },

        "MCH-Dept head.xlsx": {
        "table": "dim_MCH_CM",
        "pk": ["MCH"],
        "dtype": {
            "MCH": types.NVARCHAR(8),
            "Dept_EN" : types.NVARCHAR(25),
            "Dept_CN" : types.NVARCHAR(20),
            "MCH_Section": types.NVARCHAR(100),
            "MCH_Group" : types.NVARCHAR(30),
            "Dept_Head": types.NVARCHAR(25),
            "Current_CM": types.NVARCHAR(25),
            "New_CM": types.NVARCHAR(25),
        }
    },

    "Division.xlsx": {
        "table": "Map_Division",
        "pk": ["Division"],
        "dtype": {
            "Division": types.INTEGER(),
            "Description": types.NVARCHAR(20),
        }
    },
    # 可以加入更多 Excel
    # "xxx.xlsx": {...}
}

SCHEMA = "dbo"  # SQL Server schema name
def upsert_excel_to_sql(file_path, config):
    df = pd.read_excel(file_path)



    base_table = config["table"]            # 純表名，不含 schema
    table      = f"{SCHEMA}.{base_table}"   # dbo.Dim_WeekPeriod
    stg_name   = f"{base_table}_stg"
    stg_table  = f"{SCHEMA}.{stg_name}"

    pk = config["pk"]
    dtype = config["dtype"]

    engine = get_sql_engine()

    with engine.begin() as conn:
        try:
            before = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        except SQLAlchemyError:
            before = 0  # 表示表格不存在

        # 建立暫存表
        # df.to_sql(stg_table, con=conn, dtype=dtype, if_exists="replace", index=False)

        df.to_sql(
            name      = stg_name,
            schema    = SCHEMA,         
            con       = conn,
            if_exists = "replace",
            index     = False,
            dtype     = dtype,
        )


        # 根據 PK 欄補上 NOT NULL 屬性（SQL Server 建表預設都是 NULLABLE）
        for col in pk:
            conn.execute(text(
                f"ALTER TABLE {stg_table} "
                f"ALTER COLUMN [{col}] {sql_type_string(dtype[col])} NOT NULL"))

        # 建立正式表（如不存在, 複製結構但不含資料）
        create_table_sql = f"""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{SCHEMA}' AND TABLE_NAME = '{base_table}'
        )
        SELECT * INTO {table} FROM {stg_table} WHERE 1=0;
        """
        conn.execute(text(create_table_sql))

        # 建立 PK（如不存在）
        check_pk_sql = f"""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
        WHERE TABLE_SCHEMA = '{SCHEMA}'
          AND TABLE_NAME   = '{base_table}'
          AND CONSTRAINT_TYPE = 'PRIMARY KEY'
        """
        pk_exists = conn.execute(text(check_pk_sql)).scalar()

        if not pk_exists:
            pk_name = f"PK_{base_table}"
            add_pk_sql = f"""
            ALTER TABLE {table}
            ADD CONSTRAINT {pk_name} PRIMARY KEY ({', '.join(pk)});
            """
            conn.execute(text(add_pk_sql))

        # 合併資料
        merge = f"""
        MERGE {table} AS target
        USING {stg_table} AS source
        ON {" AND ".join([f"target.[{col}] = source.[{col}]" for col in pk])}
        WHEN MATCHED THEN 
            UPDATE SET
            {", ".join([f"target.[{col}] = source.[{col}]" for col in df.columns if col not in pk])}
        WHEN NOT MATCHED THEN 
            INSERT ({", ".join(df.columns)})
            VALUES ({", ".join([f"source.[{col}]" for col in df.columns])});
        """
        conn.execute(text(merge))
        conn.execute(text(f"DROP TABLE {stg_table}")) # 刪除 staging 表

        after = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        print(f"{table}: 筆數從 {before} → {after}，共新增 {after - before} 筆。")

        print(f"✅ 上傳完成：{file_path.name} → DB {table}")


def main(folder_path):
    for file_name, config in excel_configs.items():
        file_path = Path(folder_path) / file_name
        if file_path.exists():
            print(f"🔹 開始上傳 {file_name} 到 SQL Server...")
            upsert_excel_to_sql(file_path, config)
        else:
            continue


if __name__ == "__main__":
    main(r"C:\Users\anniec\Documents\TAWA\AutoScript\ETL_SAP\mapping_tables")