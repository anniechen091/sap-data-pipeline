import os
import pyodbc
import urllib
from sqlalchemy import text
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env file. 自動讀取 .env 檔案中的環境變數並載入進 os.environ 中

def get_sql_engine():
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('SQL_SERVER')};"
        f"DATABASE={os.getenv('SQL_DB')};"
        f"Trusted_Connection=yes;"
        f"CHARSET=UTF8;"
        f"Connection Timeout=60;"
        f"Query Timeout=0;"
    )
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    return engine

# def get_sql_engine():
#     conn_str = (
#         f"DRIVER={{ODBC Driver 17 for SQL Server}};"
#         f"SERVER={os.getenv('SQL_SERVER')};"
#         f"DATABASE={os.getenv('SQL_DB')};"
#         f"Trusted_Connection=yes;"
#         f"CHARSET=UTF8;"
#     )
#     # 🔥 使用 pyodbc 原生連線，開啟 fast_executemany
#     conn = pyodbc.connect(conn_str)
#     # conn.fast_executemany = True  # ← 加上這一行，加速 INSERT 效能

#     # ✅ 交給 SQLAlchemy 建立 engine
#     engine = create_engine("mssql+pyodbc://", creator=lambda: conn)
#     return engine


if __name__ == "__main__":
    try:
        SQL_ENGINE = get_sql_engine()
        with SQL_ENGINE.connect() as conn:
            result = conn.execute(text("SELECT GETDATE()"))
            print("✅ 連線成功，現在時間是：", result.scalar())
    except SQLAlchemyError as e:
        print("❌ 連線失敗，錯誤訊息如下：")
        print(e)