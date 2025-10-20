import os
from ETL_SAP.pipelines.etl_utils import *
from ETL_SAP.sap_scripts.downloader_zstpromo import download_zstpromo
from ETL_SAP.pipelines.etl_zstpromo import run_etl_zstpromo
from ETL_SAP.sap_scripts.downloader_zmb51 import download_zmb51
from ETL_SAP.pipelines.etl_zmb51 import run_etl_zmb51
from ETL_SAP.pipelines.etl_zmmidr_bun import run_etl_zmmidr_BUn
from ETL_SAP.sap_scripts.downloader_zmmidr_bun import download_zmmidr_BUn
from ETL_SAP.pipelines.etl_zmmidr_oun import run_etl_zmmidr_OUn
from ETL_SAP.sap_scripts.downloader_zmmidr_oun import download_zmmidr_OUn
from ETL_SAP.pipelines.etl_zmachk import run_etl_zmachk
from ETL_SAP.sap_scripts.downloader_zmachk import download_zmachk
from ETL_SAP.pipelines.etl_weekly_sales import run_etl_weekly_sales
from ETL_SAP.sap_scripts.downloader_zrssale import download_zrssale
from ETL_SAP.pipelines.etl_zrssale import run_etl_zrssale_D2, run_etl_zrssale_D3
from ETL_SAP.pipelines.etl_StoreRP import run_etl_storeRP
from ETL_SAP.sap_scripts.downloader_storeRP import download_storeRP

from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":

    print("開始執行所有 ETL pipelines...\n")

    '''Inventory''' 

    # if retry_call(download_zmmidr_OUn, args=(os.getenv("EXPORT_DIR_ZMMIDR_OUn"),)):
    #     retry_call(run_etl_zmmidr_OUn, args=(os.getenv("EXPORT_DIR_ZMMIDR_OUn"),))

    # if retry_call(download_zmmidr_BUn, args=(os.getenv("EXPORT_DIR_ZMMIDR_BUn"),)):
    #     retry_call(run_etl_zmmidr_BUn, args=(os.getenv("EXPORT_DIR_ZMMIDR_BUn"),))

    '''Sales''' 

    if retry_call(download_zmb51, args=(os.getenv("DATE_FILE_ZMB51"), os.getenv("EXPORT_DIR_ZMB51"))):
        retry_call(run_etl_zmb51, args=(os.getenv("EXPORT_DIR_ZMB51"),))

    if retry_call(download_zstpromo, args=(os.getenv("DATE_FILE_ZSTPROMO"), os.getenv("EXPORT_DIR_ZSTPROMO"))):
        retry_call(run_etl_zstpromo, args=(os.getenv("EXPORT_DIR_ZSTPROMO"),))

    
    # try:
    #     retry_call(run_etl_weekly_sales, args=(202541, 202541))
    # except Exception as e:
    #     print(f"Error exporting to Excel: {e}")


    # if retry_call(download_zrssale, args=(os.getenv("DATE_FILE_WALONG_SALES"), os.getenv("EXPORT_DIR_ZRSSALE"))):
    #     retry_call(run_etl_zrssale_D2, args=(os.getenv("EXPORT_DIR_ZRSSALE"),))
    #     retry_call(run_etl_zrssale_D3, args=(os.getenv("EXPORT_DIR_ZRSSALE"),))

    '''Article Master File''' 

    # if retry_call(download_zmachk, args=(os.getenv("EXPORT_DIR_ZMACHK"),)):
    #     retry_call(run_etl_zmachk, args=(os.getenv("EXPORT_DIR_ZMACHK"),))


    '''Store RP'''

    # if retry_call(download_storeRP, args=(os.getenv("EXPORT_DIR_StoreRP"),)):
    #     retry_call(run_etl_storeRP, args=(os.getenv("EXPORT_DIR_StoreRP"),))



    print("\n所有 ETL pipelines 執行完成！")