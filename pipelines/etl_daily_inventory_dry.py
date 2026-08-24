import os
import shutil
import tempfile
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import win32com.client as win32
import win32clipboard
from datetime import date
from ETL_SAP.sap_scripts.sap_utils import *
from ETL_SAP.sap_scripts.login import sap_login

import urllib
import urllib.parse
import urllib.request
from sqlalchemy import text
from sqlalchemy import create_engine

from dotenv import load_dotenv

load_dotenv()

# OOCL's generated workbook has no default Excel style. This warning is
# harmless and does not affect the workbook data or validation.
warnings.filterwarnings(
    "ignore", message="Workbook contains no default style, apply openpyxl's default"
)


# -----------------------------------------------------------------------
# Paths / Gmail settings
# -----------------------------------------------------------------------

BASE_DIR = Path(
    r"C:\Users\anniec\Documents\TAWA\AutoScript\DC Forecast - Seasonality\TawaWalong\ETA"
)
OOCL_ETA_PATH = BASE_DIR / "OOCL_ETA.xlsx"

# Use a dedicated Chrome profile so Selenium does not conflict with Annie's
# normal Chrome window. Run the script once before leaving and sign in to the
# company Gmail in the opened window. The login session is then reused daily.
OOCL_CHROME_PROFILE_DIR = Path(
    os.getenv("OOCL_CHROME_PROFILE_DIR", str(BASE_DIR / "ChromeProfile_OOCL"))
)
OOCL_GMAIL_URL = os.getenv(
    "OOCL_GMAIL_URL", "https://mail.google.com/mail/u/0/"
).rstrip("/")
OOCL_GMAIL_LOGIN_WAIT_SECONDS = int(
    os.getenv("OOCL_GMAIL_LOGIN_WAIT_SECONDS", "180")
)
OOCL_DOWNLOAD_WAIT_SECONDS = int(
    os.getenv("OOCL_DOWNLOAD_WAIT_SECONDS", "60")
)

# Google Sheets destination for the Dry Grocery daily inventory output.
GOOGLE_SPREADSHEET_ID = "1P6mdd94PpgomDWrx5MQ-q-88Ps2o9sGdxTWS3v_RJLQ"
GOOGLE_ZMMIDR_TAB = "Zmmidr"
GOOGLE_ZMMIDR_STAGING_TAB = "_Zmmidr_ETL_Staging"
GOOGLE_SERVICE_ACCOUNT_FILE = Path(
    os.getenv(
        "GOOGLE_SERVICE_ACCOUNT_FILE",
        str(BASE_DIR / "google_service_account.json"),
    )
)
GOOGLE_OAUTH_CLIENT_FILE = Path(
    os.getenv(
        "GOOGLE_OAUTH_CLIENT_FILE",
        str(BASE_DIR / "google_oauth_credentials.json"),
    )
)
GOOGLE_AUTHORIZED_USER_FILE = Path(
    os.getenv(
        "GOOGLE_AUTHORIZED_USER_FILE",
        str(BASE_DIR / "google_authorized_user.json"),
    )
)


def _wait_for_download(download_dir, timeout_seconds):
    """Return the newest completely downloaded Excel file."""
    deadline = time.time() + timeout_seconds
    previous_size = None
    stable_count = 0

    while time.time() < deadline:
        partial_files = list(download_dir.glob("*.crdownload"))
        excel_files = [
            path
            for path in download_dir.iterdir()
            if path.is_file() and path.suffix.lower() in {".xlsx", ".xls"}
        ]

        if excel_files and not partial_files:
            newest = max(excel_files, key=lambda path: path.stat().st_mtime)
            current_size = newest.stat().st_size

            if current_size > 0 and current_size == previous_size:
                stable_count += 1
            else:
                stable_count = 0

            if stable_count >= 2:
                return newest

            previous_size = current_size

        time.sleep(1)

    raise TimeoutError(
        f"Excel attachment did not finish downloading within {timeout_seconds} seconds."
    )


def _validate_oocl_excel(file_path):
    """Validate that the downloaded workbook matches the OOCL report layout."""
    # pandas does not support using nrows and skipfooter together. The footer
    # is irrelevant for a five-row header/column validation preview.
    preview = pd.read_excel(file_path, skiprows=4, nrows=5)
    required_columns = {"SAP PO#", "Out-Gate(Actual)", "FND/ETA"}
    missing_columns = required_columns.difference(preview.columns)

    if missing_columns:
        raise ValueError(
            "Downloaded workbook is not a valid OOCL ETA report. "
            f"Missing columns: {sorted(missing_columns)}"
        )


def _download_excel_attachment(driver, wait, download_dir):
    """Download the OOCL attachment from Gmail's visible view=att link."""
    from selenium.webdriver import ActionChains
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait

    def download_has_started(timeout_seconds=8):
        """Confirm Chrome actually created a download file or partial file."""
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            partial_files = list(download_dir.glob("*.crdownload"))
            excel_files = [
                path
                for path in download_dir.iterdir()
                if path.is_file() and path.suffix.lower() in {".xlsx", ".xls"}
            ]
            if partial_files or excel_files:
                return True
            time.sleep(0.5)
        return False

    def oocl_attachment_blocks(current_driver):
        return [
            block
            for block in current_driver.find_elements(By.CSS_SELECTOR, "div.aQH")
            if block.is_displayed()
            and "oocl_eta" in (block.get_attribute("innerText") or block.text).lower()
        ]

    attachment_blocks = wait.until(oocl_attachment_blocks)

    for attachment_block in attachment_blocks:
        filename_elements = attachment_block.find_elements(
            By.CSS_SELECTOR, ".aV3, [data-tooltip*='OOCL_ETA'], [aria-label*='OOCL_ETA']"
        )
        filename = next(
            (
                element.get_attribute("data-tooltip")
                or element.get_attribute("aria-label")
                or element.text.strip()
                for element in filename_elements
                if (
                    element.get_attribute("data-tooltip")
                    or element.get_attribute("aria-label")
                    or element.text.strip()
                )
            ),
            (attachment_block.get_attribute("innerText") or "OOCL_ETA attachment").strip(),
        )
        print(f"✅ Excel attachment found: {filename}")

        safe_filename = Path(filename).name
        if not safe_filename.lower().endswith((".xlsx", ".xls")):
            safe_filename = "OOCL_ETA_download.xlsx"

        # The actual down-arrow shown in Gmail is an <a> whose URL contains
        # view=att. It is created/made visible only while the card is hovered.
        ActionChains(driver).move_to_element(attachment_block).pause(1).perform()

        def visible_view_attachment_link(_current_driver):
            selectors = (
                'a[href*="view=att"][href*="disp=safe"]',
                'a[href*="view=att"]',
            )
            for selector in selectors:
                links = attachment_block.find_elements(By.CSS_SELECTOR, selector)
                visible_links = [link for link in links if link.is_displayed()]
                if visible_links:
                    return visible_links[0]
            return False

        try:
            download_link = WebDriverWait(driver, 10).until(
                visible_view_attachment_link
            )
            attachment_url = download_link.get_attribute("href")
        except Exception:
            download_link = None
            attachment_url = None

        if attachment_url:
            print("✅ Gmail's real attachment download link found")
            cookie_header = "; ".join(
                f"{cookie['name']}={cookie['value']}"
                for cookie in driver.get_cookies()
            )
            user_agent = driver.execute_script("return navigator.userAgent;")
            request = urllib.request.Request(
                attachment_url,
                headers={
                    "Cookie": cookie_header,
                    "User-Agent": user_agent,
                    "Referer": driver.current_url,
                },
            )

            try:
                with urllib.request.urlopen(request, timeout=60) as response:
                    attachment_bytes = response.read()

                is_excel = attachment_bytes.startswith(
                    b"PK\x03\x04"
                ) or attachment_bytes.startswith(b"\xd0\xcf\x11\xe0")
                if not is_excel:
                    raise ValueError(
                        "Gmail returned a web page instead of an Excel workbook."
                    )

                direct_download_path = download_dir / safe_filename
                with open(direct_download_path, "wb") as output_file:
                    output_file.write(attachment_bytes)

                print(f"✅ Gmail attachment downloaded: {safe_filename}")
                return safe_filename

            except Exception as exc:
                print(f"⚠️ Direct URL download failed; clicking real arrow: {exc}")

        if download_link is not None:
            try:
                download_link.click()
            except Exception:
                driver.execute_script("arguments[0].click();", download_link)
            print(f"✅ Gmail attachment control clicked: {filename}")
            if download_has_started():
                return filename

            # Gmail sometimes opens its spreadsheet preview instead of
            # downloading. Continue below and click the preview toolbar's
            # actual Download control.
            print("ℹ️ Gmail opened the attachment preview; locating its Download button")

        # Some Gmail layouts do not place the download button inside the card
        # until the attachment preview is open. Click the filename/card, then
        # use the preview's Download control.
        if download_link is None:
            preview_target = filename_elements[0] if filename_elements else attachment_block
            driver.execute_script("arguments[0].click();", preview_target)

        def preview_download_control(current_driver):
            selectors = (
                '[aria-label="Download"]',
                '[aria-label*="Download attachment"]',
                '[data-tooltip="Download"]',
                '[data-tooltip*="Download attachment"]',
                'a[download]',
            )
            for selector in selectors:
                visible_controls = [
                    element
                    for element in current_driver.find_elements(By.CSS_SELECTOR, selector)
                    if element.is_displayed()
                ]
                if visible_controls:
                    return visible_controls[-1]
            return False

        try:
            preview_download = WebDriverWait(driver, 10).until(
                preview_download_control
            )
            driver.execute_script("arguments[0].click();", preview_download)
            print(f"✅ Preview download button clicked: {filename}")
            if download_has_started():
                return filename
        except Exception:
            continue

    raise FileNotFoundError(
        "The OOCL email attachment card was found, but Gmail exposed no clickable download control."
    )


def download_latest_oocl_eta():
    """
    Download the newest Excel attachment from the newest Gmail message whose
    subject contains '[EXTERNAL] OOCL_ETA'.

    This function is intentionally non-blocking for the ETL: if Gmail is logged
    out, no matching email exists, or validation/download fails, the existing
    OOCL_ETA.xlsx is kept and the rest of the ETL continues.
    """
    driver = None
    download_dir = None

    try:
        from selenium import webdriver
        from selenium.common.exceptions import TimeoutException
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support.ui import WebDriverWait

        BASE_DIR.mkdir(parents=True, exist_ok=True)
        OOCL_CHROME_PROFILE_DIR.mkdir(parents=True, exist_ok=True)
        download_dir = Path(
            tempfile.mkdtemp(prefix="oocl_download_", dir=str(BASE_DIR))
        )

        chrome_options = Options()
        chrome_options.add_argument(
            f"--user-data-dir={OOCL_CHROME_PROFILE_DIR}"
        )
        chrome_options.add_argument("--profile-directory=Default")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_experimental_option(
            "prefs",
            {
                "download.default_directory": str(download_dir),
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
            },
        )

        chromedriver_path = os.getenv("CHROMEDRIVER_PATH")
        service = Service(executable_path=chromedriver_path) if chromedriver_path else Service()
        driver = webdriver.Chrome(service=service, options=chrome_options)
        try:
            driver.execute_cdp_cmd(
                "Browser.setDownloadBehavior",
                {
                    "behavior": "allow",
                    "downloadPath": str(download_dir),
                    "eventsEnabled": True,
                },
            )
        except Exception:
            driver.execute_cdp_cmd(
                "Page.setDownloadBehavior",
                {"behavior": "allow", "downloadPath": str(download_dir)},
            )
        driver.get(OOCL_GMAIL_URL)

        # On the first manual run, allow time to complete the company's Gmail
        # sign-in. During an unattended run, a signed-out session times out and
        # safely falls back to the previous OOCL file.
        login_deadline = time.time() + OOCL_GMAIL_LOGIN_WAIT_SECONDS
        while time.time() < login_deadline:
            gmail_is_ready = (
                "mail.google.com" in driver.current_url
                and driver.find_elements(By.CSS_SELECTOR, 'input[name="q"]')
            )
            if gmail_is_ready:
                break
            time.sleep(2)
        else:
            raise TimeoutError(
                "Gmail is signed out or the company login did not finish in time."
            )

        wait = WebDriverWait(driver, 45)
        search_query = 'subject:"[EXTERNAL] OOCL_ETA" has:attachment filename:xlsx'

        # Enter the query through Gmail's search box. Navigating directly to a
        # #search URL can briefly leave the Inbox rows in the DOM, which could
        # cause Selenium to click the first unrelated Inbox message.
        search_box = wait.until(
            lambda current_driver: current_driver.find_element(
                By.CSS_SELECTOR, 'input[name="q"]'
            )
        )
        previous_url = driver.current_url
        search_box.click()
        search_box.send_keys(Keys.CONTROL, "a")
        search_box.send_keys(search_query)
        search_box.send_keys(Keys.ENTER)

        wait.until(
            lambda current_driver: (
                "#search/" in current_driver.current_url
                and current_driver.current_url != previous_url
            )
        )

        def newest_matching_email(current_driver):
            # The search results are newest-first. Re-check the visible row text
            # so an old Inbox row can never be mistaken for the OOCL message.
            for row in current_driver.find_elements(By.CSS_SELECTOR, "tr.zA"):
                if "oocl_eta" in row.text.lower():
                    return row
            return False

        try:
            newest_email = wait.until(newest_matching_email)
        except TimeoutException as exc:
            raise FileNotFoundError(
                "No Gmail message matched '[EXTERNAL] OOCL_ETA' with an xlsx attachment."
            ) from exc

        # Open the newest verified search result.
        driver.execute_script("arguments[0].click();", newest_email)

        opened_subject = wait.until(
            lambda current_driver: next(
                (
                    element.text
                    for element in current_driver.find_elements(
                        By.CSS_SELECTOR, "h2.hP"
                    )
                    if element.is_displayed() and element.text.strip()
                ),
                False,
            )
        )
        if "[external] oocl_eta" not in opened_subject.lower():
            raise RuntimeError(
                f"Safety check failed: opened the wrong Gmail subject: {opened_subject}"
            )

        print(f"✅ Correct OOCL email opened: {opened_subject}")
        attachment_name = _download_excel_attachment(driver, wait, download_dir)

        print(f"⏳ Waiting for download to finish: {attachment_name}")
        downloaded_file = _wait_for_download(
            download_dir, OOCL_DOWNLOAD_WAIT_SECONDS
        )
        _validate_oocl_excel(downloaded_file)

        # os.replace is atomic on the same drive. The old file is overwritten
        # only after the new attachment has passed validation.
        os.replace(downloaded_file, OOCL_ETA_PATH)
        print(f"✅ OOCL ETA updated: {OOCL_ETA_PATH}")
        return True

    except Exception as exc:
        if OOCL_ETA_PATH.exists():
            print(f"⚠️ OOCL ETA download failed; using existing file: {exc}")
            return False

        raise RuntimeError(
            "OOCL ETA download failed and no existing OOCL_ETA.xlsx is available."
        ) from exc

    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass
        if download_dir is not None:
            shutil.rmtree(download_dir, ignore_errors=True)


def _get_google_sheets_client():
    """Authenticate gspread for unattended Google Sheets updates."""
    try:
        import gspread
    except ImportError as exc:
        raise RuntimeError(
            "Google Sheets upload requires: python -m pip install gspread google-auth google-auth-oauthlib"
        ) from exc

    if GOOGLE_SERVICE_ACCOUNT_FILE.exists():
        print(f"✅ Using Google service account: {GOOGLE_SERVICE_ACCOUNT_FILE}")
        return gspread.service_account(filename=str(GOOGLE_SERVICE_ACCOUNT_FILE))

    if GOOGLE_OAUTH_CLIENT_FILE.exists():
        print(f"✅ Using Google OAuth client: {GOOGLE_OAUTH_CLIENT_FILE}")
        return gspread.oauth(
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
            credentials_filename=str(GOOGLE_OAUTH_CLIENT_FILE),
            authorized_user_filename=str(GOOGLE_AUTHORIZED_USER_FILE),
        )

    raise FileNotFoundError(
        "Google Sheets credentials were not found. Add either "
        f"{GOOGLE_SERVICE_ACCOUNT_FILE} or {GOOGLE_OAUTH_CLIENT_FILE}."
    )


def _google_sheet_cell_value(value):
    """Convert pandas/numpy values into Google Sheets JSON-safe values."""
    if pd.isna(value):
        return ""
    if isinstance(value, pd.Timestamp):
        return f"{value.month}/{value.day}/{value.year}"
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    return value


def upload_inventory_to_google_sheet(inventory_df):
    """
    Replace the Zmmidr tab with the complete daily inventory output.

    Data is uploaded to a hidden staging tab first. Only after every chunk has
    succeeded does one atomic Sheets batch clear and replace the live Zmmidr
    values. Other tabs, formulas, and formatting remain untouched.
    """
    client = _get_google_sheets_client()
    import gspread

    spreadsheet = client.open_by_key(GOOGLE_SPREADSHEET_ID)
    target = spreadsheet.worksheet(GOOGLE_ZMMIDR_TAB)

    expected_columns = [
        "Article NoDC",
        "Article",
        "Site",
        "Unrestricted-Use Stock",
        "On order Stock",
        "Stock in Quality",
        "ETA",
        "Walong Status",
        "Tawa Status",
    ]
    if list(inventory_df.columns) != expected_columns:
        raise ValueError(
            "Google Sheets upload stopped because the final columns changed. "
            f"Expected {expected_columns}; received {list(inventory_df.columns)}"
        )

    required_rows = len(inventory_df) + 1
    required_columns = len(expected_columns)

    try:
        staging = spreadsheet.worksheet(GOOGLE_ZMMIDR_STAGING_TAB)
    except gspread.WorksheetNotFound:
        staging = spreadsheet.add_worksheet(
            title=GOOGLE_ZMMIDR_STAGING_TAB,
            rows=max(required_rows, 2),
            cols=required_columns,
        )

    # Keep the technical staging sheet out of normal user navigation.
    spreadsheet.batch_update(
        {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": staging.id,
                            "hidden": True,
                        },
                        "fields": "hidden",
                    }
                }
            ]
        }
    )

    staging.resize(rows=max(required_rows, 2), cols=required_columns)
    staging.batch_clear([f"A1:I{max(required_rows, 2)}"])
    staging.update(
        range_name="A1:I1",
        values=[expected_columns],
        value_input_option="USER_ENTERED",
    )

    chunk_size = 5000
    for start_index in range(0, len(inventory_df), chunk_size):
        end_index = min(start_index + chunk_size, len(inventory_df))
        chunk_values = [
            [_google_sheet_cell_value(value) for value in row]
            for row in inventory_df.iloc[start_index:end_index].itertuples(
                index=False, name=None
            )
        ]
        start_row = start_index + 2
        end_row = end_index + 1
        staging.update(
            range_name=f"A{start_row}:I{end_row}",
            values=chunk_values,
            value_input_option="USER_ENTERED",
        )
        print(f"⏳ Google Sheets staging uploaded: {end_index:,}/{len(inventory_df):,} rows")

    if target.row_count < required_rows or target.col_count < required_columns:
        target.resize(
            rows=max(target.row_count, required_rows),
            cols=max(target.col_count, required_columns),
        )

    target_clear_range = {
        "sheetId": target.id,
        "startRowIndex": 0,
        "endRowIndex": target.row_count,
        "startColumnIndex": 0,
        "endColumnIndex": required_columns,
    }
    populated_range = {
        "sheetId": staging.id,
        "startRowIndex": 0,
        "endRowIndex": required_rows,
        "startColumnIndex": 0,
        "endColumnIndex": required_columns,
    }
    destination_range = {
        "sheetId": target.id,
        "startRowIndex": 0,
        "endRowIndex": required_rows,
        "startColumnIndex": 0,
        "endColumnIndex": required_columns,
    }

    requests = [
        {
            "updateCells": {
                "range": target_clear_range,
                "fields": "userEnteredValue",
            }
        },
        {
            "copyPaste": {
                "source": populated_range,
                "destination": destination_range,
                "pasteType": "PASTE_VALUES",
                "pasteOrientation": "NORMAL",
            }
        },
    ]

    if required_rows > 1:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": target.id,
                        "startRowIndex": 1,
                        "endRowIndex": required_rows,
                        "startColumnIndex": 6,
                        "endColumnIndex": 7,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "DATE",
                                "pattern": "m/d/yyyy",
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )

    spreadsheet.batch_update({"requests": requests})

    verification = target.get("A1:I2")
    if not verification or verification[0] != expected_columns:
        raise RuntimeError("Google Sheets verification failed after replacing Zmmidr.")

    print(
        f"✅ Google Sheet updated: {GOOGLE_ZMMIDR_TAB} "
        f"({len(inventory_df):,} data rows)"
    )

def try_press(id_path, retries=3, pause=0.3):
    """Attempt a .press() on a control with small retries."""
    last = None
    for _ in range(retries):
        try:
            session.findById(id_path).press()
            return
        except Exception as e:
            last = e
            time.sleep(pause)
    raise last

def try_set_text(id_path, value, retries=3, pause=0.3):
    """Set .text on a control with small retries."""
    last = None
    for _ in range(retries):
        try:
            ctrl = session.findById(id_path)
            ctrl.text = value
            return
        except Exception as e:
            last = e
            time.sleep(pause)
    raise last

def optional_press(id_path):
    try:
        session.findById(id_path).press()
    except Exception:
        pass

def optional_select(id_path):
    try:
        session.findById(id_path).select()
    except Exception:
        pass

def set_clipboard(text):
    win32clipboard.OpenClipboard()
    win32clipboard.EmptyClipboard()
    win32clipboard.SetClipboardText(text)
    win32clipboard.CloseClipboard()


def close_sap_exported_workbooks(workbook_paths, wait_seconds=30):
    """Close only the workbooks exported by this SAP run.

    SAP may automatically open each exported workbook in Excel. Bind to each
    exact workbook path, close it without saving, and quit Excel only when that
    Excel instance has no other workbooks open. A close failure is non-fatal so
    the remaining ETL can continue.
    """
    pending = {Path(path).resolve() for path in workbook_paths}
    closed_names = []
    last_errors = {}
    deadline = time.time() + wait_seconds

    while pending and time.time() < deadline:
        for workbook_path in list(pending):
            if not workbook_path.exists():
                continue

            try:
                workbook = win32.GetObject(str(workbook_path))
                excel_app = workbook.Application
                previous_display_alerts = excel_app.DisplayAlerts
                excel_app.DisplayAlerts = False
                workbook.Close(SaveChanges=False)

                pending.remove(workbook_path)
                closed_names.append(workbook_path.name)

                if excel_app.Workbooks.Count == 0:
                    excel_app.Quit()
                else:
                    # Preserve the user's existing Excel session and settings.
                    excel_app.DisplayAlerts = previous_display_alerts
            except Exception as exc:
                last_errors[workbook_path] = exc

        if pending:
            time.sleep(1)

    if closed_names:
        print(
            "✅ Closed SAP-opened Excel workbook(s): "
            + ", ".join(sorted(closed_names))
        )

    if pending:
        details = "; ".join(
            f"{path.name}: {last_errors.get(path, 'file not found')}"
            for path in sorted(pending, key=lambda item: item.name.lower())
        )
        print(
            "⚠️ Could not close every SAP-opened Excel workbook; "
            f"ETL will continue: {details}"
        )

def get_sql_engine():
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER=TawaDW;"
        f"DATABASE=TawaDWDB;"
        f"Trusted_Connection=yes;"
        f"Connection Timeout=10;"
        f"Query Timeout=0;"
    )
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    return engine

dept = 'Dry Grocery'
today = date.today().strftime("%m.%d.%Y")


T_CODE = ["ME2M","ZINV_MCH", "ZMACHK"]
SITE = ["9790", "9900"]           # EM_WERKS multi-select
SELECTION_VARIANT = "WE101"         # SELPA-LOW
MCH = ["10600000", "10699999"]
DATE_FROM = "10/01/2022"            # mm/dd/yyyy
DATE_TO   = date.today().strftime("%m/%d/%Y")

DAILY_ROOT = BASE_DIR / "SOH OOD" / "Daily Update"
DAILY_DIR = DAILY_ROOT / today
SAVE_DIR = str(DAILY_DIR)
SAVE_NAME = ["SAP_ETA_","ZINV_MCH_", "ZMACHK_"]
DATE_FILE = today

SAP_ETA_FILE = DAILY_DIR / f"{SAVE_NAME[0]}{DATE_FILE}.xlsx"
ZINV_FILE = DAILY_DIR / f"{SAVE_NAME[1]}{DATE_FILE}.xlsx"
ZMACHK_FILE = DAILY_DIR / f"{SAVE_NAME[2]}{DATE_FILE}.xlsx"
FINAL_OUTPUT_FILE = DAILY_ROOT / f"{dept} Inventoy Info {today}.xlsx"

DAILY_DIR.mkdir(parents=True, exist_ok=True)


# Gmail download must run before the SAP / SQL ETL. A failure keeps the old
# validated OOCL_ETA.xlsx and allows the remaining daily update to continue.
download_latest_oocl_eta()

engine = get_sql_engine()
with engine.connect() as conn:
    result = conn.execute(text("SELECT GETDATE()"))
    print("✅ 連線成功，現在時間是：", result.scalar())


session = sap_login()

# ------- SAP ETA -------

try_set_text("wnd[0]/tbar[0]/okcd", T_CODE[0])
session.findById("wnd[0]").sendVKey(0)
time.sleep(0.5)

session.findById("wnd[0]").maximize
try_set_text("wnd[0]/usr/ctxtEM_WERKS-LOW", SITE[0])
try_set_text("wnd[0]/usr/ctxtEM_WERKS-HIGH", SITE[1])
try_set_text("wnd[0]/usr/ctxtS_MATKL-LOW", MCH[0])
try_set_text("wnd[0]/usr/ctxtS_MATKL-HIGH", MCH[1])
try_set_text("wnd[0]/usr/ctxtSELPA-LOW", SELECTION_VARIANT)
try_set_text("wnd[0]/usr/ctxtS_BEDAT-LOW", DATE_FROM)
try_set_text("wnd[0]/usr/ctxtS_BEDAT-HIGH", DATE_TO)
try_press("wnd[0]/tbar[1]/btn[8]")
try_press("wnd[0]/tbar[1]/btn[23]")
try_press("wnd[0]/tbar[1]/btn[43]")
try_set_text("wnd[1]/usr/ctxtDY_PATH", SAVE_DIR)
try_set_text("wnd[1]/usr/ctxtDY_FILENAME", str(SAVE_NAME[0]+DATE_FILE+".xlsx"))
session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = 7
# try_press("wnd[1]/tbar[0]/btn[0]")
try_press("wnd[1]/tbar[0]/btn[11]")

# ------- ZINV -------

try_press("wnd[0]/tbar[0]/btn[15]")
try_press("wnd[0]/tbar[0]/btn[15]")
try_set_text("wnd[0]/tbar[0]/okcd", T_CODE[1])
session.findById("wnd[0]").sendVKey(0)
time.sleep(0.5)

session.findById("wnd[0]").maximize
try_set_text("wnd[0]/usr/ctxtSBWKEY-LOW", SITE[0])
try_set_text("wnd[0]/usr/ctxtSBWKEY-HIGH", SITE[1])
try_set_text("wnd[0]/usr/ctxtSMATKL-LOW", MCH[0])
try_set_text("wnd[0]/usr/ctxtSMATKL-HIGH", MCH[1])
optional_select("wnd[0]/usr/radPLBKUM1")
optional_select("wnd[0]/usr/radP_OUNIT")
try_press("wnd[0]/tbar[1]/btn[8]")
optional_select("wnd[0]/mbar/menu[0]/menu[3]/menu[1]")
try_set_text("wnd[1]/usr/ctxtDY_PATH", SAVE_DIR)
try_set_text("wnd[1]/usr/ctxtDY_FILENAME", str(SAVE_NAME[1]+DATE_FILE+".xlsx"))
session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = 7
# try_press("wnd[1]/tbar[0]/btn[0]")
try_press("wnd[1]/tbar[0]/btn[11]")



# ------- ZMACHK -------

# Dry Grocery Report tab. Google Sheets UI filters do not change this CSV
# export, so every underlying Article is included.
sheet_id = GOOGLE_SPREADSHEET_ID
gid = "1673826654"
csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

review_dry = pd.read_csv(csv_url, header=1)
review_articles = review_dry[['Article']].dropna().drop_duplicates()


try_press("wnd[0]/tbar[0]/btn[15]")
try_press("wnd[0]/tbar[0]/btn[15]")
try_set_text("wnd[0]/tbar[0]/okcd", T_CODE[2])
session.findById("wnd[0]").sendVKey(0)
time.sleep(0.5)

review_articles.to_clipboard(index=False, header=False)

session.findById("wnd[0]").maximize
try_press("wnd[0]/usr/btn%_MATNR_%_APP_%-VALU_PUSH")
try_press("wnd[1]/tbar[0]/btn[24]")
try_press("wnd[1]/tbar[0]/btn[8]")
try_press("wnd[0]/tbar[1]/btn[8]")
optional_select("wnd[0]/mbar/menu[0]/menu[3]/menu[1]")
try_set_text("wnd[1]/usr/ctxtDY_PATH", SAVE_DIR)
review_articles.to_clipboard(index=False, header=False)
try_set_text("wnd[1]/usr/ctxtDY_FILENAME", str(SAVE_NAME[2]+DATE_FILE+".xlsx"))
session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = 17
# try_press("wnd[1]/tbar[0]/btn[0]")
try_press("wnd[1]/tbar[0]/btn[11]")

# SAP automatically opens the three exported files in Excel. Close only these
# exact workbooks before pandas reads them; leave any unrelated workbooks open.
close_sap_exported_workbooks([SAP_ETA_FILE, ZINV_FILE, ZMACHK_FILE])

# ------- combine -------

oocl_df = pd.read_excel(OOCL_ETA_PATH, skiprows=4, skipfooter=1)
oocl_df['DC ETA'] = np.where(
    oocl_df['Out-Gate(Actual)'].isna(),
    oocl_df['FND/ETA'] + pd.Timedelta(days=10),
    oocl_df['Out-Gate(Actual)'] + pd.Timedelta(days=5)
)

mask = oocl_df['SAP PO#'].notna()

oocl_df.loc[mask, 'SAP PO#'] = (
    oocl_df.loc[mask, 'SAP PO#']
        .astype('Int64')
        .astype(str)
)
oocl_df['SAP PO#'] = oocl_df['SAP PO#'].replace('<NA>', pd.NA)

oocl_clean = oocl_df[['SAP PO#', 'DC ETA']].drop_duplicates('SAP PO#')

sap = pd.read_excel(SAP_ETA_FILE)

# --- Type alignment ---
sap['Purchasing Document'] = sap['Purchasing Document'].astype(str).str.strip()
oocl_clean['SAP PO#'] = oocl_clean['SAP PO#'].astype(str).str.strip()

# --- Ensure ETA is datetime ---
oocl_clean['DC ETA'] = pd.to_datetime(oocl_clean['DC ETA'], errors='coerce')
sap['Stat.-Rel. Del. Date'] = pd.to_datetime(sap['Stat.-Rel. Del. Date'], errors='coerce')

# --- Apply OOCL ETA to 89 POs via PO# match ---
eta_map = oocl_clean.set_index('SAP PO#')['DC ETA']
sap['Stat.-Rel. Del. Date'] = sap['Purchasing Document'].map(eta_map).fillna(sap['Stat.-Rel. Del. Date'])

# --- Halve scheduled quantity for specific articles ---
mask = sap['Article'].isin([1661247, 2014080, 3662123])
sap.loc[mask, 'Scheduled Quantity'] = sap.loc[mask, 'Scheduled Quantity'] / 2

# -----------------------------------------------------------------------
# WALONG (3999992) / TAWA (3999998) 98-PO vs 89-PO handling
#
# For 9900 import orders, SAP creates two linked POs:
#   • 89XXXXXXXX  → ordering/shipment PO (Site 9890, Storage W900) — OOCL knows this PO
#   • 98XXXXXXXX  → receiving PO        (Site 9900, Storage 0001)  — OOCL does NOT know this PO
#
# Strategy:
#   1. Keep 89 POs (9890/W900) — they carry the OOCL ETA and are the source of truth.
#      Convert them to Site 9900 so downstream sees the right site.
#   2. Drop 98 POs that have a matching 89 PO (same Article + Document Date) — duplicate.
#   3. Keep 98 POs that have NO matching 89 PO (89 already received/gone, orphaned 98 PO).
#      Set their ETA to today + 3 days as a safe placeholder.
# -----------------------------------------------------------------------

is_walong_98 = (
    sap['Vendor/supplying site'].str.strip().str.startswith('3999992') |
    sap['Vendor/supplying site'].str.strip().str.startswith('3999998')
)

# Build a set of (Article, Document Date) keys from existing 89/W900 POs
po89_keys = set(
    zip(
        sap.loc[(sap['Storage Location'] == 'W900') & (sap['Site'] == 9890), 'Article'],
        #sap.loc[(sap['Storage Location'] == 'W900') & (sap['Site'] == 9890), 'Document Date'],
        sap.loc[(sap['Storage Location'] == 'W900') & (sap['Site'] == 9890), 'Scheduled Quantity'],
    )
)

# Mark each Walong/Tawa 98 PO as paired or orphaned
def has_89_pair(row):
    #return (row['Article'], row['Document Date']) in po89_keys
    return (row['Article'], row['Scheduled Quantity']) in po89_keys

#Which one?
walong_98_mask = is_walong_98 & sap['Purchasing Document'].str.startswith('98')

sap_walong_98 = sap[walong_98_mask].copy()
is_paired = sap_walong_98.apply(has_89_pair, axis=1)

# Drop paired 98 POs (the 89 PO already covers them)
paired_98_idx = sap_walong_98[is_paired].index
sap = sap.drop(index=paired_98_idx)

# Orphaned 98 POs: set ETA to today + 3 business days as placeholder
orphan_98_idx = sap_walong_98[~is_paired].index
today_ts = pd.Timestamp.today().normalize()
sap.loc[orphan_98_idx, 'Stat.-Rel. Del. Date'] = today_ts + pd.Timedelta(days=3)

# Convert remaining 89 POs (9890/W900) → Site 9900 for downstream processing
sap.loc[(sap['Site'] == 9890) & (sap['Storage Location'] == 'W900'), 'Site'] = 9900

# -----------------------------------------------------------------------

# --- Other site remapping ---
sap.loc[(sap['Site'] == 9790) & (sap['Storage Location'] == '0002'), 'Site'] = 9793

# --- Date window filter ---
sap = sap[
    (sap['Stat.-Rel. Del. Date'] > today_ts - pd.Timedelta(days=75)) &
    (sap['Stat.-Rel. Del. Date'] < today_ts + pd.Timedelta(days=365))
]

# --- Remove zero/trivial quantity rows ---
sap = sap[sap['Scheduled Quantity'] > 1]

eta = sap[['Article', 'Site', 'Stat.-Rel. Del. Date']].groupby(['Article', 'Site']).min().rename(columns = {'Stat.-Rel. Del. Date':'ETA'}).reset_index()

ood = sap[['Article', 'Site', 'Scheduled Quantity']].groupby(['Article', 'Site']).sum().rename(columns = {'Scheduled Quantity':'On order Stock'}).reset_index()

zinv = pd.read_excel(ZINV_FILE)

zinv = zinv[['Article No', 'Site', 'Available Inventory', 'Stock in Quality']].rename(columns = {'Article No':'Article',
                                                                                                  'Available Inventory':'Unrestricted-Use Stock'})

zmachk = pd.read_excel(ZMACHK_FILE)


zmachk_9891 = zmachk.copy()
zmachk_9891['Site'] = 9891
zmachk_9900 = zmachk.copy()
zmachk_9900['Site'] = 9900
zmachk_9790 = zmachk.copy()
zmachk_9790['Site'] = 9790
zmachk_9793 = zmachk.copy()
zmachk_9793['Site'] = 9793
zmachk_new = pd.concat([zmachk_9891, zmachk_9900, zmachk_9790, zmachk_9793], ignore_index = True)


conditions = [zmachk_new['Site'] == 9891, zmachk_new['Site'] == 9900, zmachk_new['Site'] == 9790,  zmachk_new['Site'] == 9793]
choices_walong = [zmachk_new['Status WS W'], "", zmachk_new['Status WS E'], zmachk_new['Status WS E']]
choices_tawa = [zmachk_new['Status SCA'], zmachk_new['Status NCA'], zmachk_new['Status EC'], ""]
zmachk_new['Walong Status'] = np.select(conditions, choices_walong, default = "")
zmachk_new['Tawa Status'] = np.select(conditions, choices_tawa, default = "")

zmachk_new = zmachk_new[['Site', 'Article', 'Walong Status', 'Tawa Status']]

for df in [zmachk_new, zinv, ood, eta]:
    df['Site'] = df['Site'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df['Article'] = df['Article'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    
inv_final = (
    zmachk_new
    .merge(zinv, on=['Site', 'Article'], how='left')
    .merge(ood, on=['Site', 'Article'], how='left')
    .merge(eta, on=['Site', 'Article'], how='left')
)

inv_final[['Unrestricted-Use Stock', 'Stock in Quality', 'On order Stock']] = (
    inv_final[['Unrestricted-Use Stock', 'Stock in Quality', 'On order Stock']].fillna(0)
)

inv_final['ETA'] = pd.to_datetime(inv_final['ETA'], errors='coerce').dt.normalize()

inv_final['Article NoDC'] = inv_final['Site'].astype(str) + inv_final['Article'].astype(str)
inv_final = inv_final[['Article NoDC', 'Article', 'Site', 'Unrestricted-Use Stock', 'On order Stock', 'Stock in Quality', 'ETA', 'Walong Status', 'Tawa Status']]

with pd.ExcelWriter(
    FINAL_OUTPUT_FILE,
    engine="openpyxl",
    date_format="m/d/yyyy",
    datetime_format="m/d/yyyy",
) as writer:
    inv_final.to_excel(writer, index=False, sheet_name="Sheet1")
    output_sheet = writer.sheets["Sheet1"]
    for row in output_sheet.iter_rows(
        min_row=2,
        max_row=output_sheet.max_row,
        min_col=7,
        max_col=7,
    ):
        row[0].number_format = "m/d/yyyy"

print(f"✅ Daily inventory file created: {FINAL_OUTPUT_FILE}")

upload_inventory_to_google_sheet(inv_final)