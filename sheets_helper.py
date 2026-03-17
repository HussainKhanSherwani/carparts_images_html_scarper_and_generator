"""
sheets_helper.py
Google Sheets read/write via service account.
Also handles downloading Drive images via OAuth credentials.
"""

import io
import re
import json
import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials

SHEET_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SA_SCOPES    = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# Column names (exact, as they appear in the sheet header row)
COL_TIMESTAMP    = "Timestamp"
COL_EMAIL        = "Email Address"
COL_EMPLOYEE     = "Employee Name"
COL_IMAGE_TYPE   = "Image Type"
COL_DEPARTMENT   = "Department"
COL_SKU          = "NES's Custom Label / SKU"
COL_EBAY_LINK    = "Source Link ( Only Carpartwholesale's eBay )"
COL_FITMENT_LINK = "Fitment Link (Carpartwholesale, ETC)"
COL_HTML_STORE   = "HTML Store Name"
COL_HTML_LINK    = "HTML Link (Carpartwholesale, ETC)"
COL_VEHICLE_IMG  = "Add Vehicle Image"
COL_PRODUCT_IMG  = "Add Product Image"
COL_STATUS       = "CGI Status"
STATUS_DONE      = "completed"


def _sa_sheets_creds():
    """Service account credentials scoped for Sheets + Drive read."""
    raw = os.environ.get("SA_CREDENTIALS", "")
    if not raw:
        raise RuntimeError("SA_CREDENTIALS not set in environment.")
    info  = json.loads(raw)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SA_SCOPES)
    return creds


def fetch_pending_rows(sheet_id: str) -> list[dict]:
    """
    Read the sheet, return rows where Status != 'completed'.
    Each dict has: row_index (1-based, including header), ebay_link, vehicle_img_url.
    """
    creds  = _sa_sheets_creds()
    svc    = build("sheets", "v4", credentials=creds)
    result = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range="'Form Responses 1'!A1:Z",
    ).execute()

    rows   = result.get("values", [])
    if not rows:
        return []

    header = rows[0]

    def col(row, name):
        try:
            idx = header.index(name)
            return row[idx].strip() if idx < len(row) else ""
        except ValueError:
            return ""

    pending = []
    for i, row in enumerate(rows[1:], start=2):  # start=2 because row 1 is header
        status = col(row, COL_STATUS).lower()
        if status == STATUS_DONE:
            continue
        ebay_link   = col(row, COL_EBAY_LINK)
        vehicle_img = col(row, COL_VEHICLE_IMG)
        if not ebay_link:
            continue
        pending.append({
            "row_index":    i,
            "timestamp":    col(row, COL_TIMESTAMP),
            "email":        col(row, COL_EMAIL),
            "employee":     col(row, COL_EMPLOYEE),
            "image_type":   col(row, COL_IMAGE_TYPE),
            "department":   col(row, COL_DEPARTMENT),
            "sku":          col(row, COL_SKU),
            "ebay_link":    col(row, COL_EBAY_LINK),
            "fitment_link": col(row, COL_FITMENT_LINK),
            "html_store":   col(row, COL_HTML_STORE),
            "html_link":    col(row, COL_HTML_LINK),
            "vehicle_img":  col(row, COL_VEHICLE_IMG),
            "product_img":  col(row, COL_PRODUCT_IMG),
        })

    return pending


def mark_row_completed(sheet_id: str, row_index: int) -> None:
    """Write 'completed' to the Status column for the given row."""
    creds = _sa_sheets_creds()
    svc   = build("sheets", "v4", credentials=creds)

    # Find Status column letter
    result = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id, range="'Form Responses 1'!1:1"
    ).execute()
    header = result.get("values", [[]])[0]
    try:
        col_idx  = header.index(COL_STATUS)
        col_letter = _col_letter(col_idx + 1)
    except ValueError:
        # Status column doesn't exist — append it at the end
        col_idx    = len(header)
        col_letter = _col_letter(col_idx + 1)
        # Write header first if this is a new column
        svc.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f"'Form Responses 1'!{col_letter}1",
            valueInputOption="RAW",
            body={"values": [["CGI Status"]]},
        ).execute()

    svc.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"'Form Responses 1'!{col_letter}{row_index}",
        valueInputOption="RAW",
        body={"values": [[STATUS_DONE]]},
    ).execute()


def _col_letter(n: int) -> str:
    """Convert 1-based column index to letter (1→A, 27→AA)."""
    result = ""
    while n:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def extract_drive_file_id(url: str) -> str | None:
    """Extract file ID from any Google Drive URL format."""
    patterns = [
        r"/file/d/([a-zA-Z0-9_-]+)",
        r"id=([a-zA-Z0-9_-]+)",
        r"/open\?id=([a-zA-Z0-9_-]+)",
    ]
    for pat in patterns:
        m = re.search(pat, url)
        if m:
            return m.group(1)
    return None


def download_drive_image(oauth_creds: Credentials, url: str) -> bytes | None:
    """
    Download an image from a Google Drive view link using OAuth credentials.
    Returns raw bytes or None on failure.
    """
    file_id = extract_drive_file_id(url)
    if not file_id:
        return None
    try:
        svc     = build("drive", "v3", credentials=oauth_creds)
        request = svc.files().get_media(fileId=file_id, supportsAllDrives=True)
        buf     = io.BytesIO()
        dl      = MediaIoBaseDownload(buf, request)
        done    = False
        while not done:
            _, done = dl.next_chunk()
        return buf.getvalue()
    except Exception:
        return None