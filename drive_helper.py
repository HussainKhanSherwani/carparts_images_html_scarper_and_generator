"""
drive_helper.py
─────────────────────────────────────────────────────────────────────────────
Architecture:
  • User logs in with Google OAuth — identity + Drive scope.
  • ALL Drive operations use the logged-in USER's credentials.
  • Files go to DRIVE_FOLDER_ID which must be shared with the user (Editor).
  • No service account involved in uploads at all.
─────────────────────────────────────────────────────────────────────────────
"""

import io
import json
import requests as _requests

from google.oauth2.credentials     import Credentials
from google_auth_oauthlib.flow     import Flow
from googleapiclient.discovery     import build
from googleapiclient.http          import MediaIoBaseUpload

# Login scope includes Drive so the user's token can upload
LOGIN_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/drive",
]

REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob"


# ══════════════════════════════════════════════════════════════════════════════
# 1. LOGIN
# ══════════════════════════════════════════════════════════════════════════════

def get_login_url(client_secret_data: dict) -> str:
    flow = Flow.from_client_config(
        client_secret_data, scopes=LOGIN_SCOPES, redirect_uri=REDIRECT_URI,
    )
    url, _ = flow.authorization_url(access_type="offline", prompt="consent")
    return url


def verify_login(client_secret_data: dict, auth_code: str) -> tuple:
    """Returns (email, credentials)."""
    flow = Flow.from_client_config(
        client_secret_data, scopes=LOGIN_SCOPES, redirect_uri=REDIRECT_URI,
    )
    flow.fetch_token(code=auth_code.strip())
    creds = flow.credentials

    resp = _requests.get(
        "https://www.googleapis.com/oauth2/v3/userinfo",
        headers={"Authorization": f"Bearer {creds.token}"},
        timeout=10,
    )
    if resp.status_code != 200:
        raise ValueError(f"Could not fetch user info: {resp.text}")

    email = resp.json().get("email", "").lower()
    return email, creds


# ══════════════════════════════════════════════════════════════════════════════
# 2. DRIVE OPERATIONS — all use the logged-in user's credentials
# ══════════════════════════════════════════════════════════════════════════════

def _svc(creds: Credentials):
    return build("drive", "v3", credentials=creds)


def get_or_create_folder(creds: Credentials, name: str, parent_id: str) -> str:
    """Finds or creates a subfolder inside parent_id. Returns folder ID."""
    svc   = _svc(creds)
    query = (
        f"mimeType='application/vnd.google-apps.folder'"
        f" and name='{name}' and '{parent_id}' in parents and trashed=false"
    )
    hits = svc.files().list(
        q=query, fields="files(id)", 
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute().get("files", [])

    if hits:
        return hits[0]["id"]

    meta   = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    folder = svc.files().create(
        body=meta, fields="id", supportsAllDrives=True,
    ).execute()
    return folder["id"]


def upload_file(creds: Credentials, folder_id: str, filename: str, content: bytes, mimetype: str) -> str:
    """Uploads bytes into folder_id using user credentials. Returns file ID."""
    svc   = _svc(creds)
    fh    = io.BytesIO(content)
    media = MediaIoBaseUpload(fh, mimetype=mimetype, resumable=True)
    meta  = {"name": filename, "parents": [folder_id]}
    f     = svc.files().create(
        body=meta, media_body=media, fields="id", supportsAllDrives=True,
    ).execute()
    return f["id"]