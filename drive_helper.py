"""
drive_helper.py
Manual OAuth token exchange — bypasses google_auth_oauthlib's PKCE entirely.
"""

import io
import os
import json
import requests as _requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http      import MediaIoBaseUpload

LOGIN_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/drive",
]
SCOPE_STR = " ".join(LOGIN_SCOPES)


def _redirect_uri() -> str:
    return os.environ.get("APP_URL", "http://localhost:8501").rstrip("/")


# ══════════════════════════════════════════════════════════════════════════════
# 1. LOGIN — build URL manually, no Flow object, no PKCE
# ══════════════════════════════════════════════════════════════════════════════

def get_login_url(client_secret_data: dict) -> str:
    """Builds Google OAuth URL manually — zero PKCE, no Flow state needed."""
    import urllib.parse
    cfg       = client_secret_data.get("web", client_secret_data)
    client_id = cfg["client_id"]
    auth_uri  = cfg.get("auth_uri", "https://accounts.google.com/o/oauth2/auth")

    params = {
        "client_id":     client_id,
        "redirect_uri":  _redirect_uri(),
        "response_type": "code",
        "scope":         SCOPE_STR,
        "access_type":   "offline",
        "prompt":        "consent",
    }
    return f"{auth_uri}?{urllib.parse.urlencode(params)}"


def verify_login(client_secret_data: dict, auth_code: str) -> tuple:
    """
    Exchanges auth code for tokens via direct HTTP POST — no Flow, no PKCE.
    Returns (email, google.oauth2.credentials.Credentials).
    """
    cfg           = client_secret_data.get("web", client_secret_data)
    client_id     = cfg["client_id"]
    client_secret = cfg["client_secret"]
    token_uri     = cfg.get("token_uri", "https://oauth2.googleapis.com/token")

    # Manual token exchange
    resp = _requests.post(token_uri, data={
        "code":          auth_code.strip(),
        "client_id":     client_id,
        "client_secret": client_secret,
        "redirect_uri":  _redirect_uri(),
        "grant_type":    "authorization_code",
    }, timeout=15)

    if resp.status_code != 200:
        raise ValueError(f"Token exchange failed: {resp.text}")

    tokens = resp.json()

    # Fetch user email
    user_resp = _requests.get(
        "https://www.googleapis.com/oauth2/v3/userinfo",
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
        timeout=10,
    )
    if user_resp.status_code != 200:
        raise ValueError(f"Could not fetch user info: {user_resp.text}")
    email = user_resp.json().get("email", "").lower()

    # Build Credentials object for Drive API
    creds = Credentials(
        token         = tokens["access_token"],
        refresh_token = tokens.get("refresh_token"),
        token_uri     = token_uri,
        client_id     = client_id,
        client_secret = client_secret,
        scopes        = LOGIN_SCOPES,
    )
    return email, creds


# ══════════════════════════════════════════════════════════════════════════════
# 2. DRIVE OPERATIONS
# ══════════════════════════════════════════════════════════════════════════════

def _svc(creds: Credentials):
    return build("drive", "v3", credentials=creds)


def get_or_create_folder(creds: Credentials, name: str, parent_id: str) -> str:
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
    folder = svc.files().create(body=meta, fields="id", supportsAllDrives=True).execute()
    return folder["id"]


def upload_file(creds: Credentials, folder_id: str, filename: str, content: bytes, mimetype: str) -> str:
    svc   = _svc(creds)
    fh    = io.BytesIO(content)
    media = MediaIoBaseUpload(fh, mimetype=mimetype, resumable=True)
    meta  = {"name": filename, "parents": [folder_id]}
    f     = svc.files().create(
        body=meta, media_body=media, fields="id", supportsAllDrives=True,
    ).execute()
    return f["id"]