"""
drive_helper.py
Uses proper OAuth redirect flow — works with Web Application client type.
Redirect URI must match exactly what's registered in Google Cloud Console.
"""

import io
import os
import json
import requests as _requests

from google.oauth2.credentials     import Credentials
from google_auth_oauthlib.flow     import Flow
from googleapiclient.discovery     import build
from googleapiclient.http          import MediaIoBaseUpload

LOGIN_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/drive",
]

def _redirect_uri() -> str:
    """
    Streamlit is a single-page app — all redirects land on the root URL.
    The redirect URI used to GET the code must EXACTLY match the one used to EXCHANGE it.
    We store it in session state so both steps use the same value.
    """
    import streamlit as st
    # If already computed this session, reuse it
    if "oauth_redirect_uri" in st.session_state:
        return st.session_state["oauth_redirect_uri"]
    # Build from APP_URL secret/env, fallback to localhost
    app_url = os.environ.get("APP_URL", "http://localhost:8501").rstrip("/")
    st.session_state["oauth_redirect_uri"] = app_url
    return app_url


# ══════════════════════════════════════════════════════════════════════════════
# 1. LOGIN — proper redirect flow
# ══════════════════════════════════════════════════════════════════════════════

def get_login_url(client_secret_data: dict) -> tuple:
    """
    Returns (auth_url, state) — redirect user to auth_url.
    State is saved in session to verify on callback.
    """
    flow = Flow.from_client_config(
        client_secret_data,
        scopes=LOGIN_SCOPES,
        redirect_uri=_redirect_uri(),
    )
    auth_url, state = flow.authorization_url(
        access_type="offline",
        prompt="consent",
        include_granted_scopes="true",
    )
    return auth_url, state


def verify_login(client_secret_data: dict, auth_code: str) -> tuple:
    """
    Exchanges the auth code for credentials.
    Returns (email, credentials).
    """
    flow = Flow.from_client_config(
        client_secret_data,
        scopes=LOGIN_SCOPES,
        redirect_uri=_redirect_uri(),
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