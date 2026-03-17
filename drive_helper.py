"""
drive_helper.py
Manual OAuth token exchange — no PKCE.
Also supports Service Account for user Drive uploads.
"""

import io
import os
import json
import requests as _requests
from google.oauth2.credentials        import Credentials
from google.oauth2                    import service_account
from googleapiclient.discovery        import build
from googleapiclient.http             import MediaIoBaseUpload

LOGIN_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/drive",
]
SA_SCOPES = ["https://www.googleapis.com/auth/drive"]
SCOPE_STR = " ".join(LOGIN_SCOPES)


def _redirect_uri() -> str:
    return os.environ.get("APP_URL", "http://localhost:8501").rstrip("/")


# ══════════════════════════════════════════════════════════════════════════════
# 1. LOGIN — OAuth (own Drive)
# ══════════════════════════════════════════════════════════════════════════════

def get_login_url(client_secret_data: dict) -> str:
    import urllib.parse
    cfg      = client_secret_data.get("web", client_secret_data)
    params   = {
        "client_id":     cfg["client_id"],
        "redirect_uri":  _redirect_uri(),
        "response_type": "code",
        "scope":         SCOPE_STR,
        "access_type":   "offline",
        "prompt":        "consent",
    }
    return f"{cfg.get('auth_uri','https://accounts.google.com/o/oauth2/auth')}?{urllib.parse.urlencode(params)}"


def verify_login(client_secret_data: dict, auth_code: str) -> tuple:
    cfg           = client_secret_data.get("web", client_secret_data)
    client_id     = cfg["client_id"]
    client_secret = cfg["client_secret"]
    token_uri     = cfg.get("token_uri", "https://oauth2.googleapis.com/token")

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

    user_resp = _requests.get(
        "https://www.googleapis.com/oauth2/v3/userinfo",
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
        timeout=10,
    )
    if user_resp.status_code != 200:
        raise ValueError(f"Could not fetch user info: {user_resp.text}")
    email = user_resp.json().get("email", "").lower()

    creds = Credentials(
        token=tokens["access_token"], refresh_token=tokens.get("refresh_token"),
        token_uri=token_uri, client_id=client_id, client_secret=client_secret,
        scopes=LOGIN_SCOPES,
    )
    return email, creds


# ══════════════════════════════════════════════════════════════════════════════
# 2. SERVICE ACCOUNT (user's Drive)
# ══════════════════════════════════════════════════════════════════════════════

def get_sa_credentials() -> service_account.Credentials | None:
    """Load SA credentials from env / secrets."""
    raw = os.environ.get("SA_CREDENTIALS", "")
    if not raw:
        return None
    try:
        info  = json.loads(raw)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SA_SCOPES)
        return creds
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# 3. DRIVE OPERATIONS  (work with both OAuth and SA creds)
# ══════════════════════════════════════════════════════════════════════════════

def _svc(creds):
    return build("drive", "v3", credentials=creds)


def get_or_create_folder(creds, name: str, parent_id: str) -> str:
    svc   = _svc(creds)
    safe  = name.replace("'", "\\'")
    query = (
        f"mimeType='application/vnd.google-apps.folder'"
        f" and name='{safe}' and '{parent_id}' in parents and trashed=false"
    )
    hits = svc.files().list(
        q=query, fields="files(id)",
        includeItemsFromAllDrives=True, supportsAllDrives=True,
    ).execute().get("files", [])
    if hits:
        return hits[0]["id"]
    meta   = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    folder = svc.files().create(body=meta, fields="id", supportsAllDrives=True).execute()
    return folder["id"]


def delete_existing_files(creds, folder_id: str, filename: str) -> int:
    """
    Delete ALL files with the given name in the folder, regardless of owner.
    Returns count of successfully deleted files.
    Google Drive allows duplicate filenames — we remove every copy before re-uploading.
    """
    svc  = _svc(creds)
    safe = filename.replace("'", "\\'")

    hits = svc.files().list(
        q=f"name='{safe}' and '{folder_id}' in parents and trashed=false",
        fields="files(id)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        corpora="allDrives",
    ).execute().get("files", [])

    deleted = 0
    for f in hits:
        # Try hard delete first
        try:
            svc.files().delete(fileId=f["id"], supportsAllDrives=True).execute()
            deleted += 1
            continue
        except Exception:
            pass
        # Fall back to trash if delete is denied (e.g. owned by another user)
        try:
            svc.files().update(
                fileId=f["id"],
                body={"trashed": True},
                supportsAllDrives=True,
            ).execute()
            deleted += 1
        except Exception:
            pass  # Can't remove — will result in duplicate for this file

    return deleted


def upload_file(creds, folder_id: str, filename: str, content: bytes, mimetype: str,
                deduplicate: bool = True) -> str:
    """Upload file, deleting ALL existing copies with the same name first."""
    if deduplicate:
        delete_existing_files(creds, folder_id, filename)
    svc   = _svc(creds)
    fh    = io.BytesIO(content)
    media = MediaIoBaseUpload(fh, mimetype=mimetype, resumable=False)
    meta  = {"name": filename, "parents": [folder_id]}
    f     = svc.files().create(
        body=meta, media_body=media, fields="id", supportsAllDrives=True,
    ).execute()
    return f["id"]


def find_file_in_folder(creds, folder_id: str, filename: str) -> str | None:
    """Return file ID if a file with this exact name exists in folder, else None."""
    svc  = _svc(creds)
    safe = filename.replace("'", "\'")
    hits = svc.files().list(
        q=f"name='{safe}' and '{folder_id}' in parents and trashed=false",
        fields="files(id)",
        includeItemsFromAllDrives=True, supportsAllDrives=True,
        corpora="allDrives",
    ).execute().get("files", [])
    return hits[0]["id"] if hits else None


def download_file_bytes(creds, file_id: str) -> bytes | None:
    """Download any Drive file by ID, return raw bytes."""
    svc = _svc(creds)
    try:
        request = svc.files().get_media(fileId=file_id, supportsAllDrives=True)
        buf = io.BytesIO()
        from googleapiclient.http import MediaIoBaseDownload
        dl = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = dl.next_chunk()
        return buf.getvalue()
    except Exception:
        return None


def download_latest_template(creds, parent_id: str) -> bytes | None:
    """
    Download _LATEST_TEMPLATE.psd (or .psb) from the parent folder.
    Returns file bytes, or None if not found.
    """
    svc  = _svc(creds)
    hits = svc.files().list(
        q=f"name='_LATEST_TEMPLATE.psd' and '{parent_id}' in parents and trashed=false",
        fields="files(id,name,size)",
        includeItemsFromAllDrives=True, supportsAllDrives=True,
        corpora="allDrives",
    ).execute().get("files", [])
    # Also check .psb
    if not hits:
        hits = svc.files().list(
            q=f"name='_LATEST_TEMPLATE.psb' and '{parent_id}' in parents and trashed=false",
            fields="files(id,name,size)",
            includeItemsFromAllDrives=True, supportsAllDrives=True,
            corpora="allDrives",
        ).execute().get("files", [])
    if not hits:
        return None
    file_id = hits[0]["id"]
    request = svc.files().get_media(fileId=file_id, supportsAllDrives=True)
    buf = io.BytesIO()
    from googleapiclient.http import MediaIoBaseDownload
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()


def list_subfolders(creds, parent_id: str) -> list[dict]:
    """List all subfolders in parent. Returns [{"id":..., "name":...}, ...]"""
    svc  = _svc(creds)
    hits = svc.files().list(
        q=f"mimeType='application/vnd.google-apps.folder' and '{parent_id}' in parents and trashed=false",
        fields="files(id,name)",
        includeItemsFromAllDrives=True, supportsAllDrives=True,
        pageSize=500,
    ).execute().get("files", [])
    return hits


def update_template_in_all_folders(creds, parent_id: str, template_bytes: bytes,
                                   mimetype: str) -> tuple[int, int]:
    """
    Upload/replace template PSD in every subfolder of parent_id.
    Template file is named after the folder.
    Returns (success_count, error_count).
    """
    folders = list_subfolders(creds, parent_id)
    ok = err = 0
    for folder in folders:
        try:
            fname = f"{folder['name']}.psd"
            upload_file(creds, folder["id"], fname, template_bytes, mimetype, deduplicate=True)
            ok += 1
        except Exception:
            err += 1
    return ok, err