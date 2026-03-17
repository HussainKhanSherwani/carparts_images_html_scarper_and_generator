import streamlit as st
import json
import io
import time
import zipfile
import pandas as pd
import hashlib
import os
from datetime import datetime, timedelta
from streamlit_cookies_manager import EncryptedCookieManager
from drive_helper import (
    get_login_url, verify_login,
    get_or_create_folder, upload_file, get_sa_credentials,
    list_subfolders, update_template_in_all_folders, download_latest_template,
    find_file_in_folder, download_file_bytes,
)
from googleapiclient.discovery import build
from sheets_helper import fetch_pending_rows, mark_row_completed, download_drive_image
from PIL import Image, ImageOps
from google import genai
from google.genai import types
from scraper import (
    scrape_ebay_item, merge_all_data,
    extract_text_data, extract_item_number, parse_links,
)

# ── Cookie manager (persistent login for 7 days) ─────────────────────────────
cookies = EncryptedCookieManager(
    prefix="ebay_gen_",
    password=os.environ.get("COOKIE_SECRET", "ebay-listing-secret-key-2024"),
)
if not cookies.ready():
    st.stop()

st.set_page_config(
    page_title="eBay Listing Generator",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@400;600;700;800&display=swap');
html, body, [class*="css"] { font-family: 'Syne', sans-serif; }
.stApp { background: #0f0f0f; color: #e8e3d8; }
.main .block-container { padding-top: 2rem; max-width: 1100px; }
section[data-testid="stSidebar"] { background: #141414; border-right: 1px solid #2a2a2a; }

.app-header { padding: 20px 0 28px 0; border-bottom: 1px solid #2a2a2a; margin-bottom: 28px; }
.app-header h1 { font-weight: 800; font-size: 2rem; color: #f0c040; margin: 0; letter-spacing: -1px; }
.app-header p  { color: #555; font-size: 0.8rem; font-family: 'DM Mono', monospace; margin: 6px 0 0 0; }

.step-label {
    font-family: 'DM Mono', monospace; font-size: 0.68rem; color: #f0c040;
    letter-spacing: 2px; text-transform: uppercase; margin-bottom: 8px; margin-top: 4px;
}

.badge { display:inline-block; padding:3px 10px; border-radius:20px; font-size:0.7rem; font-family:'DM Mono',monospace; }
.badge-ok    { background:#1a3a1a; color:#4caf50; border:1px solid #2d5a2d; }
.badge-warn  { background:#3a2a00; color:#f0c040; border:1px solid #5a4400; }
.badge-error { background:#3a1a1a; color:#f44336; border:1px solid #5a2020; }

.stButton > button {
    background:#f0c040 !important; color:#0f0f0f !important;
    font-family:'Syne',sans-serif !important; font-weight:700 !important;
    border:none !important; border-radius:6px !important;
    padding:10px 28px !important; font-size:0.9rem !important; transition:all 0.2s !important;
}
.stButton > button:hover { background:#ffd860 !important; transform:translateY(-1px); }

/* login button — blue */
.login-btn > button { background:#4285f4 !important; color:#fff !important; }
.login-btn > button:hover { background:#5a95f5 !important; }

.stTextInput > div > div > input,
.stTextArea  > div > div > textarea {
    background:#1a1a1a !important; border:1px solid #2a2a2a !important;
    color:#e8e3d8 !important; border-radius:6px !important;
    font-family:'DM Mono',monospace !important; font-size:0.85rem !important;
}
.stTextInput > div > div > input:focus,
.stTextArea  > div > div > textarea:focus {
    border-color:#f0c040 !important; box-shadow:0 0 0 2px rgba(240,192,64,0.15) !important;
}
.stFileUploader > div { background:#1a1a1a !important; border:1px dashed #333 !important; border-radius:8px !important; }
.stProgress > div > div > div > div { background:#f0c040 !important; }

.log-box {
    background:#0a0a0a; border:1px solid #2a2a2a; border-radius:6px;
    padding:16px; font-family:'DM Mono',monospace; font-size:0.78rem;
    color:#888; max-height:320px; overflow-y:auto; line-height:1.9;
}
.log-ok   { color:#4caf50; }
.log-warn { color:#f0c040; }
.log-err  { color:#f44336; }
.log-info { color:#64b5f6; }

.result-card {
    background:#1a1a1a; border:1px solid #2a2a2a; border-radius:8px;
    padding:14px 18px; margin-bottom:10px;
}
.result-card .item-id { font-family:'DM Mono',monospace; font-size:0.75rem; color:#f0c040; }

.login-card {
    background:#1a1a1a; border:1px solid #2a2a2a; border-radius:12px;
    padding:40px; text-align:center; max-width:480px; margin:80px auto;
}
.login-card h2 { color:#f0c040; font-size:1.5rem; margin-bottom:8px; }
.login-card p  { color:#666; font-size:0.85rem; font-family:'DM Mono',monospace; margin-bottom:28px; }

.user-chip {
    display:inline-flex; align-items:center; gap:8px;
    background:#1a1a1a; border:1px solid #2a2a2a; border-radius:20px;
    padding:6px 14px; font-family:'DM Mono',monospace; font-size:0.75rem; color:#aaa;
}
.user-chip .dot { width:8px; height:8px; border-radius:50%; background:#4caf50; display:inline-block; }

hr { border-color:#2a2a2a !important; }
#MainMenu, footer, header { visibility:hidden; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG — edit these for your deployment
# ══════════════════════════════════════════════════════════════════════════════
# Allowed emails — only these can use the app after signing in
ALLOWED_EMAILS = {
    "hussainkhansherwani09@gmail.com",
    "ghulamhussainsherwani@gmail.com",
    # add more as needed
}

# ScrapingAnt key — baked in, user never sees it
SCRAPING_ANT_KEY = ""   # set via env var or st.secrets in production

# ══════════════════════════════════════════════════════════════════════════════
# SESSION STATE
# ══════════════════════════════════════════════════════════════════════════════
for k, v in {
    "user_email":     None,
    "drive_creds":    None,
    "psd_template":   None,    # bytes of latest PSD template
    "psd_filename":   None,    # original filename of uploaded PSD
    "psd_source":     None,    # "upload" or "drive"
    "logs":           [],
    "output_zip":     None,
    "results":        [],
    "pending_rows":   [],
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Restore login from cookie (valid for 7 days) ──────────────────────────────
if st.session_state.user_email is None:
    _cookie_email   = cookies.get("user_email", "")
    _cookie_expiry  = cookies.get("login_expiry", "")
    _cookie_creds   = cookies.get("drive_creds", "")
    if _cookie_email and _cookie_expiry:
        try:
            _expiry_dt = datetime.fromisoformat(_cookie_expiry)
            if datetime.utcnow() < _expiry_dt and _cookie_email in ALLOWED_EMAILS:
                st.session_state.user_email = _cookie_email
                # Restore Drive credentials from cookie
                if _cookie_creds and st.session_state.drive_creds is None:
                    try:
                        from google.oauth2.credentials import Credentials as _Creds
                        _cd = json.loads(_cookie_creds)
                        st.session_state.drive_creds = _Creds(
                            token         = _cd.get("token"),
                            refresh_token = _cd.get("refresh_token"),
                            token_uri     = _cd.get("token_uri"),
                            client_id     = _cd.get("client_id"),
                            client_secret = _cd.get("client_secret"),
                            scopes        = _cd.get("scopes", []),
                        )
                    except Exception:
                        pass  # creds corrupt — user will need to re-login once
        except Exception:
            pass


def add_log(msg: str, level: str = "info"):
    css  = {"ok":"log-ok","warn":"log-warn","error":"log-err","info":"log-info"}.get(level,"log-info")
    icon = {"ok":"✅","warn":"⚠️","error":"❌","info":"→"}.get(level,"→")
    st.session_state.logs.append(f'<span class="{css}">{icon} {msg}</span>')


# ══════════════════════════════════════════════════════════════════════════════
# LOAD SECRETS  (Streamlit Cloud secrets.toml or env vars)
# ══════════════════════════════════════════════════════════════════════════════
import os

def _get_secret(key: str, fallback: str = "") -> str:
    try:
        return st.secrets[key]
    except Exception:
        return os.environ.get(key, fallback)

ANT_KEY         = _get_secret("SCRAPING_ANT_KEY", SCRAPING_ANT_KEY)
DRIVE_FOLDER_ID = _get_secret("DRIVE_FOLDER_ID", "")
SHEET_ID        = _get_secret("SHEET_ID", "")
GEMINI_KEY      = _get_secret("GEMINI_KEY", "")
_emails_raw     = _get_secret("ALLOWED_EMAILS", "hussainkhansherwani09@gmail.com,ghulamhussainsherwani@gmail.com")
ALLOWED_EMAILS  = {e.strip().lower() for e in _emails_raw.split(",") if e.strip()}
_CS_RAW         = _get_secret("CLIENT_SECRET", "")
CLIENT_SECRET   = json.loads(_CS_RAW) if _CS_RAW else None

# Push secrets into env so drive_helper.py can read them
_SA_RAW  = _get_secret("SA_CREDENTIALS", "")
_APP_URL = _get_secret("APP_URL", "") or "http://localhost:8501"
if _SA_RAW:        os.environ["SA_CREDENTIALS"]  = _SA_RAW
if DRIVE_FOLDER_ID: os.environ["DRIVE_FOLDER_ID"] = DRIVE_FOLDER_ID
if _APP_URL:       os.environ["APP_URL"]          = _APP_URL.rstrip("/")

# ── Auto-load template.html from disk ─────────────────────────────────────────
if "template_content" not in st.session_state:
    for _tpath in ["template.html", os.path.join(os.getcwd(), "template.html")]:
        if os.path.exists(_tpath):
            with open(_tpath, "r", encoding="utf-8") as _f:
                st.session_state["template_content"] = _f.read()
            break

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR — dev fallbacks only
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown('<div class="step-label">⚙ Config</div>', unsafe_allow_html=True)

    # ── Dev fallbacks ─────────────────────────────────────────────────────
    st.caption("Only needed if secrets.toml is not configured.")
    if not CLIENT_SECRET:
        cs_upload = st.file_uploader("client_secret.json", type=["json"])
        if cs_upload:
            CLIENT_SECRET = json.loads(cs_upload.read())
            st.success("client_secret loaded")
    if not ANT_KEY:
        ANT_KEY = st.text_input("ScrapingAnt API Key", type="password")
    if not DRIVE_FOLDER_ID:
        DRIVE_FOLDER_ID = st.text_input("Drive Folder ID", placeholder="1C-pzZz...")

    # ── PSD Template ──────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown('<div class="step-label">🎨 Photoshop Template</div>', unsafe_allow_html=True)
    _psd_up = st.file_uploader("Upload latest .psd / .psb", type=["psd", "psb"], key="psd_uploader")
    if _psd_up:
        # Only process on first upload — skip re-runs caused by Streamlit rerenders
        _psd_hash = hashlib.md5(_psd_up.getvalue()).hexdigest()
        if _psd_hash != st.session_state.get("psd_hash"):
            st.session_state.psd_template = _psd_up.getvalue()
            st.session_state.psd_filename = _psd_up.name
            st.session_state.psd_source   = "upload"
            st.session_state["psd_hash"]  = _psd_hash
            st.success(f"Template loaded: {_psd_up.name}")

    if st.session_state.psd_template:
        st.caption(f"{st.session_state.psd_filename} ({len(st.session_state.psd_template)//1024} KB)")
        if st.button("🔄 Update Template in All Drive Folders", key="update_tpl_btn"):
            _active_creds = st.session_state.drive_creds
            _tpl_root = DRIVE_FOLDER_ID
            if not _active_creds:
                st.error("No Drive credentials — sign in first.")
            elif not _tpl_root:
                st.error("Drive Folder ID not set.")
            else:
                with st.spinner("Saving to Drive and updating all folders..."):
                    # 1. Save as _LATEST_TEMPLATE in parent folder
                    try:
                        upload_file(_active_creds, _tpl_root,
                                    "_LATEST_TEMPLATE.psd",
                                    st.session_state.psd_template,
                                    "image/vnd.adobe.photoshop", deduplicate=True)
                    except Exception as _e:
                        st.warning(f"Could not save _LATEST_TEMPLATE: {_e}")
                    # 2. Push renamed copy to every item subfolder
                    _folders = list_subfolders(_active_creds, _tpl_root)
                    if not _folders:
                        st.warning(f"No subfolders found in `{_tpl_root}`.")
                    else:
                        ok, err = update_template_in_all_folders(
                            _active_creds, _tpl_root,
                            st.session_state.psd_template, "image/vnd.adobe.photoshop"
                        )
                        st.success(f"Saved to Drive + updated {ok} of {len(_folders)} folder(s)." + (f" {err} error(s)." if err else ""))

    # ── Clean duplicates ─────────────────────────────────────────────────
    st.markdown("---")
    st.markdown('<div class="step-label">🧹 Maintenance</div>', unsafe_allow_html=True)
    if st.button("🧹 Remove Duplicate Files in Drive", key="clean_dupes_btn"):
        _clean_creds = st.session_state.drive_creds
        _clean_root  = DRIVE_FOLDER_ID
        if not _clean_creds:
            st.error("Sign in first.")
        elif not _clean_root:
            st.error("Set Drive Folder ID first.")
        else:
            with st.spinner("Scanning for duplicates..."):
                _svc_obj = build("drive", "v3", credentials=_clean_creds)
                _folders = list_subfolders(_clean_creds, _clean_root)
                _total_removed = 0
                for _fold in _folders:
                    # Get all files in this folder
                    _files = _svc_obj.files().list(
                        q=f"'{_fold['id']}' in parents and mimeType!='application/vnd.google-apps.folder' and trashed=false",
                        fields="files(id,name)",
                        includeItemsFromAllDrives=True, supportsAllDrives=True,
                        corpora="allDrives", pageSize=500,
                    ).execute().get("files", [])
                    # Group by name — keep newest, delete rest
                    from collections import defaultdict
                    _by_name = defaultdict(list)
                    for _fi in _files:
                        _by_name[_fi["name"]].append(_fi["id"])
                    for _name, _ids in _by_name.items():
                        if len(_ids) > 1:
                            # Delete all but the last one
                            for _dup_id in _ids[:-1]:
                                try:
                                    _svc_obj.files().delete(fileId=_dup_id, supportsAllDrives=True).execute()
                                    _total_removed += 1
                                except Exception:
                                    try:
                                        _svc_obj.files().update(fileId=_dup_id, body={"trashed": True}, supportsAllDrives=True).execute()
                                        _total_removed += 1
                                    except Exception:
                                        pass
            st.success(f"Removed {_total_removed} duplicate file(s) across {len(_folders)} folder(s).")

    # ── Sign out ──────────────────────────────────────────────────────────
    if st.session_state.user_email:
        st.markdown("---")
        st.markdown(f'<div class="user-chip"><span class="dot"></span>{st.session_state.user_email}</div>', unsafe_allow_html=True)
        if st.button("Sign out", key="sidebar_signout"):
            st.session_state.user_email  = None
            st.session_state.drive_creds = None
            cookies["user_email"]   = ""
            cookies["login_expiry"] = ""
            cookies["drive_creds"]  = ""
            cookies.save()
            st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# LOGIN GATE
# ══════════════════════════════════════════════════════════════════════════════
if not st.session_state.user_email:
    st.markdown("""
    <div class="login-card">
        <h2>🛒 eBay Listing Generator</h2>
        <p>SIGN IN WITH GOOGLE TO CONTINUE</p>
    </div>
    """, unsafe_allow_html=True)

    if not CLIENT_SECRET:
        st.warning("⚠️ client_secret.json not configured. Upload it in the sidebar.")
        st.stop()

    
    # Check if Google redirected back with ?code= in URL
    _params     = st.query_params
    _auth_code  = _params.get("code", "")
    _auth_state = _params.get("state", "")

    if _auth_code:
        # Google redirected back — exchange code for credentials
        with st.spinner("Signing you in..."):
            try:
                email, drive_creds = verify_login(CLIENT_SECRET, _auth_code)
                st.query_params.clear()   # remove ?code= from URL
                if email in ALLOWED_EMAILS:
                    st.session_state.user_email  = email
                    st.session_state.drive_creds = drive_creds
                    _expiry = (datetime.utcnow() + timedelta(days=7)).isoformat()
                    # Serialize creds to cookie so they survive page refresh
                    _creds_data = json.dumps({
                        "token":         drive_creds.token,
                        "refresh_token": drive_creds.refresh_token,
                        "token_uri":     drive_creds.token_uri,
                        "client_id":     drive_creds.client_id,
                        "client_secret": drive_creds.client_secret,
                        "scopes":        list(drive_creds.scopes or []),
                    })
                    cookies["user_email"]   = email
                    cookies["login_expiry"] = _expiry
                    cookies["drive_creds"]  = _creds_data
                    cookies.save()
                    st.rerun()
                else:
                    st.error(f"❌ Access denied for {email}. Contact the admin to be added.")
                    st.stop()
            except Exception as e:
                st.error(f"Sign-in failed: {e}")
                st.stop()
    else:
        # Show sign-in button — redirect to Google
        auth_url = get_login_url(CLIENT_SECRET)
        col_l, col_c, col_r = st.columns([1, 2, 1])
        with col_c:
            st.markdown(
                f'''<div style="text-align:center;margin-top:-60px">
                    <a href="{auth_url}">
                        <button style="background:#4285f4;color:#fff;border:none;padding:14px 32px;
                        border-radius:8px;font-size:1rem;font-family:Syne,sans-serif;font-weight:700;
                        cursor:pointer;letter-spacing:0.5px;width:100%">
                            🔑 Sign in with Google
                        </button>
                    </a>
                </div>''',
                unsafe_allow_html=True,
            )
        st.stop()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN APP  (only reachable after successful login)
# ══════════════════════════════════════════════════════════════════════════════

# Header
st.markdown(f"""
<div class="app-header">
  <h1>🛒 eBay Listing Generator</h1>
  <p>GOOGLE SHEET → AUTO-SCRAPE → HTML + IMAGES + TEXT EXPORT</p>
</div>
<div style="display:flex;justify-content:flex-end;align-items:center;gap:10px;margin-top:-20px;margin-bottom:16px">
  <div class="user-chip"><span class="dot"></span>{st.session_state.user_email}</div>
</div>
""", unsafe_allow_html=True)

_signout_col = st.columns([6, 1])[1]
with _signout_col:
    if st.button("Sign out", key="main_signout", use_container_width=True):
        st.session_state.user_email  = None
        st.session_state.drive_creds = None
        cookies["user_email"]   = ""
        cookies["login_expiry"] = ""
        cookies["drive_creds"]  = ""
        cookies.save()
        st.rerun()

# Status badges
c1, c2, c3, c4 = st.columns(4)
with c1:
    ok = "template_content" in st.session_state
    st.markdown(f'<span class="badge {"badge-ok" if ok else "badge-error"}">● Template {"Loaded" if ok else "Required — upload in sidebar"}</span>', unsafe_allow_html=True)
with c2:
    ok = bool(ANT_KEY)
    st.markdown(f'<span class="badge {"badge-ok" if ok else "badge-error"}">● ScrapingAnt {"Ready" if ok else "Key Missing"}</span>', unsafe_allow_html=True)
with c3:
    ok = bool(DRIVE_FOLDER_ID)
    st.markdown(f'<span class="badge {"badge-ok" if ok else "badge-warn"}">● Drive Folder {"Set" if ok else "Not Set (ZIP only)"}</span>', unsafe_allow_html=True)
with c4:
    ok = bool(SHEET_ID)
    st.markdown(f'<span class="badge {"badge-ok" if ok else "badge-error"}">● Sheet {"Connected" if ok else "SHEET_ID Missing"}</span>', unsafe_allow_html=True)

st.markdown("---")

# ── Sheet preview ─────────────────────────────────────────────────────────────
st.markdown('<div class="step-label">📊 Google Sheet Queue</div>', unsafe_allow_html=True)
_pending_rows = []
if not SHEET_ID:
    st.warning("⚠️ SHEET_ID not set in secrets.toml")
else:
    if st.button("🔄 Load Pending Rows from Sheet", key="load_sheet_btn"):
        with st.spinner("Fetching sheet..."):
            try:
                _pending_rows = fetch_pending_rows(SHEET_ID)
                st.session_state["pending_rows"] = _pending_rows
            except Exception as _se:
                st.error(f"Sheet error: {_se}")
    _pending_rows = st.session_state.get("pending_rows", [])
    if _pending_rows:
        st.markdown(f'<span class="badge badge-ok">● {len(_pending_rows)} pending row(s) ready to process</span>', unsafe_allow_html=True)
        _preview_df = pd.DataFrame([{
            "Row": r["row_index"],
            "eBay Link": r["ebay_link"][:60] + "..." if len(r["ebay_link"]) > 60 else r["ebay_link"],
            "Vehicle Image": "✅" if r["vehicle_img"] else "—",
        } for r in _pending_rows])
        st.dataframe(_preview_df, use_container_width=True, hide_index=True)
    elif SHEET_ID:
        st.caption("Click 'Load Pending Rows' to fetch from sheet.")

st.markdown("---")

# ── Output options ────────────────────────────────────────────────────────────
st.markdown('<div class="step-label">▶ Output Options</div>', unsafe_allow_html=True)
col_a, col_b, col_c, col_d = st.columns(4)
with col_a: gen_html   = st.checkbox("HTML Listing",    value=True)
with col_b: gen_images = st.checkbox("Cloudinary Images", value=True)
with col_c: gen_text   = st.checkbox("Text Export",     value=True)
with col_d: upload_drive = st.checkbox("Upload to Drive", value=bool(DRIVE_FOLDER_ID), disabled=not DRIVE_FOLDER_ID)

# ── Run button ────────────────────────────────────────────────────────────────
_pending_rows = st.session_state.get("pending_rows", [])
_missing = []
if not _pending_rows:                           _missing.append("sheet rows (click Load)")
if not ANT_KEY:                                 _missing.append("ScrapingAnt key")
if "template_content" not in st.session_state:  _missing.append("template.html")

if _missing:
    st.warning(f"⚠️ Still needed before generating: **{', '.join(_missing)}**")

if st.button("🚀 Generate Listings", disabled=bool(_missing)):
    ebay_links    = [r["ebay_link"] for r in _pending_rows]
    _row_map      = {r["ebay_link"]: r for r in _pending_rows}  # link → row data
    template = st.session_state["template_content"]

    # Auto-fetch PSD template from Drive if not already loaded in session
    if not st.session_state.psd_template and st.session_state.drive_creds and DRIVE_FOLDER_ID:
        with st.spinner("Fetching _LATEST_TEMPLATE.psd from Drive..."):
            _auto_tpl = download_latest_template(st.session_state.drive_creds, DRIVE_FOLDER_ID)
        if _auto_tpl:
            st.session_state.psd_template = _auto_tpl
            st.session_state.psd_filename = "_LATEST_TEMPLATE.psd"
            st.session_state.psd_source   = "drive"
            st.toast("Template loaded from Drive ✅")

    st.session_state.logs       = []
    st.session_state.results    = []
    st.session_state.output_zip = None

    total   = len(ebay_links)
    zip_buf = io.BytesIO()

    # ── Progress UI containers ────────────────────────────────────────────────
    st.markdown("---")
    prog_header   = st.empty()   # "Processing X / Y"
    progress_bar  = st.progress(0)
    stage_status  = st.empty()   # current stage pill
    item_cols     = st.columns([2, 1, 1, 1, 1, 1])
    results_live  = st.empty()   # live results table
    log_container = st.empty()   # scrolling log

    def update_ui(idx, item_id, stage, pct):
        prog_header.markdown(
            f'<div style="font-family:DM Mono,monospace;font-size:0.8rem;color:#aaa;margin-bottom:4px">' 
            f'Item <b style="color:#f0c040">{idx+1}</b> of <b style="color:#f0c040">{total}</b>'
            f' &nbsp;·&nbsp; <span style="color:#e8e3d8">{item_id}</span></div>',
            unsafe_allow_html=True,
        )
        progress_bar.progress(min(100, max(0, int(pct))))
        stage_status.markdown(
            f'<div style="margin:6px 0 10px 0">' 
            f'<span class="badge badge-warn">⚙ {stage}</span></div>',
            unsafe_allow_html=True,
        )
        log_html = "<br>".join(st.session_state.logs[-30:])
        log_container.markdown(f'<div class="log-box">{log_html}</div>', unsafe_allow_html=True)

    def done_ui():
        prog_header.markdown(
            f'<div style="font-family:DM Mono,monospace;font-size:0.8rem;color:#4caf50">' 
            f'✅ All {total} item(s) processed</div>',
            unsafe_allow_html=True,
        )
        progress_bar.progress(min(100, max(0, int(100))))
        stage_status.markdown(
            '<div style="margin:6px 0 10px 0"><span class="badge badge-ok">✓ Done</span></div>',
            unsafe_allow_html=True,
        )
        log_html = "<br>".join(st.session_state.logs[-50:])
        log_container.markdown(f'<div class="log-box">{log_html}</div>', unsafe_allow_html=True)

    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for idx, url in enumerate(ebay_links):
            base_pct = int(idx / total * 100)

            item_id = extract_item_number(url)
            if not item_id:
                add_log(f"Could not parse item ID: {url}", "error")
                continue

            add_log(f"[{idx+1}/{total}] ─── Item {item_id} ───", "info")

            # ── Stage 1: Scraping ─────────────────────────────────────────
            update_ui(idx, item_id, "Scraping eBay page...", base_pct + 2)
            scraped = scrape_ebay_item(url, ANT_KEY)

            # Log spans
            for _s in scraped.get("span_texts", []):
                add_log(f"Span: {_s}", "info")

            if scraped["hidden_sku"]:
                add_log(f"SKU: {scraped['hidden_sku']}", "ok")
                add_log(f"Cloudinary images: {len(scraped['cloud_images'])}", "ok" if scraped["cloud_images"] else "warn")
            else:
                add_log("SKU not found — Cloudinary skipped", "warn")

            _pln_log = scraped.get("part_link_number")
            if _pln_log:
                add_log(f"Part Link Number: {_pln_log}", "ok")
            else:
                add_log("Part Link Number not found — folder name will use SKU/seller only", "warn")

            _seller_log = scraped.get("seller_name")
            if _seller_log:
                add_log(f"Seller: {_seller_log}", "info")

            # Log the final folder name that will be used
            _pln_f  = scraped.get("part_link_number") or ""
            _sku_lf = scraped.get("hidden_sku") or ""
            _folder_preview = " ".join(p for p in [_pln_f, _sku_lf, item_id] if p)
            add_log(f"Drive folder name: {_folder_preview}", "info")

            gal_count = len(scraped["gallery_imgs"])
            add_log(f"Gallery images: {gal_count}", "ok" if gal_count else "warn")
            desc_ok = bool(scraped["desc_html"])
            add_log("Description ✓" if desc_ok else "Description failed", "ok" if desc_ok else "error")
            update_ui(idx, item_id, "Scraping eBay page...", base_pct + 5)

            # ── Stage 1b: Gemini 3D Render (vehicle image from sheet) ────
            _row_data     = _row_map.get(url, {})
            _vehicle_url  = _row_data.get("vehicle_img", "")
            _rendered_img = None  # bytes of Gemini-rendered image

            if _vehicle_url and GEMINI_KEY:
                update_ui(idx, item_id, "Processing vehicle image...", base_pct + 6)
                try:
                    import os as _os, re as _re
                    from PIL import Image as _PILImage, ImageOps as _ImageOps
                    from urllib.parse import unquote as _unquote

                    # ── Extract vehicle name from Drive filename ───────────────
                    # Format: "2011 Honda Civic DX-G Sedan 4-Door - UploaderName.jpg"
                    # We need to fetch the actual filename from Drive metadata
                    _file_id_veh = None
                    from sheets_helper import extract_drive_file_id
                    _file_id_veh = extract_drive_file_id(_vehicle_url)

                    _raw_filename = ""
                    if _file_id_veh:
                        try:
                            _drv_svc  = build("drive", "v3", credentials=st.session_state.drive_creds)
                            _meta     = _drv_svc.files().get(
                                fileId=_file_id_veh, fields="name", supportsAllDrives=True
                            ).execute()
                            _raw_filename = _meta.get("name", "")
                        except Exception:
                            pass

                    # Fallback: parse from URL
                    if not _raw_filename:
                        _raw_filename = _unquote(_vehicle_url.split("/")[-1].split("?")[0])

                    # Get original extension (keep it as .jpg output)
                    _name_no_ext  = _os.path.splitext(_raw_filename)[0]
                    # Remove uploader suffix: last " - Name" segment
                    _vehicle_name = _re.sub(r" - [^-]+$", "", _name_no_ext).strip() or "this vehicle"
                    _render_fname = f"{_vehicle_name}.jpg"   # final filename in Drive + ZIP
                    add_log(f"Vehicle: {_vehicle_name}", "info")

                    # ── Check if rendered image already exists in parent folder ──
                    _existing_id = find_file_in_folder(
                        st.session_state.drive_creds, DRIVE_FOLDER_ID, _render_fname
                    )

                    if _existing_id:
                        # ✅ Cache hit — download from parent folder, skip Gemini
                        add_log(f"Render cache hit: {_render_fname} — copying from parent folder", "ok")
                        _rendered_img = download_file_bytes(st.session_state.drive_creds, _existing_id)
                        if not _rendered_img:
                            add_log("Cache download failed — will re-render", "warn")
                    else:
                        _rendered_img = None

                    if not _rendered_img:
                        # ── Download source image from Drive ───────────────────
                        update_ui(idx, item_id, "Downloading vehicle image...", base_pct + 7)
                        _vehicle_bytes = download_drive_image(st.session_state.drive_creds, _vehicle_url)
                        if not _vehicle_bytes:
                            add_log("Vehicle image download failed", "warn")
                        else:
                            # ── Run Gemini 3D render ───────────────────────────
                            update_ui(idx, item_id, "Running 3D render (Gemini)...", base_pct + 8)
                            _src_img = _PILImage.open(io.BytesIO(_vehicle_bytes))
                            _gemini  = genai.Client(api_key=GEMINI_KEY)
                            _prompt  = f"""
I have attached {_vehicle_name}.
I need to create an image of {_vehicle_name} in color code is #FF0000
with background of pure white color code is #FFFFFF and also add drop shadow.
I need 3D images looks like create in 3D rendering software.
I need view at 25 degrees from driver's side.
Don't add much brightness on the {_vehicle_name}.
I need HD quality and size 2500 by 2500 pixels with 300 DPI.
"""
                            _resp = _gemini.models.generate_content(
                                model="gemini-2.5-flash-image",
                                contents=[_prompt, _src_img],
                                config=types.GenerateContentConfig(
                                    temperature=1.0,
                                    safety_settings=[types.SafetySetting(
                                        category="HARM_CATEGORY_DANGEROUS_CONTENT",
                                        threshold="BLOCK_NONE"
                                    )],
                                ),
                            )
                            for _part in _resp.parts:
                                if _part.inline_data:
                                    _raw_img = _part.as_image()
                                    _pil     = _PILImage.open(io.BytesIO(_raw_img.image_bytes))
                                    _padded  = _ImageOps.pad(_pil, (1280, 960),
                                        method=_PILImage.Resampling.LANCZOS, color=(255, 255, 255))
                                    _buf = io.BytesIO()
                                    _padded.save(_buf, format="JPEG", quality=95, dpi=(300, 300))
                                    _rendered_img = _buf.getvalue()
                                    break

                            if _rendered_img:
                                # Save to parent folder as cache for future runs
                                upload_file(st.session_state.drive_creds, DRIVE_FOLDER_ID,
                                            _render_fname, _rendered_img, "image/jpeg", deduplicate=True)
                                add_log(f"3D render ✓ saved to parent: {_render_fname}", "ok")
                            else:
                                add_log("Gemini returned no image", "warn")

                except Exception as _ge:
                    add_log(f"Gemini error: {_ge}", "warn")
                    _render_fname = "render.jpg"

            elif _vehicle_url and not GEMINI_KEY:
                add_log("GEMINI_KEY not set — skipping 3D render", "warn")

            # ── Stage 2: Building HTML ────────────────────────────────────
            html_str = text_str = None
            sku = scraped["hidden_sku"] or "images"

            if gen_html and desc_ok:
                update_ui(idx, item_id, "Building HTML listing...", base_pct + 10)
                try:
                    html_str = merge_all_data(template, scraped["desc_html"], scraped["gallery_imgs"])
                    add_log("HTML merged ✓", "ok")
                except Exception as e:
                    add_log(f"HTML error: {e}", "error")

            # ── Stage 3: Text export ──────────────────────────────────────
            if gen_text and desc_ok:
                update_ui(idx, item_id, "Extracting text...", base_pct + 12)
                try:
                    text_str = extract_text_data(scraped["desc_html"], scraped.get("compat_rows", []))
                    add_log("Text extracted ✓", "ok")
                except Exception as e:
                    add_log(f"Text error: {e}", "error")

            # ── Stage 4: Packing ZIP ──────────────────────────────────────
            update_ui(idx, item_id, "Packing ZIP...", base_pct + 14)
            # Folder: PartLinkNumber HiddenSKU ItemID
            _pln_z      = scraped.get("part_link_number") or ""
            _sku_z      = scraped.get("hidden_sku") or ""
            _zip_folder = " ".join(p for p in [_pln_z, _sku_z, item_id] if p)
            if html_str:
                zf.writestr(f"{_zip_folder}/{_zip_folder}.html", html_str)
            if text_str:
                zf.writestr(f"{_zip_folder}/{_zip_folder}.txt", text_str)
            # Images flat in parent folder: {sku}_{img_no}.jpg
            if gen_images and scraped["cloud_images"]:
                for img_no, img_bytes in scraped["cloud_images"].items():
                    zf.writestr(f"{_zip_folder}/{sku}_{img_no}.jpg", img_bytes)
            # 3D rendered vehicle image
            if _rendered_img:
                zf.writestr(f"{_zip_folder}/{_render_fname}", _rendered_img)
            # PSD template named after folder
            if st.session_state.psd_template:
                zf.writestr(f"{_zip_folder}/{_zip_folder}.psd", st.session_state.psd_template)

            # ── Stage 5: Drive upload ─────────────────────────────────────
            if upload_drive and DRIVE_FOLDER_ID:
                update_ui(idx, item_id, "Uploading to Drive...", base_pct + 16)
                try:
                    # Use logged-in user's OAuth credentials
                    _creds = st.session_state.drive_creds
                    _drive_root = DRIVE_FOLDER_ID
                    if not _creds:
                        add_log("Drive skipped — sign out and back in to reconnect Google Drive", "warn")
                        raise RuntimeError("no OAuth creds")
                    if not _drive_root:
                        add_log("Drive skipped — DRIVE_FOLDER_ID not set in secrets", "warn")
                        raise RuntimeError("no folder ID")
                    # Folder: PartLinkNumber HiddenSKU ItemID
                    _pln   = scraped.get("part_link_number") or ""
                    _sku_f = scraped.get("hidden_sku") or ""
                    folder_display_name = " ".join(p for p in [_pln, _sku_f, item_id] if p)
                    item_folder = get_or_create_folder(_creds, folder_display_name, _drive_root)
                    # HTML + TXT named after folder (deduplicated)
                    if html_str:
                        upload_file(_creds, item_folder, f"{folder_display_name}.html", html_str.encode("utf-8"), "text/html")
                    if text_str:
                        upload_file(_creds, item_folder, f"{folder_display_name}.txt", text_str.encode("utf-8"), "text/plain")
                    # Images: {sku}_{img_no}.jpg flat in parent folder
                    if scraped["cloud_images"]:
                        for img_no, img_bytes in scraped["cloud_images"].items():
                            upload_file(_creds, item_folder, f"{sku}_{img_no}.jpg", img_bytes, "image/jpeg")
                    # 3D rendered vehicle image
                    if _rendered_img:
                        upload_file(_creds, item_folder, _render_fname, _rendered_img, "image/jpeg")
                    # PSD template named after folder
                    if st.session_state.psd_template:
                        upload_file(_creds, item_folder,
                                    f"{folder_display_name}.psd",
                                    st.session_state.psd_template,
                                    "image/vnd.adobe.photoshop")
                        # Also keep master copy in root output folder
                        upload_file(_creds, _drive_root,
                                    "_LATEST_TEMPLATE.psd",
                                    st.session_state.psd_template,
                                    "image/vnd.adobe.photoshop")
                    add_log("Uploaded to Drive ✓", "ok")
                except Exception as e:
                        if hasattr(e, "resp") and hasattr(e, "content"):
                            status = e.resp.status
                            try:
                                import json as _json
                                detail = _json.loads(e.content).get("error", {})
                                msg = f"HTTP {status} — {detail.get('message', e.content.decode())}"
                            except Exception:
                                msg = f"HTTP {status} — {e.content}"
                            add_log(f"Drive error: {msg}", "error")
                            if status == 403:
                                add_log("Fix: Make sure the Drive folder is shared with your account (Editor)", "warn")
                            elif status == 404:
                                add_log("Fix: Check DRIVE_FOLDER_ID in secrets.toml", "warn")
                        else:
                            add_log(f"Drive error: {type(e).__name__}: {e}", "error")

            # ── Mark sheet row as completed ───────────────────────────
            _row_idx = _row_map.get(url, {}).get("row_index")
            if _row_idx and SHEET_ID:
                try:
                    mark_row_completed(SHEET_ID, _row_idx)
                    add_log(f"Sheet row {_row_idx} marked completed ✓", "ok")
                except Exception as _me:
                    add_log(f"Could not mark sheet row: {_me}", "warn")

            st.session_state.results.append({
                "item_id": item_id, "sku": sku,
                "html": bool(html_str), "text": bool(text_str),
                "images": len(scraped["cloud_images"]),
            })

            # Update live results table
            _rows = "".join(
                f'<tr><td style="color:#f0c040;font-family:DM Mono,monospace;font-size:0.75rem">{r["item_id"]}</td>'
                f'<td style="font-family:DM Mono,monospace;font-size:0.7rem;color:#666">{r["sku"]}</td>'
                f'<td>{"✅" if r["html"] else "❌"}</td><td>{"✅" if r["text"] else "❌"}</td>'
                f'<td>{"✅ " + str(r["images"]) if r["images"] else "—"}</td></tr>'
                for r in st.session_state.results
            )
            results_live.markdown(
                f'<table style="width:100%;border-collapse:collapse;font-size:0.8rem;margin-top:8px">' 
                f'<thead><tr style="color:#555;font-family:DM Mono,monospace;font-size:0.68rem">' 
                f'<th align="left">ITEM</th><th align="left">SKU</th>' 
                f'<th>HTML</th><th>TXT</th><th>IMGS</th></tr></thead>' 
                f'<tbody>{_rows}</tbody></table>',
                unsafe_allow_html=True,
            )
            update_ui(idx, item_id, f"Item {idx+1} complete", int((idx + 1) / total * 95))

    zip_buf.seek(0)
    st.session_state.output_zip = zip_buf.read()
    add_log(f"All done! {len(st.session_state.results)} item(s) processed.", "ok")
    done_ui()

# ── Results ───────────────────────────────────────────────────────────────────
if st.session_state.results:
    st.markdown("---")
    st.markdown('<div class="step-label">📦 Results</div>', unsafe_allow_html=True)
    for r in st.session_state.results:
        h = '<span class="badge badge-ok">HTML ✓</span>'  if r["html"]   else '<span class="badge badge-error">HTML ✗</span>'
        t = '<span class="badge badge-ok">TXT ✓</span>'   if r["text"]   else '<span class="badge badge-error">TXT ✗</span>'
        i = f'<span class="badge badge-ok">🖼 {r["images"]}</span>' if r["images"] else '<span class="badge badge-warn">0 imgs</span>'
        st.markdown(
            f'<div class="result-card">'
            f'<span class="item-id">ITEM # {r["item_id"]}</span>'
            f'<span style="color:#555;font-family:DM Mono,monospace;font-size:0.7rem;margin-left:10px">SKU: {r["sku"]}</span>'
            f'<div style="margin-top:8px">{h} &nbsp;{t} &nbsp;{i}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

# ── Download ZIP ──────────────────────────────────────────────────────────────
if st.session_state.output_zip:
    st.download_button(
        "⬇️ Download All Results (ZIP)",
        data=st.session_state.output_zip,
        file_name="ebay_listings_output.zip",
        mime="application/zip",
    )

# ── Log expander ──────────────────────────────────────────────────────────────
if st.session_state.logs:
    with st.expander("📜 Full Log"):
        st.markdown(f'<div class="log-box">{"<br>".join(st.session_state.logs)}</div>', unsafe_allow_html=True)