"""
drive_uploader.py
=================
Upload files to Google Drive.

Supports TWO authentication methods:

1) SERVICE ACCOUNT (default fallback)
   - Needs GOOGLE_SERVICE_ACCOUNT_FILE or GOOGLE_SERVICE_ACCOUNT_JSON
   - Limitation: Service Accounts have NO storage quota.
   - Works ONLY if the folder is inside a SHARED DRIVE (Team Drive)
     OR if you have Google Workspace with domain-wide delegation.

2) OAUTH 2.0 (RECOMMENDED for personal Google accounts)
   - Uses YOUR personal Google account (has storage quota)
   - First run opens browser for authorization
   - Saves token.json for future runs
   - Set GOOGLE_USE_OAUTH=true to enable

Setup OAuth 2.0:
----------------
1. Go to https://console.cloud.google.com/ → APIs & Services → Credentials
2. Click "+ CREATE CREDENTIALS" → "OAuth client ID"
3. Application type: "Desktop app"
4. Download the JSON → rename to "client_secret.json" → put in project root
5. Set in .env: GOOGLE_USE_OAUTH=true
6. First run will open browser → authorize → token.json saved automatically

.env variables:
---------------
# OAuth 2.0 (RECOMMENDED)
GOOGLE_USE_OAUTH=true
GOOGLE_CLIENT_SECRET_FILE=client_secret.json
GOOGLE_DRIVE_FOLDER_ID=your_folder_id

# OR Service Account (only works with Shared Drive)
GOOGLE_SERVICE_ACCOUNT_FILE=service_account.json
GOOGLE_DRIVE_FOLDER_ID=your_folder_id
"""

import os
import io
import json
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
USE_OAUTH = os.getenv("GOOGLE_USE_OAUTH", "false").lower().strip() in ("true", "1", "yes")

# OAuth 2.0
OAUTH_CLIENT_SECRET_FILE = os.getenv("GOOGLE_CLIENT_SECRET_FILE", "client_secret.json")
OAUTH_TOKEN_FILE = os.getenv("GOOGLE_TOKEN_FILE", "token.json")

# Service Account
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json")
SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

# Shared Drive (for Service Account mode)
SHARED_DRIVE_ID = os.getenv("GOOGLE_SHARED_DRIVE_ID", "")

# Common
DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")

# ------------------------------------------------------------------
# Lazy imports & client cache
# ------------------------------------------------------------------
_drive_service = None
_folder_cache = {}


def _get_credentials():
    """Return credentials (OAuth or Service Account)."""
    if USE_OAUTH:
        return _get_oauth_credentials()
    else:
        return _get_service_account_credentials()


def _get_oauth_credentials():
    """OAuth 2.0 flow for personal Google account."""
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow

    SCOPES = ["https://www.googleapis.com/auth/drive"]

    creds = None
    if os.path.exists(OAUTH_TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(OAUTH_TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("[Drive] Refreshing OAuth token...")
            creds.refresh(Request())
        else:
            if not os.path.exists(OAUTH_CLIENT_SECRET_FILE):
                raise RuntimeError(
                    f"OAuth client secret not found: {OAUTH_CLIENT_SECRET_FILE}.\n"
                    "Please download it from Google Cloud Console → Credentials → "
                    "OAuth client ID (Desktop app) and save it as client_secret.json"
                )
            print("[Drive] Starting OAuth flow - browser will open for authorization...")
            flow = InstalledAppFlow.from_client_secrets_file(OAUTH_CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
            print("[Drive] OAuth authorization successful!")

        # Save token for future runs
        with open(OAUTH_TOKEN_FILE, "w", encoding="utf-8") as token:
            token.write(creds.to_json())
        print(f"[Drive] Token saved to {OAUTH_TOKEN_FILE}")

    return creds


def _get_service_account_credentials():
    """Service Account credentials."""
    from google.oauth2 import service_account

    if SERVICE_ACCOUNT_JSON:
        info = json.loads(SERVICE_ACCOUNT_JSON)
    elif os.path.exists(SERVICE_ACCOUNT_FILE):
        with open(SERVICE_ACCOUNT_FILE, "r", encoding="utf-8") as f:
            info = json.load(f)
    else:
        raise RuntimeError(
            "Google Drive credentials not found.\n"
            "For OAuth (recommended): set GOOGLE_USE_OAUTH=true and provide client_secret.json\n"
            "For Service Account: set GOOGLE_SERVICE_ACCOUNT_FILE or GOOGLE_SERVICE_ACCOUNT_JSON"
        )

    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return creds


def _get_drive_service():
    global _drive_service
    if _drive_service is not None:
        return _drive_service

    from googleapiclient.discovery import build

    creds = _get_credentials()
    _drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _drive_service


def _find_or_create_folder(name: str, parent_id: str) -> str:
    """Find or create a folder under parent_id. Returns folder ID."""
    cache_key = f"{parent_id}/{name}"
    if cache_key in _folder_cache:
        return _folder_cache[cache_key]

    service = _get_drive_service()

    # Escape single quotes for Google Drive API query (double them)
    safe_name = name.replace("'", "''")
    query = (
        f"mimeType='application/vnd.google-apps.folder' "
        f"and name='{safe_name}' "
        f"and '{parent_id}' in parents and trashed=false"
    )

    kwargs = {"q": query, "spaces": "drive", "fields": "files(id, name)", "pageSize": 1}
    if not USE_OAUTH and SHARED_DRIVE_ID:
        kwargs["driveId"] = SHARED_DRIVE_ID
        kwargs["includeItemsFromAllDrives"] = True
        kwargs["supportsAllDrives"] = True
        kwargs["corpora"] = "drive"

    results = service.files().list(**kwargs).execute()
    files = results.get("files", [])

    if files:
        folder_id = files[0]["id"]
        _folder_cache[cache_key] = folder_id
        return folder_id

    # Create folder
    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }

    create_kwargs = {"body": metadata, "fields": "id"}
    if not USE_OAUTH and SHARED_DRIVE_ID:
        create_kwargs["supportsAllDrives"] = True

    folder = service.files().create(**create_kwargs).execute()
    folder_id = folder["id"]
    _folder_cache[cache_key] = folder_id
    return folder_id


def _ensure_path(path: str, root_folder_id: str) -> str:
    """
    Ensure the full folder path exists in Drive.
    path looks like: haraj/year=2026/month=08/day=19/Cars/excel
    Returns the leaf folder ID.
    """
    parts = [p for p in path.split("/") if p]
    current_id = root_folder_id
    for part in parts:
        current_id = _find_or_create_folder(part, current_id)
    return current_id


def upload_buffer(
    buffer: io.BytesIO,
    key: str,           # e.g. "haraj/year=2026/month=08/day=19/Cars/excel/Toyota.xlsx"
    filename: str,      # e.g. "Toyota.xlsx"
    content_type: str = "application/octet-stream",
) -> str | None:
    """
    Upload buffer to Google Drive under the folder structure defined by `key`.
    Returns the logical key on success, None on failure.
    """
    if not DRIVE_FOLDER_ID:
        print("  [ERROR] GOOGLE_DRIVE_FOLDER_ID not set. Cannot upload to Drive.")
        return None

    try:
        service = _get_drive_service()

        # key ends with filename, so folder path is everything before the last /
        folder_path = key.rsplit("/", 1)[0] if "/" in key else ""
        parent_folder_id = _ensure_path(folder_path, DRIVE_FOLDER_ID)

        buffer.seek(0)

        from googleapiclient.http import MediaIoBaseUpload

        media = MediaIoBaseUpload(buffer, mimetype=content_type, resumable=True)

        file_metadata = {
            "name": filename,
            "parents": [parent_folder_id],
        }

        create_kwargs = {"body": file_metadata, "media_body": media, "fields": "id"}
        if not USE_OAUTH and SHARED_DRIVE_ID:
            create_kwargs["supportsAllDrives"] = True

        file = service.files().create(**create_kwargs).execute()
        file_id = file.get("id")
        print(f"  [Drive] Uploaded -> {key} (id={file_id})")
        return key

    except Exception as e:
        print(f"  [ERROR] Drive upload failed for {filename}: {e}")
        return None