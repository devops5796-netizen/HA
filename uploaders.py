"""
uploaders.py
============
Unified upload interface supporting both R2 (Cloudflare) and Google Drive.

Usage in haraj_pipeline.py:
    from uploaders import upload_buffer, set_upload_target
    set_upload_target("drive")  # or "r2"
    key = upload_buffer(buffer, filename, r2_path, file_type, content_type, dt)
"""

import io
from datetime import datetime

# Global upload target: "r2" or "drive"
_UPLOAD_TARGET = "r2"


def set_upload_target(target: str):
    """Set the upload target: 'r2' or 'drive'."""
    global _UPLOAD_TARGET
    target = target.lower().strip()
    if target not in ("r2", "drive"):
        raise ValueError(f"Unknown upload target: {target}. Use 'r2' or 'drive'.")
    _UPLOAD_TARGET = target
    print(f"[Uploaders] Upload target set to: {_UPLOAD_TARGET.upper()}")


def get_upload_target() -> str:
    return _UPLOAD_TARGET


def build_key(r2_path: str, file_type: str, filename: str, dt: datetime = None) -> str:
    """Build the logical key/path for both R2 and Drive."""
    if dt is None:
        dt = datetime.now()
    year = f"year={dt.year}"
    month = f"month={dt.strftime('%m')}"
    day = f"day={dt.strftime('%d')}"
    return f"haraj/{year}/{month}/{day}/{r2_path}/{file_type}/{filename}"


def upload_buffer(
    buffer: io.BytesIO,
    filename: str,
    r2_path: str,
    file_type: str = "images",
    content_type: str = "image/webp",
    dt: datetime = None,
) -> str | None:
    """
    Upload a buffer to the configured target (R2 or Google Drive).
    Returns the uploaded file key/path, or None on failure.
    """
    key = build_key(r2_path, file_type, filename, dt)

    if _UPLOAD_TARGET == "r2":
        from r2_uploader import upload_buffer as r2_upload
        return r2_upload(buffer, filename, r2_path, file_type, content_type, dt)

    elif _UPLOAD_TARGET == "drive":
        from drive_uploader import upload_buffer as drive_upload
        return drive_upload(buffer, key, filename, content_type)

    return None