import os
import io
from datetime import datetime
import boto3
from dotenv import load_dotenv

load_dotenv()

CF_R2_ACCESS_KEY = os.getenv("CF_R2_ACCESS_KEY_ID")
CF_R2_SECRET_KEY = os.getenv("CF_R2_SECRET_ACCESS_KEY")
CF_R2_ENDPOINT_URL = os.getenv("CF_R2_ENDPOINT_URL")
BUCKET_NAME = os.getenv("CF_R2_BUCKET_NAME", "")

CLEAN_ENDPOINT = ""
if CF_R2_ENDPOINT_URL:
    CLEAN_ENDPOINT = CF_R2_ENDPOINT_URL.rstrip("/").removesuffix("/" + BUCKET_NAME)

_client = None


def get_r2_client():
    global _client
    if _client is not None:
        return _client
    if CF_R2_ACCESS_KEY and CF_R2_SECRET_KEY and CLEAN_ENDPOINT:
        try:
            _client = boto3.client(
                "s3",
                endpoint_url=CLEAN_ENDPOINT,
                aws_access_key_id=CF_R2_ACCESS_KEY,
                aws_secret_access_key=CF_R2_SECRET_KEY,
                region_name="auto",
            )
            return _client
        except Exception as e:
            print(f"Failed to initialize R2 client: {e}")
            return None
    print("Warning: R2 environment variables are missing.")
    return None


def build_haraj_key(r2_path: str, file_type: str, filename: str, dt: datetime = None) -> str:
    """
    Date-partitioned layout:
    haraj/year=YYYY/month=MM/day=DD/{r2_path}/{file_type}/{filename}
    e.g. haraj/year=2026/month=08/day=11/Cars/Cars_for_sale_rent/excel/Toyota.xlsx

    r2_path can contain "/" itself (e.g. "Cars/Cars_for_sale_rent") -- it's
    just concatenated as-is, since S3/R2 keys are plain strings and slashes
    inside them behave as folder separators.
    """
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
    client = get_r2_client()
    if not client or not BUCKET_NAME:
        return None

    r2_key = build_haraj_key(r2_path, file_type, filename, dt)

    try:
        buffer.seek(0)
        client.upload_fileobj(buffer, BUCKET_NAME, r2_key, ExtraArgs={"ContentType": content_type})
        return r2_key
    except Exception as e:
        print(f"  [ERROR] R2 upload failed for {filename}: {e}")
        return None