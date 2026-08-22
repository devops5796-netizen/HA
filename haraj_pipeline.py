"""
haraj_pipeline.py
==================
Reads haraj_all_categories.csv (produced by discover_categories.py),
scrapes ads for every leaf category, and uploads to R2 as:

    haraj/year=YYYY/month=MM/day=DD/{r2_path}/excel/{file}.xlsx
    haraj/year=YYYY/month=MM/day=DD/{r2_path}/json/{file}.json
    haraj/year=YYYY/month=MM/day=DD/{r2_path}/images/{ad_id}-N.webp

Grouping rules
--------------
- Cars: level_1 values that are actual car BRANDS (i.e. not in
  NON_BRAND_CAR_LEVEL1) are merged under one shared folder
  "Cars/Cars_for_sale_rent/", one excel file per brand, one sheet per
  model (level_2).
- Cars: level_1 values in NON_BRAND_CAR_LEVEL1 ("Parts & Accessories",
  "Trucks and heavy equipment", "Motorcycles") get their own folder
  "Cars/<that level_1 name>/", exactly like a normal category.
- Any other top category: "<cat_name>/<level_1_name>/" per level_1,
  one excel file per level_1, one sheet per level_2 (or a single sheet
  named after level_1 when that category has no level_2 at all).
- A category with no level_1 at all (leaf right at the top tag) becomes
  its own flat folder "<cat_name>/" with one file, one sheet.

Usage:
    python haraj_pipeline.py --categories haraj_all_categories.csv
    python haraj_pipeline.py --cat-name Cars --max-pages 3   # quick test
    python haraj_pipeline.py --cat-name Cars --limit-rows 5  # even smaller test
"""

import argparse
import io
import json
import random
import re
import time
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from PIL import Image

from uploaders import upload_buffer, set_upload_target
from request_tracker import tracker

# ============================================================
# CONFIG
# ============================================================

BASE_URL = "https://graphql.haraj.com.sa/"
CLIENT_ID = "mVaYf0dJ-lh8g-XZhw-s1Xg-ssMJAOwsMTf7v3"
LANG = "en"

MIN_REQUEST_DELAY = 1.0      
MAX_REQUEST_DELAY = 2.0      
MIN_LEAF_DELAY = 3.0         
MAX_LEAF_DELAY = 5.0         
MIN_IMAGE_DELAY = 0.3       
MAX_IMAGE_DELAY = 0.7       

MAX_RETRIES = 3
RETRY_DELAY = 10
IMAGE_TIMEOUT = 20


Riyadh_now = datetime.now(ZoneInfo("Asia/Riyadh"))
TARGET_DATE = (Riyadh_now.date() - timedelta(days=1))

# Level-1 names under "Cars" that are NOT car brands -- these keep their
# own folder instead of being merged into Cars_for_sale_rent.
NON_BRAND_CAR_LEVEL1 = {
    "Parts & Accessories",
    "Trucks and heavy equipment",
    "Motorcycles",
}

HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json",
    "origin": "https://haraj.com.sa",
    "referer": "https://haraj.com.sa/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/151.0.0.0 Safari/537.36"
    ),
}

session = requests.Session()
session.headers.update(HEADERS)

QUERY = """
query FetchAds(
    $id: [Int] = null, $city: String = null, $cities: [String],
    $authorUsername: String = null, $page: Int = null, $limit: Int = null,
    $afterPostDate: Int = null, $afterUpdateDate: Int = null,
    $beforeUpdateDate: Int = null, $beforePostDate: Int = null,
    $tag: String = null, $near: String = null,
    $onlyWithImage: Boolean = null, $onlyWithVideo: Boolean = null,
    $orderMainByPostId: Boolean = null, $notTag: String = null
) {
    posts(
        id: $id, city: $city, cities: $cities, authorUsername: $authorUsername,
        page: $page, limit: $limit, afterPostDate: $afterPostDate,
        afterUpdateDate: $afterUpdateDate, beforeUpdateDate: $beforeUpdateDate,
        beforePostDate: $beforePostDate, tag: $tag, near: $near,
        onlyWithImage: $onlyWithImage, onlyWithVideo: $onlyWithVideo,
        orderMainByPostId: $orderMainByPostId, notTag: $notTag
    ) {
        analyticsContext
        items { ...PostFields }
        pageInfo { hasNextPage }
        viewOptions { hasSellersList mustLoginToView }
    }
}

fragment PostFields on Post {
    id title postDate updateDate authorUsername authorId URL
    bodyTEXT bodyHTML thumbURL hasImage hasVideo
    city geoCity geoNeighborhood geoHash
    tags imagesList
    commentEnabled commentStatus isPromoted commentCount upRank downRank
    status postType
    generalInfo { key value }
    price { formattedPrice inputPrice }
    realEstateInfo { re_REGA_Advertiser_registration_number re_REGA_Authorization_number }
    carInfo { sellOrWaiver is4DW model mileage fuel gear condition carOrRelated Bank }
    tagsFilters
    jobsInfo { jobs_OfferType jobs_ExperienceLevel jobs_ContractType jobs_Qualification jobs_CommercialeRgisterNumber }
    postNotesList { iconName iconUrl note link }
    BuyButton { Link StoreName Name canRequestWasataService isMakeOfferEnabled }
}
"""

# ============================================================
# SELLER CONFIG
# ============================================================

SELLER_URL = "https://matjar.haraj.com.sa/graphql"
SELLER_HEADERS = {
    "accept": "*/*",
    "content-type": "application/json",
    "origin": "https://haraj.com.sa",
    "referer": "https://haraj.com.sa/",
    "apollographql-client-name": "web",
    "apollographql-client-version": "N0.0.1 , 2026-08-03 15",
    "user-agent": HEADERS["user-agent"],
}

SELLER_QUERY = """
query Profile($profileId: ID!) {
    profile(id: $profileId) {
        id
        handler
        type
        description
        pages { id title content order }
        locations { id value description }
        contacts { id description info type }
        verifications { id type status data }
        updatedAt
    }
}
"""

MIN_SELLER_DELAY = 0.1      
MAX_SELLER_DELAY = 0.2      

# ============================================================
# TEXT HELPERS
# ============================================================

def clean_text(value):
    if value is None:
        return ""
    value = str(value)
    for zw in ("\u200b", "\u200c", "\u200d", "\ufeff"):
        value = value.replace(zw, "")
    value = unicodedata.normalize("NFKC", value)
    if any(x in value for x in ["Ø", "Ù", "Ã", "Â", "â"]):
        try:
            value = value.encode("latin1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
    return re.sub(r"\s+", " ", value).strip()


def clean_for_excel(value):
    if isinstance(value, str):
        return re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", value)
    return value


def sanitize_filename(value: str) -> str:
    value = clean_text(value)
    value = re.sub(r'[\\/:*?"<>|]', "-", value)
    return value or "Unknown"


def clean_sheet_name(name: str) -> str:
    name = clean_text(name)
    name = re.sub(r'[\\/*?:\[\]]', "_", name).strip()
    return (name or "Unknown")[:31]


def unique_sheet_name(name: str, existing: set) -> str:
    base = clean_sheet_name(name)
    if base not in existing:
        return base
    counter = 2
    while True:
        suffix = f"_{counter}"
        candidate = base[:31 - len(suffix)] + suffix
        if candidate not in existing:
            return candidate
        counter += 1


def serialize_value(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def build_post_row(post: dict) -> dict:
    """Keep every field as its own column."""
    result = {}

    for key, value in post.items():
        if key == "imagesList":
            result[key] = value
        else:
            result[key] = serialize_value(value)

    return result



# ============================================================
# IMAGE DOWNLOAD -> R2
# ============================================================

def download_images(image_urls: list[str], ad_id: str, r2_path: str, dt: datetime) -> list[str]:
    r2_paths = []
    for idx, url in enumerate(image_urls, start=1):
        filename = f"{ad_id}-{idx}.webp"
        try:
            r = requests.get(url, timeout=IMAGE_TIMEOUT)
            if r.status_code != 200:
                print(f"      [ERROR] image {idx} ({ad_id}): HTTP {r.status_code}")
                tracker.log_request(source="images", success=False)
                continue
            img = Image.open(io.BytesIO(r.content)).convert("RGB")
            buf = io.BytesIO()
            img.save(buf, format="WEBP", quality=75, method=6)
            buf.seek(0)
            key = upload_buffer(
                buf, filename=filename, r2_path=r2_path,
                file_type="images", content_type="image/webp", dt=dt,
            )
            tracker.log_request(source="images", success=bool(key))
            if key:
                r2_paths.append(key)
        except Exception as e:
            print(f"      [ERROR] image {idx} ({ad_id}): {e}")
            tracker.log_request(source="images", success=False)
 
        if idx < len(image_urls):
            time.sleep(random.uniform(MIN_IMAGE_DELAY, MAX_IMAGE_DELAY))
    return r2_paths


# ============================================================
# GRAPHQL SCRAPING (generalized from try_toyota.py)
# ============================================================

def fetch_page(tag: str, page: int, before_update_date: int | None = None) -> tuple[list, bool, int | None]:
    params = {
        "queryName": "posts",
        "lang": LANG,
        "clientId": CLIENT_ID,
    }
    
    variables = {
        "tag": tag,
        "page": page,
        "orderMainByPostId": True,  # ← جديد: نرتب حسب postDate
    }
    if before_update_date is not None:
        variables["beforeUpdateDate"] = before_update_date
    
    payload = {"query": QUERY, "variables": variables}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.post(BASE_URL, params=params, json=payload, timeout=60)
            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                print(f"    GraphQL errors: {json.dumps(data['errors'], ensure_ascii=False)}")
                tracker.log_request(source="listing_pages", success=False)
                return [], False, None

            posts_data = data.get("data", {}).get("posts", {})
            items = posts_data.get("items", [])
            has_next = posts_data.get("pageInfo", {}).get("hasNextPage", False)
            tracker.log_request(source="listing_pages", success=True)
            
            last_update_date = items[-1].get("updateDate") if items else None
            return items, has_next, last_update_date

        except requests.exceptions.RequestException as e:
            print(f"    Request failed (attempt {attempt}/{MAX_RETRIES}) tag={tag} page={page}: {e}")
            tracker.log_request(source="listing_pages", success=False)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
            else:
                print("    Max retries reached.")
                return [], False, None
        except Exception as e:
            print(f"    Unexpected error: {e}")
            tracker.log_request(source="listing_pages", success=False)
            return [], False, None

    return [], False, None

def convert_timestamp_columns(df: pd.DataFrame) -> pd.DataFrame:
    timestamp_columns = [
        "postDate",
        "updateDate",
    ]
    df = df.copy()
    for col in timestamp_columns:
        if col in df.columns:
            df[col] = (
                pd.to_datetime(
                    pd.to_numeric(df[col], errors="coerce"),
                    unit="s",
                    errors="coerce",
                    utc=True
                )
                .dt.tz_convert("Asia/Riyadh")
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )

            print(f"  Converted timestamp column: {col}")

    return df

def filter_yesterday_hits(hits):
    filtered = []
    for hit in hits:
        timestamp_value = hit.get("postDate")
        if timestamp_value is None:
            continue
        try:
            dt_utc = datetime.fromtimestamp(int(timestamp_value), tz=timezone.utc)
            dt_Riyadh = dt_utc.astimezone(ZoneInfo("Asia/Riyadh"))
            if dt_Riyadh.date() == TARGET_DATE:
                filtered.append(hit)
        except (ValueError, TypeError):
            pass
    return filtered


# ============================================================
# SELLER PROFILE FETCHING
# ============================================================

def fetch_seller_profile(author_id: str) -> dict | None:
    payload = {"query": SELLER_QUERY, "variables": {"profileId": str(author_id)}}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.post(SELLER_URL, headers=SELLER_HEADERS, json=payload, timeout=60)
            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                err_msg = data["errors"][0].get("message", "") if data["errors"] else ""
                print(f"    [Seller] GraphQL error for {author_id}: {err_msg}")
                tracker.log_request(source="seller_profiles", success=False)
                return None

            profile = data.get("data", {}).get("profile")
            tracker.log_request(source="seller_profiles", success=bool(profile))
            return profile

        except requests.exceptions.RequestException as e:
            print(f"    [Seller] Request failed (attempt {attempt}/{MAX_RETRIES}) for {author_id}: {e}")
            tracker.log_request(source="seller_profiles", success=False)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
            else:
                print(f"    [Seller] Max retries reached for {author_id}")
                return None
        except Exception as e:
            print(f"    [Seller] Unexpected error for {author_id}: {e}")
            tracker.log_request(source="seller_profiles", success=False)
            return None
    return None


def flatten_seller_profile(profile: dict) -> dict:
    if not profile:
        return {}
    row = {
        "seller_id": profile.get("id"),
        "seller_handler": profile.get("handler"),
        "seller_type": profile.get("type"),
        "seller_description": profile.get("description"),
        "seller_updatedAt": profile.get("updatedAt"),
        "seller_locations": json.dumps(profile.get("locations") or [], ensure_ascii=False),
        "seller_contacts": json.dumps(profile.get("contacts") or [], ensure_ascii=False),
        "seller_verifications": json.dumps(profile.get("verifications") or [], ensure_ascii=False),
        "seller_pages": json.dumps(profile.get("pages") or [], ensure_ascii=False),
    }
    contacts = profile.get("contacts") or []
    for i, contact in enumerate(contacts, start=1):
        row[f"seller_contact_{i}_id"] = contact.get("id")
        row[f"seller_contact_{i}_description"] = contact.get("description")
        row[f"seller_contact_{i}_info"] = contact.get("info")
        row[f"seller_contact_{i}_type"] = contact.get("type")
    return row


def fetch_sellers_for_records(records: list[dict]) -> dict[str, dict]:
    unique_ids = set()
    for rec in records:
        aid = rec.get("authorId")
        if aid is not None:
            unique_ids.add(str(aid).strip())
    
    if not unique_ids:
        return {}
    
    sellers = {}
    print(f"  Unique sellers to fetch: {len(unique_ids)}")
    for idx, author_id in enumerate(sorted(unique_ids), 1):
        profile = fetch_seller_profile(author_id)
        if profile:
            sellers[author_id] = flatten_seller_profile(profile)
        if idx < len(unique_ids):
            time.sleep(random.uniform(MIN_SELLER_DELAY, MAX_SELLER_DELAY))
    return sellers


def scrape_tag(tag: str, max_pages: int | None = None) -> list[dict]:
    all_posts = []
    seen_ids = set()
    page = 0
    before_update_date = None
    consecutive_empty = 0

    while True:
        if max_pages is not None and page >= max_pages:
            print(f"    Reached page limit: {max_pages}")
            break

        items, has_next, last_update_date = fetch_page(tag, page, before_update_date)
        if not items:
            break

        new_items = 0
        for post in items:
            post_id = post.get("id")
            if post_id in seen_ids:
                continue
            seen_ids.add(post_id)
            all_posts.append(build_post_row(post))
            new_items += 1

        print(f"    page {page}: {len(items)} returned, {new_items} new (total {len(all_posts)})")

        if not has_next:
            break

        # ← جديد: لو 3 صفحات ورا بعض مفيش جديد، نوقف (API limit ~1000)
        if new_items == 0:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                print(f"    Stopping: {consecutive_empty} consecutive empty pages")
                break
        else:
            consecutive_empty = 0

        # ← Cursor: نبعت آخر updateDate في الصفحة الجاية
        if last_update_date is not None:
            before_update_date = last_update_date

        page += 1
        delay = random.uniform(MIN_REQUEST_DELAY, MAX_REQUEST_DELAY)
        time.sleep(delay)

    return all_posts


# ============================================================
# CATEGORY CLASSIFICATION -> (scrape_tag, r2_path, file_key, sheet_name)
# ============================================================

def classify_row(row: dict) -> dict:
    cat_name = row["cat_name"]
    l1_name, l1_tag = row.get("level_1_name"), row.get("level_1_tag_name")
    l2_name, l2_tag = row.get("level_2_name"), row.get("level_2_tag_name")
    cat_tag = row["cat_tag_name"]

    if pd.notna(l2_tag) and l2_tag:
        tag_to_scrape = l2_tag
    elif pd.notna(l1_tag) and l1_tag:
        tag_to_scrape = l1_tag
    else:
        tag_to_scrape = cat_tag

    has_l1 = pd.notna(l1_name) and bool(l1_name)
    has_l2 = pd.notna(l2_name) and bool(l2_name)

    if cat_name == "Cars":
        if has_l1 and l1_name in NON_BRAND_CAR_LEVEL1:
            r2_path = f"Cars/{l1_name}"
            file_key = l1_name
            sheet_name = l2_name if has_l2 else l1_name
        elif has_l1:  # a brand
            r2_path = "Cars/Cars_for_sale_rent"
            file_key = l1_name
            sheet_name = l2_name if has_l2 else l1_name
        else:
            r2_path = "Cars"
            file_key = "Cars"
            sheet_name = "All"
    else:
        if has_l1:
            r2_path = f"{cat_name}/{l1_name}"
            file_key = l1_name
            sheet_name = l2_name if has_l2 else l1_name
        else:
            r2_path = cat_name
            file_key = cat_name
            sheet_name = cat_name

    return {
        "scrape_tag": tag_to_scrape,
        "r2_path": r2_path,
        "file_key": file_key,
        "sheet_name": sheet_name,
    }


# ============================================================
# UPLOAD ONE GROUP (excel + json, all its sheets)
# ============================================================

def upload_group(r2_path: str, file_key: str, sheets: dict[str, list], dt: datetime) -> None:
    total_ads = sum(len(rows) for rows in sheets.values())
    print(f"\n{r2_path} / {file_key}: {len(sheets)} sheet(s), {total_ads} ad(s)")
    for name, rows in sheets.items():
        print(f"  - {name}: {len(rows)}")

    if total_ads == 0:
        print("  (nothing to upload)")
        return

    excel_buf = io.BytesIO()
    used_names = set()
    with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
        for sheet_name, rows in sheets.items():
            if not rows:
                continue
            df = pd.DataFrame(rows)
            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].apply(clean_for_excel)
            name = unique_sheet_name(sheet_name, used_names)
            used_names.add(name)
            df.to_excel(writer, sheet_name=name, index=False)

    excel_key = upload_buffer(
        excel_buf, filename=f"{sanitize_filename(file_key)}.xlsx", r2_path=r2_path,
        file_type="excel",
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        dt=dt,
    )
    print(f"  Excel -> {excel_key}")

    json_bytes = json.dumps(sheets, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    json_key = upload_buffer(
        io.BytesIO(json_bytes), filename=f"{sanitize_filename(file_key)}.json", r2_path=r2_path,
        file_type="json", content_type="application/json", dt=dt,
    )
    print(f"  JSON  -> {json_key}")


# ============================================================
# MAIN
# ============================================================

def run(
    categories_csv: str = "haraj_all_categories.csv",
    cat_name_filter: str | None = None,
    max_pages: int | None = None,
    limit_rows: int | None = None,
    dt: datetime | None = None,
    upload_target: str = "r2",
) -> None:
    set_upload_target(upload_target)
    dt = dt or datetime.now()

    df = pd.read_csv(categories_csv, encoding="utf-8-sig")
    if cat_name_filter:
        df = df[df["cat_name"] == cat_name_filter]
    if limit_rows:
        df = df.head(limit_rows)

    rows = df.to_dict("records")
    total = len(rows)
    print(f"Leaf categories to scrape: {total}" + (f" (cat={cat_name_filter})" if cat_name_filter else ""))

    # groups[(r2_path, file_key)][sheet_name] -> list of ad records
    groups: dict[tuple, dict] = defaultdict(lambda: defaultdict(list))

    for i, row in enumerate(rows, 1):
        meta = classify_row(row)
        print("\n" + "#" * 80)
        print(
            f"[{i}/{total}] {row['cat_name']} > {row.get('level_1_name')} > {row.get('level_2_name')} "
            f"(tag='{meta['scrape_tag']}')"
        )
        print(f"  -> r2_path={meta['r2_path']}  file={meta['file_key']}  sheet={meta['sheet_name']}")
        print("#" * 80)

        records = scrape_tag(meta["scrape_tag"], max_pages=max_pages)

        before_filter = len(records)
        records = filter_yesterday_hits(records)

        print(
            f"  Date filter: {before_filter} -> {len(records)} ads "
            f"(postDate = {TARGET_DATE})"
        )

        if records:
            # Convert postDate and updateDate to Riyadh datetime strings
            records_df = pd.DataFrame(records)
            records_df = convert_timestamp_columns(records_df)
            records = records_df.to_dict("records")

        # Fetch seller profiles for unique authors in this batch
        seller_map = fetch_sellers_for_records(records)
        print(f"  Seller profiles fetched: {len(seller_map)}")

        for rec in records:
            rec["cat_name"] = row["cat_name"]
            rec["level_1_name"] = row.get("level_1_name")
            rec["level_2_name"] = row.get("level_2_name")

            aid = rec.get("authorId")
            if aid is not None:
                aid_str = str(aid).strip()
                if aid_str in seller_map:
                    rec.update(seller_map[aid_str])

            ad_id = str(rec.get("id") or "unknown")
            image_urls = rec.get("imagesList", [])

            # rec["image_r2_paths"] = download_images(image_urls, ad_id, meta["r2_path"], dt=dt)
            # rec.pop("imagesList", None)

        groups[(meta["r2_path"], meta["file_key"])][meta["sheet_name"]].extend(records)

        if i < total:
            delay = random.uniform(MIN_LEAF_DELAY, MAX_LEAF_DELAY)
            print(f"  Waiting {delay:.1f}s before next leaf...")
            time.sleep(delay)

    print("\n" + "=" * 80)
    print(f"SCRAPING DONE -- uploading {len(groups)} file(s)")
    print("=" * 80)

    for (r2_path, file_key), sheets in groups.items():
        upload_group(r2_path, file_key, sheets, dt=dt)

    stats_file = f"request_stats_{cat_name_filter or 'all'}.json"
    stats = tracker.save(stats_file)
    print(f"\n--- Request Stats -> {stats_file} ---")
    print(f"Total: {stats['total_requests']} req | {stats['total_req_per_min']} req/min")
    print(f"By source: {stats['per_source']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--categories", default="haraj_all_categories.csv")
    parser.add_argument("--cat-name", default=None, help="Only process this cat_name (e.g. Cars)")
    parser.add_argument("--max-pages", type=int, default=None, help="Test limit: max pages per leaf")
    parser.add_argument("--limit-rows", type=int, default=None, help="Test limit: only first N leaf rows")
    parser.add_argument("--upload-target", default="r2", choices=["r2", "drive"],
                        help="Upload target: 'r2' (Cloudflare R2) or 'drive' (Google Drive)")
    args = parser.parse_args()

    run(
        categories_csv=args.categories,
        cat_name_filter=args.cat_name,
        max_pages=args.max_pages,
        limit_rows=args.limit_rows,
        upload_target=args.upload_target,
    )