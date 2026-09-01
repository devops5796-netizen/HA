"""
haraj_pipeline.py
=========================
Unified scraper supporting 3 modes:
  1. scrape by cat         (top-level tag -> uncategorized)
  2. scrape by sub cat     (level-1 tag)
  3. scrape by sub cat + city (level-1 tag + city filter)

All data merged (deduped by ad id), then regrouped by sub-cat -> city.
Output per sub-cat: Excel (sheets=cities) + JSON + Summary.
"""

import argparse
import io
import json
import os
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

CITIES = {
    "Riyadh": "الرياض",
    "Eastern Region": "المنطقة الشرقية",
    "Eastern Region": "الشرقية",
    "Jeddah": "جدة",
    "Jeddah": "جده",
    "Makkah": "مكة",
    "Makkah": "مكه",
    "Yanbu": "ينبع",
    "Hafar Al Batin": "حفر الباطن",
    "Madinah": "المدينة",
    "Taif": "الطائف",
    "Taif": "الطايف",
    "Tabouk": "تبوك",
    "Qassim": "القصيم",
    "Hail": "حائل",
    "Abha": "أبها",
    "Aseer": "عسير",
    "Bahah": "الباحة",
    "Bahah": "الباحه",
    "Jazan": "جازان",
    "Jazan": "جيزان",
    "Najran": "نجران",
    "Jouf": "الجوف",
    "Arar": "عرعر",
    "Kuwait": "الكويت",
    "UAE": "الإمارات",
    "Bahrain": "البحرين",
}

# Reverse lookup so we can map a post's own (Arabic) `city` field back to
# our canonical English city key when the task itself didn't specify a city
# (i.e. "cat" and "subcat" modes, which don't filter by city).
CITY_AR_TO_EN = {ar: en for en, ar in CITIES.items()}

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
# FAILED TASKS TRACKER
# ============================================================


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


def clean_arabic(value):
    """Normalize alef/ya variants so tag strings compare equal regardless of
    hamza style (أ/إ/آ -> ا) or alef maksura vs ya (ى -> ي). Mirrors
    discover_categories.py's clean_arabic so it matches level_1_tag_name /
    level_2_tag_name values coming from haraj_all_categories.csv."""
    value = clean_text(value)
    value = value.replace("أ", "ا").replace("إ", "ا").replace("آ", "ا").replace("ى", "ي")
    return value.strip()


def resolve_city(raw_city) -> str:
    """Map a post's own (Arabic) `city` field to our canonical English city
    key using CITY_AR_TO_EN. Falls back to the cleaned raw value if it
    doesn't match a known city, or 'unknown' if empty."""
    city = clean_text(raw_city)
    if not city:
        return "unknown"
    return CITY_AR_TO_EN.get(city, city)


def clean_for_excel(value):
    if isinstance(value, str):
        return re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]", "", value)
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
        existing.add(base)
        return base
    counter = 2
    while True:
        suffix = f"_{counter}"
        candidate = base[:31 - len(suffix)] + suffix
        if candidate not in existing:
            existing.add(candidate)
            return candidate
        counter += 1


def serialize_value(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def build_post_row(post: dict) -> dict:
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
                tracker.log_request(source="images", success=False, details=f"Image HTTP {r.status_code}: {url[:100]}")
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
            tracker.log_request(source="images", success=False, details=f"Image exception ({type(e).__name__}): {url[:100]}")
        if idx < len(image_urls):
            time.sleep(random.uniform(MIN_IMAGE_DELAY, MAX_IMAGE_DELAY))
    return r2_paths


# ============================================================
# GRAPHQL SCRAPING
# ============================================================

def fetch_page(tag: str, page: int, city: str | None = None, before_update_date: int | None = None) -> tuple[list, bool, int | None]:
    params = {
        "queryName": "posts",
        "lang": LANG,
        "clientId": CLIENT_ID,
    }
    variables = {
        "tag": tag,
        "page": page,
        "orderMainByPostId": True,
    }
    if city:
        variables["city"] = city
    if before_update_date is not None:
        variables["beforeUpdateDate"] = before_update_date

    payload = {"query": QUERY, "variables": variables}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.post(BASE_URL, params=params, json=payload, timeout=60)
            response.raise_for_status()
            data = response.json()
            if "errors" in data:
                err_msg = str(data.get("errors", "unknown"))[:200]
                tracker.log_request(source="listing_pages", success=False, details=f"GraphQL errors: {err_msg}")
                return [], False, None
            posts_data = data.get("data", {}).get("posts", {})
            items = posts_data.get("items", [])
            has_next = posts_data.get("pageInfo", {}).get("hasNextPage", False)
            tracker.log_request(source="listing_pages", success=True)
            last_update_date = items[-1].get("updateDate") if items else None
            return items, has_next, last_update_date
        except requests.exceptions.RequestException as e:
            tracker.log_request(source="listing_pages", success=False, details=f"RequestException ({type(e).__name__}): {str(e)[:150]}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
            else:
                return [], False, None
        except Exception as e:
            tracker.log_request(source="listing_pages", success=False, details=f"Unexpected ({type(e).__name__}): {str(e)[:150]}")
            return [], False, None
    return [], False, None


def convert_timestamp_columns(df: pd.DataFrame) -> pd.DataFrame:
    timestamp_columns = ["postDate", "updateDate"]
    df = df.copy()
    for col in timestamp_columns:
        if col in df.columns:
            df[col] = (
                pd.to_datetime(
                    pd.to_numeric(df[col], errors="coerce"),
                    unit="s", errors="coerce", utc=True
                )
                .dt.tz_convert("Asia/Riyadh")
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )
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


def scrape_tag(tag: str, max_pages: int | None = None, city: str | None = None) -> list[dict]:
    all_posts = []
    seen_ids = set()
    page = 0
    before_update_date = None
    consecutive_empty = 0

    while True:
        if max_pages is not None and page >= max_pages:
            break
        items, has_next, last_update_date = fetch_page(tag, page, city=city, before_update_date=before_update_date)
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
        if not has_next:
            break
        if new_items == 0:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                break
        else:
            consecutive_empty = 0
        if last_update_date is not None:
            before_update_date = last_update_date
        page += 1
        time.sleep(random.uniform(MIN_REQUEST_DELAY, MAX_REQUEST_DELAY))
    return all_posts


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
                return None
            profile = data.get("data", {}).get("profile")
            return profile
        except requests.exceptions.RequestException:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
            else:
                return None
        except Exception:
            return None
    return None


def extract_phones(contacts: list) -> list[str]:
    """Extract phone numbers from contacts list."""
    phones = []
    for c in (contacts or []):
        if isinstance(c, dict) and c.get("type") == "PHONE" and c.get("info"):
            phones.append(str(c["info"]).strip())
    return phones


def flatten_seller_profile(profile: dict) -> dict:
    """Flatten seller profile. No seller_id. No flattened contacts.
    Returns: seller_contacts (JSON), phone (list), and other fields."""
    if not profile:
        return {}

    contacts = profile.get("contacts") or []
    phones = extract_phones(contacts)

    return {
        "seller_handler": profile.get("handler"),
        "seller_type": profile.get("type"),
        "seller_description": profile.get("description"),
        "seller_updatedAt": profile.get("updatedAt"),
        "seller_locations": json.dumps(profile.get("locations") or [], ensure_ascii=False),
        "seller_contacts": json.dumps(contacts, ensure_ascii=False),
        "seller_verifications": json.dumps(profile.get("verifications") or [], ensure_ascii=False),
        "seller_pages": json.dumps(profile.get("pages") or [], ensure_ascii=False),
        "phone": phones,
    }


def fetch_sellers_for_records(records: list[dict]) -> dict[str, dict]:
    unique_ids = set()
    for rec in records:
        aid = rec.get("authorId")
        if aid is not None:
            unique_ids.add(str(aid).strip())
    if not unique_ids:
        return {}
    sellers = {}
    success_count = 0
    fail_count = 0
    for idx, author_id in enumerate(sorted(unique_ids), 1):
        profile = fetch_seller_profile(author_id)
        if profile:
            sellers[author_id] = flatten_seller_profile(profile)
            success_count += 1
        else:
            fail_count += 1
        if idx < len(unique_ids):
            time.sleep(random.uniform(MIN_SELLER_DELAY, MAX_SELLER_DELAY))
    print(f"  Seller profiles: {success_count} success | {fail_count} failed | {len(unique_ids)} total")
    return sellers


# ============================================================
# TAG-BASED SUB-CAT / LEVEL-2 RESOLUTION
# ============================================================

def build_subcat_lookup(df: pd.DataFrame) -> dict[str, dict[str, str]]:
    """cat_name -> {clean_arabic(level_1_tag_name): level_1_name}

    Used to recover a real sub-category for records scraped in "cat" mode
    (top-level tag only, no sub-cat filter) by matching each post's own
    `tags` field against the known level-1 tags for that category.
    """
    lookup: dict[str, dict[str, str]] = defaultdict(dict)
    sub = df[df["level_1_tag_name"].notna()].drop_duplicates(subset=["cat_name", "level_1_tag_name"])
    for _, row in sub.iterrows():
        key = clean_arabic(row["level_1_tag_name"])
        lookup[row["cat_name"]][key] = row["level_1_name"]
    return dict(lookup)


def build_level2_lookup(df: pd.DataFrame) -> dict[tuple[str, str], dict[str, str]]:
    """(cat_name, level_1_name) -> {clean_arabic(level_2_tag_name): level_2_name}

    Used to recover the level-2 (e.g. car model, bird type) of ANY record --
    regardless of which mode scraped it -- since no scrape task ever filters
    by level-2 directly; it's always inferred from the post's own tags.
    """
    lookup: dict[tuple[str, str], dict[str, str]] = defaultdict(dict)
    sub = df[df["level_2_tag_name"].notna()].drop_duplicates(subset=["cat_name", "level_1_name", "level_2_tag_name"])
    for _, row in sub.iterrows():
        key = clean_arabic(row["level_2_tag_name"])
        lookup[(row["cat_name"], row["level_1_name"])][key] = row["level_2_name"]
    return dict(lookup)


def build_has_level2_set(df: pd.DataFrame) -> set[tuple[str, str]]:
    """Set of (cat_name, level_1_name) pairs that have at least one known
    level-2 leaf in the taxonomy. Drives whether a branch gets split into
    per-city files with level-2 sheets, or a single file with city sheets."""
    sub = df[df["level_2_name"].notna()]
    return set(zip(sub["cat_name"], sub["level_1_name"]))


def _parse_tags(raw_tags) -> list:
    if not raw_tags:
        return []
    tags = raw_tags
    if isinstance(tags, str):
        try:
            tags = json.loads(tags)
        except (json.JSONDecodeError, TypeError):
            return []
    if not isinstance(tags, (list, tuple)):
        return []
    return tags


def classify_cat_mode_subcat(raw_tags, cat_name: str, subcat_lookup: dict) -> str | None:
    """Try to recover the real level-1 sub-category of a "cat" mode record
    (no sub-cat filter was used for its scrape) by scanning its own `tags`
    field. Returns the matched level_1_name, or None if nothing matched."""
    cat_map = subcat_lookup.get(cat_name)
    if not cat_map:
        return None
    for t in _parse_tags(raw_tags):
        match = cat_map.get(clean_arabic(t))
        if match:
            return match
    return None


def classify_level2(raw_tags, cat_name: str, level_1_name: str | None, level2_lookup: dict) -> str | None:
    """Recover the level-2 leaf (car model, bird type, ...) of a record by
    scanning its own `tags` field against the known level-2 tags for its
    (cat_name, level_1_name) branch. Returns None if level_1_name is
    unknown, the branch has no level-2 taxonomy, or nothing matched."""
    if not level_1_name:
        return None
    cat_map = level2_lookup.get((cat_name, level_1_name))
    if not cat_map:
        return None
    for t in _parse_tags(raw_tags):
        match = cat_map.get(clean_arabic(t))
        if match:
            return match
    return None


# ============================================================
# FILE / FOLDER GROUPING RULES
# ============================================================
#
#   - cat with NO level_1 at all (leaf right at the top tag):
#       haraj/.../{cat}/excel/{cat}.xlsx            sheets = city
#
#   - cat has level_1, but this record couldn't be matched to any:
#       haraj/.../{cat}/Uncategorized/excel/Uncategorized.xlsx   sheets = city
#
#   - level_1 has NO level_2 anywhere (e.g. Furniture sub-cats):
#       haraj/.../{cat}/{level1}/excel/{level1}.xlsx  sheets = city
#
#   - level_1 HAS level_2 (e.g. Animals -> Parrot -> Zina birds/...):
#       haraj/.../{cat}/{level1}/excel/{level1}_{city}.xlsx   sheets = level_2
#       (one excel file PER CITY)
#
#   - Cars is special: brand-type level_1 values (Toyota, Nissan, ...) all
#     share ONE folder "Cars_for_sale_rent" instead of one folder per brand,
#     but keep the brand name in the filename and split sheets by model
#     (same level2_sheets mechanics as above). The 3 non-brand Cars level_1
#     values (Parts & Accessories / Trucks and heavy equipment /
#     Motorcycles) are NOT brands -- they follow the normal rule above with
#     their own dedicated folder.
# ============================================================

CARS_CAT_NAME = "Cars"
CARS_NON_BRAND_LEVEL1 = {"Parts & Accessories", "Trucks and heavy equipment", "Motorcycles"}
CARS_BRAND_BUCKET = "Cars_for_sale_rent"


def resolve_grouping(cat_name: str, level_1_name: str | None, has_level2_set: set) -> tuple[str, str, str]:
    """Return (r2_folder, file_label, granularity) for a record.
    granularity is 'level2_sheets' (one file per city, sheets=level_2) or
    'city_sheets' (one file total, sheets=city)."""
    if level_1_name is None:
        return cat_name, cat_name, "city_sheets"

    if level_1_name == "Uncategorized":
        return f"{cat_name}/Uncategorized", "Uncategorized", "city_sheets"

    has_l2 = (cat_name, level_1_name) in has_level2_set
    granularity = "level2_sheets" if has_l2 else "city_sheets"

    if cat_name == CARS_CAT_NAME and level_1_name not in CARS_NON_BRAND_LEVEL1:
        folder = f"{cat_name}/{CARS_BRAND_BUCKET}"
    else:
        folder = f"{cat_name}/{level_1_name}"

    return folder, level_1_name, granularity


# ============================================================
# TASK GENERATOR (3 modes)
# ============================================================

def generate_scrape_tasks(df: pd.DataFrame, modes: tuple = ("cat", "subcat", "subcat_city")) -> list[dict]:
    tasks = []
    if "cat" in modes:
        for _, row in df.drop_duplicates(subset=["cat_name"]).iterrows():
            tasks.append({
                "mode": "cat",
                "cat_name": row["cat_name"],
                "cat_tag": row["cat_tag_name"],
                "sub_cat": "Uncategorized",  # placeholder for logs only -- real per-record classification happens in run()
                "sub_cat_tag": None,
                "city_en": None,
                "city_ar": None,
                "tag": row["cat_tag_name"],
            })
    subcat_df = df[df["level_1_name"].notna()].drop_duplicates(subset=["cat_name", "level_1_name"])
    if "subcat" in modes:
        for _, row in subcat_df.iterrows():
            tasks.append({
                "mode": "subcat",
                "cat_name": row["cat_name"],
                "cat_tag": row["cat_tag_name"],
                "sub_cat": row["level_1_name"],
                "sub_cat_tag": row["level_1_tag_name"],
                "city_en": None,
                "city_ar": None,
                "tag": row["level_1_tag_name"] if pd.notna(row["level_1_tag_name"]) else row["cat_tag_name"],
            })
    if "subcat_city" in modes:
        for _, row in subcat_df.iterrows():
            tag = row["level_1_tag_name"] if pd.notna(row["level_1_tag_name"]) else row["cat_tag_name"]
            for city_en, city_ar in CITIES.items():
                tasks.append({
                    "mode": "subcat_city",
                    "cat_name": row["cat_name"],
                    "cat_tag": row["cat_tag_name"],
                    "sub_cat": row["level_1_name"],
                    "sub_cat_tag": row["level_1_tag_name"],
                    "city_en": city_en,
                    "city_ar": city_ar,
                    "tag": tag,
                })
    return tasks


# ============================================================
# DEDUP LOGIC: subcat_city > subcat > cat
# ============================================================

MODE_PRIORITY = {"cat": 1, "subcat": 2, "subcat_city": 3}

def should_replace(existing: dict, new: dict) -> bool:
    existing_mode = existing.get("_meta_mode", "cat")
    new_mode = new.get("_meta_mode", "cat")
    return MODE_PRIORITY.get(new_mode, 0) > MODE_PRIORITY.get(existing_mode, 0)


def merge_record(existing: dict, new: dict) -> dict:
    if should_replace(existing, new):
        return new
    return existing


# ============================================================
# COLUMN CLEANUP (before upload)
# ============================================================

COLUMNS_TO_DROP = [
    "cat_name",
    "level_1_name",
    "level_2_name",
    "seller_id",
    "_meta_mode",
    "_meta_cat",
    "_meta_sub_cat",
    "_meta_city",
    "bodyHTML",  #empty columns
]
# "postType",  # all is AD
# "status"     # all is True

SELLER_COLUMNS = [
    "seller_handler",
    "seller_type",
    "seller_description",
    "seller_updatedAt",
    "seller_locations",
    "seller_contacts",
    "seller_verifications",
    "seller_pages",
    "phone",
]

def strip_unwanted_columns(record: dict) -> dict:
    """Remove internal metadata + unwanted columns before upload."""
    result = dict(record)

    for col in COLUMNS_TO_DROP:
        result.pop(col, None)

    # Also drop any flattened seller_contact_* columns (safety net)
    for key in list(result.keys()):
        if key.startswith("seller_contact_"):
            result.pop(key, None)

    # Make sure all seller columns always exist
    for col in SELLER_COLUMNS:
        result.setdefault(col, None)

    return result

# ============================================================
# UPLOAD: one file with CITY sheets
# (leaf categories, level_1 with no level_2, "Uncategorized" bucket)
# ============================================================

def upload_subcat_group(r2_folder: str, file_label: str, cities: dict[str, list], dt: datetime) -> int:
    total_ads = sum(len(rows) for rows in cities.values())
    print(f"\n[{r2_folder}] {file_label}: {len(cities)} city sheet(s), {total_ads} ad(s)")
    for city, rows in cities.items():
        print(f"  - {city}: {len(rows)}")
    if total_ads == 0:
        print("  (nothing to upload)")
        return 0

    # Strip unwanted columns from every record
    cleaned_cities = {}
    for city_name, rows in cities.items():
        cleaned_cities[city_name] = [strip_unwanted_columns(r) for r in rows]

    excel_buf = io.BytesIO()
    used_names = set()
    with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
        for city_name, rows in cleaned_cities.items():
            if not rows:
                continue
            df = pd.DataFrame(rows)
            for col in df.columns:
                df[col] = df[col].map(clean_for_excel)
            name = unique_sheet_name(city_name, used_names)
            df.to_excel(writer, sheet_name=name, index=False)

    excel_key = upload_buffer(
        excel_buf,
        filename=f"{sanitize_filename(file_label)}.xlsx",
        r2_path=r2_folder,
        file_type="excel",
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        dt=dt,
    )
    print(f"  Excel -> {excel_key}")

    json_bytes = json.dumps(cleaned_cities, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    json_key = upload_buffer(
        io.BytesIO(json_bytes),
        filename=f"{sanitize_filename(file_label)}.json",
        r2_path=r2_folder,
        file_type="json",
        content_type="application/json",
        dt=dt,
    )
    print(f"  JSON  -> {json_key}")
    return total_ads


# ============================================================
# UPLOAD: one file PER CITY, with LEVEL-2 sheets
# (level_1 branches that have a level_2 taxonomy -- e.g. Animals/Parrot,
# Cars_for_sale_rent/Toyota, Cars/Trucks and heavy equipment, ...)
# ============================================================

def upload_level2_group(r2_folder: str, file_label: str, city: str, level2_map: dict[str, list], dt: datetime) -> int:
    total_ads = sum(len(rows) for rows in level2_map.values())
    file_base = f"{file_label}_{city}"
    print(f"\n[{r2_folder}] {file_base}: {len(level2_map)} level-2 sheet(s), {total_ads} ad(s)")
    for level_2, rows in level2_map.items():
        print(f"  - {level_2}: {len(rows)}")
    if total_ads == 0:
        print("  (nothing to upload)")
        return 0

    cleaned_level2 = {}
    for level_2_name, rows in level2_map.items():
        cleaned_level2[level_2_name] = [strip_unwanted_columns(r) for r in rows]

    excel_buf = io.BytesIO()
    used_names = set()
    with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
        for level_2_name, rows in cleaned_level2.items():
            if not rows:
                continue
            df = pd.DataFrame(rows)
            for col in df.columns:
                df[col] = df[col].map(clean_for_excel)
            name = unique_sheet_name(level_2_name, used_names)
            df.to_excel(writer, sheet_name=name, index=False)

    filename_base = f"{sanitize_filename(file_label)}_{sanitize_filename(city)}"
    excel_key = upload_buffer(
        excel_buf,
        filename=f"{filename_base}.xlsx",
        r2_path=r2_folder,
        file_type="excel",
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        dt=dt,
    )
    print(f"  Excel -> {excel_key}")

    json_bytes = json.dumps(cleaned_level2, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    json_key = upload_buffer(
        io.BytesIO(json_bytes),
        filename=f"{filename_base}.json",
        r2_path=r2_folder,
        file_type="json",
        content_type="application/json",
        dt=dt,
    )
    print(f"  JSON  -> {json_key}")
    return total_ads


# ============================================================
# SUMMARY
# ============================================================

def build_summary(cat_name: str, subcat_groups: dict, dt: datetime, cat_slug: str = None) -> dict:
    cat_slug = cat_slug or sanitize_filename(cat_name)

    subcategories = []
    total_listings = 0
    for sub_cat, cities in subcat_groups.items():
        count = sum(len(rows) for rows in cities.values())
        total_listings += count
        subcategories.append({
            "name": sub_cat,
            "listings_count": count,
            "cities": {city: len(rows) for city, rows in cities.items()},
        })

    # Calculate failed requests from tracker records (each has success: bool)
    stats = tracker.summary()
    total_requests = stats.get("total_requests", 0)
    failed_requests = 0
    try:
        with tracker.lock:
            failed_requests = sum(1 for r in tracker.records if not r.get("success", True))
    except Exception:
        pass

    duration_min = stats.get("total_duration_min", 0)
    requests_duration_sec = duration_min * 60 if duration_min else None

    request_metrics = {
        "requests_total": total_requests,
        "requests_failed": failed_requests,
        "duration_sec": stats.get("total_duration", 0),
        "requests_per_min": stats.get("total_req_per_min", 0),
        "requests_duration_sec": requests_duration_sec,
    }

    if total_requests > 0:
        request_metrics["error_rate_pct"] = round(failed_requests / total_requests * 100, 2)
    else:
        request_metrics["error_rate_pct"] = None

    if requests_duration_sec and requests_duration_sec > 0:
        request_metrics["requests_per_min"] = round(
            total_requests / (requests_duration_sec / 60.0), 2
        )

    return {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "data_scraped_date": TARGET_DATE.strftime("%Y-%m-%d"),
        "saved_to_R2_date": dt.strftime("%Y-%m-%d"),
        "category": {
            "name_en": cat_name,
            "name_ar": cat_name,
            "slug": cat_slug,
            "r2_path": cat_name,
        },
        "workflow_name": "haraj",
        "total_subcategories": len(subcategories),
        "total_listings": total_listings,
        "subcategories": subcategories,
        "request_metrics": request_metrics,
    }


def upload_summary(cat_name: str, summary: dict, dt: datetime):
    summary_bytes = json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8")
    key = upload_buffer(
        io.BytesIO(summary_bytes),
        filename="summary.json",
        r2_path=cat_name,
        file_type="summary",
        content_type="application/json",
        dt=dt,
    )
    print(f"  Summary -> {key}")


# ============================================================
# MAIN
# ============================================================

def apply_shard(df: pd.DataFrame, shard_index: int, shard_count: int) -> pd.DataFrame:
    """Split a category's rows across `shard_count` parallel jobs by
    level_1_name, so one huge category (like Cars, ~90+ brands) can be
    scraped by several jobs in parallel instead of one job timing out at
    GitHub Actions' 6h job limit.

    Deterministic: sorts the level_1 values first, so every shard job (which
    only knows its own index) computes the exact same partition independent
    of row order in the CSV.
    """
    if shard_count <= 1:
        return df
    NONE_MARKER = "\x00__NONE__"
    level1_values = sorted(df["level_1_name"].fillna(NONE_MARKER).unique())
    my_values = {v for i, v in enumerate(level1_values) if i % shard_count == shard_index}
    mask = df["level_1_name"].fillna(NONE_MARKER).isin(my_values)
    return df[mask]


def run(
    categories_csv: str = "haraj_all_categories.csv",
    cat_name_filter: str | None = None,
    max_pages: int | None = None,
    limit_tasks: int | None = None,
    dt: datetime | None = None,
    upload_target: str = "r2",
    modes: tuple = ("cat", "subcat", "subcat_city"),
    skip_summary: bool = False,
    shard_index: int = 0,
    shard_count: int = 1,
) -> None:
    set_upload_target(upload_target)
    dt = dt or datetime.now()

    df = pd.read_csv(categories_csv, encoding="utf-8-sig")
    if cat_name_filter:
        df = df[df["cat_name"] == cat_name_filter]
    if shard_count > 1:
        df = apply_shard(df, shard_index, shard_count)
        print(f"Shard {shard_index}/{shard_count}: {df['level_1_name'].nunique()} level_1 value(s), {len(df)} row(s)")

    subcat_lookup = build_subcat_lookup(df)          # cat -> {tag: level_1_name}
    level2_lookup = build_level2_lookup(df)           # (cat, level_1) -> {tag: level_2_name}
    has_level2_set = build_has_level2_set(df)          # {(cat, level_1), ...}
    cats_with_taxonomy = set(subcat_lookup.keys())     # cats that have ANY level_1 at all

    tasks = generate_scrape_tasks(df, modes=modes)
    if limit_tasks:
        tasks = tasks[:limit_tasks]

    total_tasks = len(tasks)
    print(f"Total scrape tasks: {total_tasks} (modes={modes})")

    all_records = {}
    for i, task in enumerate(tasks, 1):
        print("\n" + "#" * 80)
        print(f"[{i}/{total_tasks}] mode={task['mode']} | cat={task['cat_name']} | sub={task['sub_cat']} | city={task['city_en'] or 'ALL'}")
        print(f"  tag={task['tag']}")
        print("#" * 80)

        try:
            records = scrape_tag(task["tag"], max_pages=max_pages, city=task["city_ar"])
        except Exception as e:
            print(f"  [ERROR] scrape failed: {e}")
            if i < total_tasks:
                time.sleep(random.uniform(MIN_LEAF_DELAY, MAX_LEAF_DELAY))
            continue

        before_filter = len(records)
        records = filter_yesterday_hits(records)
        print(f"  Date filter: {before_filter} -> {len(records)} ads (postDate = {TARGET_DATE})")

        for rec in records:
            rec["_meta_mode"] = task["mode"]
            rec["_meta_cat"] = task["cat_name"]

            if task["mode"] == "cat":
                # No sub-cat filter was used for this scrape -- try to
                # recover the real level-1 sub-category from the post's own
                # tags before giving up.
                matched_l1 = classify_cat_mode_subcat(rec.get("tags"), task["cat_name"], subcat_lookup)
                if task["cat_name"] not in cats_with_taxonomy:
                    # This category has NO level_1 taxonomy at all -- it's a
                    # true leaf category, not "uncategorized".
                    rec["_meta_sub_cat"] = None
                else:
                    rec["_meta_sub_cat"] = matched_l1 or "Uncategorized"
            else:
                # subcat / subcat_city tasks always carry a real level_1.
                rec["_meta_sub_cat"] = task["sub_cat"]

            # Level-2 (model / bird type / ...) is never targeted by a scrape
            # task directly -- always inferred from the post's own tags,
            # scoped to whatever level_1 branch we just settled on.
            level_1_for_l2 = rec["_meta_sub_cat"] if rec["_meta_sub_cat"] not in (None, "Uncategorized") else None
            rec["_meta_level2"] = classify_level2(rec.get("tags"), task["cat_name"], level_1_for_l2, level2_lookup)

            # subcat_city tasks already filtered by a specific city, so trust
            # the task. Otherwise (cat/subcat modes) resolve the post's own
            # `city` field to our canonical English city key.
            rec["_meta_city"] = task["city_en"] if task["city_en"] else resolve_city(rec.get("city"))

            ad_id = str(rec.get("id", ""))
            if not ad_id:
                continue

            if ad_id in all_records:
                all_records[ad_id] = merge_record(all_records[ad_id], rec)
            else:
                all_records[ad_id] = rec

        if i < total_tasks:
            delay = random.uniform(MIN_LEAF_DELAY, MAX_LEAF_DELAY)
            print(f"  Waiting {delay:.1f}s before next task...")
            time.sleep(delay)

    print("\n" + "=" * 80)
    print(f"SCRAPING DONE — unique ads collected: {len(all_records)}")
    print("=" * 80)

    if not all_records:
        print("No ads found. Exiting.")
        return

    records_list = list(all_records.values())
    seller_map = fetch_sellers_for_records(records_list)
    print(f"\nSeller profiles fetched: {len(seller_map)}")

    for rec in records_list:
        aid = rec.get("authorId")
        if aid is not None:
            aid_str = str(aid).strip()
            if aid_str in seller_map:
                rec.update(seller_map[aid_str])

    cat_slug = sanitize_filename(cat_name_filter) if cat_name_filter else "all"
    if shard_count > 1:
        cat_slug = f"{cat_slug}_shard{shard_index}"

    stats_file = f"request_stats_{cat_slug}.json"
    tracker.save(stats_file)

    # --------------------------------------------------------------
    # Grouping for UPLOAD (drives the actual file/folder layout):
    #   city_sheets groups:   (folder, label) -> {city: [records]}
    #   level2_sheets groups: (folder, label) -> {city: {level_2: [records]}}
    # --------------------------------------------------------------
    city_sheet_groups: dict[tuple[str, str], dict[str, list]] = defaultdict(lambda: defaultdict(list))
    level2_sheet_groups: dict[tuple[str, str], dict[str, dict[str, list]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    # Separate, simpler aggregation purely for the summary report -- keyed by
    # level_1 only (ignores level_2 / the Cars brand-bucket redirect), so the
    # summary keeps reporting one entry per real sub-category regardless of
    # how files ended up physically split.
    summary_groups: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))

    for rec in records_list:
        cat = rec.get("_meta_cat", "Unknown")
        level_1 = rec.get("_meta_sub_cat")
        city = rec.get("_meta_city") or "unknown"

        summary_groups[level_1 if level_1 is not None else cat][city].append(rec)

        folder, label, granularity = resolve_grouping(cat, level_1, has_level2_set)
        if granularity == "level2_sheets":
            level_2 = rec.get("_meta_level2") or "Other"
            level2_sheet_groups[(folder, label)][city][level_2].append(rec)
        else:
            city_sheet_groups[(folder, label)][city].append(rec)

    total_uploaded = 0

    print("\n" + "=" * 80)
    print(f"UPLOADING category: {cat_name_filter or 'all'}")
    print("=" * 80)

    for (folder, label), cities in city_sheet_groups.items():
        total_uploaded += upload_subcat_group(folder, label, cities, dt=dt)

    for (folder, label), city_map in level2_sheet_groups.items():
        for city, level2_map in city_map.items():
            total_uploaded += upload_level2_group(folder, label, city, level2_map, dt=dt)

    summary = build_summary(cat_name_filter or "all", summary_groups, dt, cat_slug=cat_slug)

    if skip_summary:
        placeholder_path = f"summary_placeholder_{cat_slug}.json"
        with open(placeholder_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        print(f"✅ Summary placeholder saved: {placeholder_path}")
    else:
        upload_summary(cat_name_filter or "all", summary, dt)

    print("\n" + "=" * 80)
    print(f"GLOBAL STATS (Scraping Only)")
    stats = tracker.summary()
    print(f"Total requests : {stats['total_requests']}")
    print(f"Per source     : {stats.get('per_source', {})}")
    print(f"Duration       : {stats.get('total_duration_min', 0):.2f} min")
    print("=" * 80)

    print("\n" + "=" * 80)
    print("FAILED SCRAPING REQUESTS DETAIL")
    print("=" * 80)
    tracker.print_failed_summary()
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--categories", default="haraj_all_categories.csv")
    parser.add_argument("--cat-name", default=None)
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--limit-tasks", type=int, default=None)
    parser.add_argument("--modes", default="cat,subcat,subcat_city")
    parser.add_argument("--upload-target", default="r2", choices=["r2", "drive"])
    parser.add_argument("--skip-summary", action="store_true",
                        help="Save summary placeholder instead of uploading")
    parser.add_argument("--shard-index", type=int, default=0,
                        help="This job's shard number (0-based) when splitting one category's level_1 branches across multiple jobs")
    parser.add_argument("--shard-count", type=int, default=1,
                        help="Total number of shards for --cat-name (e.g. 6 to split Cars' ~90 brands into 6 parallel jobs)")
    args = parser.parse_args()

    modes = tuple(m.strip() for m in args.modes.split(","))
    run(
        categories_csv=args.categories,
        cat_name_filter=args.cat_name,
        max_pages=args.max_pages,
        limit_tasks=args.limit_tasks,
        upload_target=args.upload_target,
        modes=modes,
        skip_summary=args.skip_summary,
        shard_index=args.shard_index,
        shard_count=args.shard_count,
    )