"""
discover_categories.py
=======================
Generalized version of the old scraping.py: instead of walking only
"حراج السيارات" (Cars), this walks EVERY top-level tag and records
whatever category depth actually exists for it:

- top tag -> level 1 -> level 2   (most categories, e.g. Cars -> Toyota -> Camry)
- top tag -> level 1 only          (some categories have no level 2 at all)
- top tag only                     (some categories have no children at all)

Output: haraj_all_categories.csv, one row per LEAF (the deepest node found),
so the scraper (haraj_pipeline.py) knows exactly which tag to query ads with
for every branch of the tree.
"""

import random
import re
import time
import unicodedata
from urllib.parse import urljoin, quote, unquote

import pandas as pd
import requests
from bs4 import BeautifulSoup

from request_tracker import tracker

BASE_URL = "https://haraj.com.sa"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/151.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
}

# ============================================================
# Top-level tags -> English folder name.
# Best-effort translation from main.py's TAGS list -- PLEASE REVIEW
# and correct any of these before the first real run, since these
# become actual R2 folder names.
# ============================================================
TAGS = {
    "حراج السيارات": "Cars",
    "حراج الأجهزة": "Devices",
    "حراج العقار": "Real_Estate",
    "اثاث": "Furniture",
    "خدمات": "Services",
    "مستلزمات شخصية": "Fashion",
    "مواشي وحيوانات وطيور": "Animals",
    "وظائف": "Jobs",
    "العاب وترفيه": "Games",
    "تعليم وتدريب": "Coaching",
    "اطعمة ومشروبات": "Food",
    "حفلات ومناسبات": "Events",
    "برمجة وتصاميم": "Coding",
    "زراعة وحدائق": "Gardens",
    "نوادر و تراثيات": "Rarities",
    "مشاريع واستثمارات": "Projects_and_Investments",
    "مكتبة وفنون": "Arts",
    "صيد ورحلات": "Trips",
    "سفر وسياحة": "Tourism",
    "مفقودات": "Lost_and_Found",
    "قسم غير مصنف": "More",
}


def clean_text(text):
    if not text:
        return ""
    text = str(text)
    for zw in ("\u200b", "\u200c", "\u200d", "\ufeff"):
        text = text.replace(zw, "")
    text = unicodedata.normalize("NFKC", text)
    if any(x in text for x in ["Ø", "Ù", "Ã", "Â", "â"]):
        try:
            text = text.encode("latin1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_arabic(text):
    text = clean_text(text)
    text = text.replace("أ", "ا").replace("إ", "ا").replace("آ", "ا").replace("ى", "ي")
    return text.strip()


def get_soup(url):
    print(f"GET: {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        print(f"  Status: {response.status_code}")
        response.raise_for_status()
    except Exception:
        tracker.log_request(source="category_discovery", success=False)
        raise
    tracker.log_request(source="category_discovery", success=True)
    response.encoding = response.apparent_encoding or "utf-8"
    return BeautifulSoup(response.text, "html.parser")


def get_children(page_url: str, testid: str) -> list[dict]:
    """
    Generic child-tag fetcher. `testid` is "child-tags-level-1" when
    reading a TOP TAG page, or "child-tags-level-2" when reading a
    LEVEL-1 category's own page -- Haraj's HTML labels the container by
    the depth of the CHILDREN it holds, not by the page's own depth.
    """
    soup = get_soup(page_url)
    container = soup.select_one(f'[data-testid="{testid}"]')
    if not container:
        return []

    links = container.select('a[href*="/en/tags/"]')
    seen = set()
    out = []
    for link in links:
        href = link.get("href")
        if not href or "/en/tags/" not in href:
            continue
        full_url = urljoin(BASE_URL, href)
        if full_url in seen:
            continue
        seen.add(full_url)

        span = link.select_one("span")
        name = clean_text(span.get_text(" ", strip=True)) if span else clean_text(link.get_text(" ", strip=True))
        if not name:
            continue

        tag_name_raw = href.split("/en/tags/", 1)[-1]
        tag_name = clean_arabic(unquote(tag_name_raw))

        out.append({"name": name, "tag_name": tag_name, "url": full_url})
    return out


def discover_all(tags: dict = TAGS, delay_between_categories: tuple[float, float] = (4.0, 8.0),
                  delay_between_level1: tuple[float, float] = (4.0, 8.0)) -> pd.DataFrame:
    rows = []

    for cat_tag, cat_name in tags.items():
        cat_url = f"{BASE_URL}/en/tags/{quote(cat_tag)}"
        print("\n" + "=" * 70)
        print(f"Category: {cat_name}  ({cat_tag})")
        print("=" * 70)

        try:
            level_1_list = get_children(cat_url, "child-tags-level-1")
        except Exception as e:
            print(f"  ERROR fetching level 1 for {cat_name}: {e}")
            level_1_list = []

        print(f"  Level 1 found: {len(level_1_list)}")

        if not level_1_list:
            # Leaf right at the top tag -- nothing deeper to scrape.
            rows.append({
                "cat_name": cat_name, "cat_tag_name": cat_tag,
                "level_1_name": None, "level_1_tag_name": None, "level_1_url": None,
                "level_2_name": None, "level_2_tag_name": None, "level_2_url": None,
            })
            time.sleep(random.uniform(*delay_between_categories))
            continue

        for i, l1 in enumerate(level_1_list, 1):
            print(f"  [{i}/{len(level_1_list)}] Level 1: {l1['name']}")
            try:
                level_2_list = get_children(l1["url"], "child-tags-level-2")
            except Exception as e:
                print(f"    ERROR fetching level 2 for {l1['name']}: {e}")
                level_2_list = []

            if not level_2_list:
                # Leaf at level 1 -- this category has no level 2.
                rows.append({
                    "cat_name": cat_name, "cat_tag_name": cat_tag,
                    "level_1_name": l1["name"], "level_1_tag_name": l1["tag_name"], "level_1_url": l1["url"],
                    "level_2_name": None, "level_2_tag_name": None, "level_2_url": None,
                })
            else:
                print(f"    Level 2 found: {len(level_2_list)}")
                for l2 in level_2_list:
                    rows.append({
                        "cat_name": cat_name, "cat_tag_name": cat_tag,
                        "level_1_name": l1["name"], "level_1_tag_name": l1["tag_name"], "level_1_url": l1["url"],
                        "level_2_name": l2["name"], "level_2_tag_name": l2["tag_name"], "level_2_url": l2["url"],
                    })

            if i < len(level_1_list):
                time.sleep(random.uniform(*delay_between_level1))

        time.sleep(random.uniform(*delay_between_categories))

    df = pd.DataFrame(rows).drop_duplicates().reset_index(drop=True)

    output_file = "haraj_all_categories.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)
    print(f"Total leaf rows: {len(df)}")
    print(f"Saved to: {output_file}")

    # Quick sanity summary
    print("\nRows per category:")
    print(df.groupby("cat_name").size().sort_values(ascending=False).to_string())

    stats_file = "request_stats_discover_categories.json"
    stats = tracker.save(stats_file)
    print(f"\n--- Request Stats -> {stats_file} ---")
    print(f"Total: {stats['total_requests']} req | {stats['total_req_per_min']} req/min")
    print(f"By source: {stats['per_source']}")

    return df


if __name__ == "__main__":
    discover_all()