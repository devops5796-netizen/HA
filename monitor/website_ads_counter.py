"""Count website-published ads from Haraj's public GraphQL listing API."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Optional, Tuple

import requests

SOURCE_ID = "haraj_public_graphql"
REQUEST_TIMEOUT_SECONDS = 20
MAX_PAGES = 500

QUERY = """
query WebsitePublishedAds(
    $page: Int, $limit: Int, $afterPostDate: Int, $beforePostDate: Int
) {
    posts(
        page: $page, limit: $limit, afterPostDate: $afterPostDate,
        beforePostDate: $beforePostDate, orderMainByPostId: true
    ) {
        items { id postDate }
        pageInfo { hasNextPage }
    }
}
"""

HEADERS = {
    "accept": "*/*",
    "content-type": "application/json",
    "origin": "https://haraj.com.sa",
    "referer": "https://haraj.com.sa/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/151.0.0.0 Safari/537.36"
    ),
}


class WebsitePublishedAdsError(RuntimeError):
    """Raised when a complete, date-correct website count cannot be verified."""


def _date_bounds(listing_date: str) -> Tuple[int, int]:
    try:
        day = datetime.strptime(listing_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise WebsitePublishedAdsError("Listing date must use YYYY-MM-DD.") from exc

    start = int(day.timestamp())
    return start, start + 24 * 60 * 60


def _read_posts(response: Any) -> Tuple[list, bool]:
    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        raise WebsitePublishedAdsError(
            f"Haraj GraphQL request failed: {exc}"
        ) from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise WebsitePublishedAdsError("Haraj GraphQL returned invalid JSON.") from exc

    if not isinstance(payload, dict):
        raise WebsitePublishedAdsError("Haraj GraphQL response was not an object.")
    if payload.get("errors"):
        raise WebsitePublishedAdsError(
            f"Haraj GraphQL errors: {str(payload['errors'])[:180]}"
        )

    data = payload.get("data")
    posts = data.get("posts") if isinstance(data, dict) else None
    if not isinstance(posts, dict):
        raise WebsitePublishedAdsError("Haraj GraphQL response omitted data.posts.")

    items = posts.get("items")
    page_info = posts.get("pageInfo")
    if not isinstance(items, list) or not isinstance(page_info, dict):
        raise WebsitePublishedAdsError("Haraj GraphQL response has an invalid page shape.")
    has_next_page = page_info.get("hasNextPage")
    if not isinstance(has_next_page, bool):
        raise WebsitePublishedAdsError("Haraj GraphQL response omitted pageInfo.hasNextPage.")
    return items, has_next_page


def _post_identity_and_date(item: Any) -> Tuple[str, int]:
    if not isinstance(item, dict):
        raise WebsitePublishedAdsError("Haraj GraphQL returned a non-object listing.")

    listing_id = item.get("id")
    if isinstance(listing_id, bool) or not isinstance(listing_id, (int, str)):
        raise WebsitePublishedAdsError("Haraj listing omitted its stable id.")
    identity = str(listing_id).strip()
    if not identity:
        raise WebsitePublishedAdsError("Haraj listing returned an empty id.")

    raw_date = item.get("postDate")
    if isinstance(raw_date, bool) or not isinstance(raw_date, (int, float, str)):
        raise WebsitePublishedAdsError(f"Haraj listing {identity} omitted postDate.")
    try:
        timestamp = int(raw_date)
    except (TypeError, ValueError, OverflowError) as exc:
        raise WebsitePublishedAdsError(
            f"Haraj listing {identity} has an invalid postDate."
        ) from exc
    return identity, timestamp


def count_website_published_ads(
    source_config: Mapping[str, Any],
    listing_date: str,
    session: Optional[Any] = None,
) -> Dict[str, Any]:
    """Count distinct Haraj IDs whose UTC ``postDate`` falls on listing_date.

    Haraj's untagged ``posts`` query is site-wide, so all categories are covered
    without issuing a request per configured scraper category.
    """
    start, end = _date_bounds(listing_date)
    expected_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    if listing_date != expected_date.isoformat():
        raise WebsitePublishedAdsError(
            "Haraj public listings are only counted for yesterday UTC; "
            "historical availability is unverified."
        )

    if source_config.get("source") != SOURCE_ID:
        raise WebsitePublishedAdsError("Unsupported website published-ads source.")
    if source_config.get("scope") != "sitewide":
        raise WebsitePublishedAdsError("Haraj source must use sitewide category scope.")

    endpoint = source_config.get("endpoint")
    client_id = source_config.get("client_id")
    language = source_config.get("language", "en")
    if not isinstance(endpoint, str) or not endpoint.startswith("https://"):
        raise WebsitePublishedAdsError("Haraj GraphQL endpoint must use HTTPS.")
    if not isinstance(client_id, str) or not client_id.strip():
        raise WebsitePublishedAdsError("Haraj GraphQL client_id is not configured.")
    if not isinstance(language, str) or not language.strip():
        raise WebsitePublishedAdsError("Haraj GraphQL language is not configured.")

    page_size = source_config.get("page_size", 100)
    max_pages = source_config.get("max_pages", MAX_PAGES)
    delay_seconds = source_config.get("delay_seconds", 0.25)
    if isinstance(page_size, bool) or not isinstance(page_size, int) or page_size <= 0:
        raise WebsitePublishedAdsError("Haraj GraphQL page_size must be a positive integer.")
    if isinstance(max_pages, bool) or not isinstance(max_pages, int) or not 0 < max_pages <= MAX_PAGES:
        raise WebsitePublishedAdsError(f"Haraj GraphQL max_pages must be between 1 and {MAX_PAGES}.")
    if isinstance(delay_seconds, bool) or not isinstance(delay_seconds, (int, float)) or delay_seconds < 0:
        raise WebsitePublishedAdsError("Haraj GraphQL delay_seconds must be non-negative.")

    owned_session = session is None
    http = session or requests.Session()
    unique_ids = set()
    page_signatures = set()
    pages_scanned = 0
    ads_seen = 0
    duplicate_ids = 0

    try:
        for page in range(max_pages):
            variables = {
                "page": page,
                "limit": page_size,
                "afterPostDate": start - 1,
                "beforePostDate": end + 1,
            }
            try:
                response = http.post(
                    endpoint,
                    params={
                        "queryName": "posts",
                        "lang": language,
                        "clientId": client_id,
                    },
                    json={"query": QUERY, "variables": variables},
                    headers=HEADERS,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
            except requests.RequestException as exc:
                raise WebsitePublishedAdsError(
                    f"Haraj GraphQL request failed on page {page}: {exc}"
                ) from exc

            items, has_next_page = _read_posts(response)
            pages_scanned += 1
            page_ids = []
            for item in items:
                identity, timestamp = _post_identity_and_date(item)
                if timestamp < start - 1 or timestamp > end + 1:
                    raise WebsitePublishedAdsError(
                        "Haraj ignored the requested postDate window; "
                        f"page {page} contained an out-of-window listing."
                    )
                page_ids.append(identity)
                ads_seen += 1
                if start <= timestamp < end:
                    if identity in unique_ids:
                        duplicate_ids += 1
                    else:
                        unique_ids.add(identity)

            if not has_next_page:
                return {
                    "count": len(unique_ids),
                    "diagnostics": {
                        "pages_scanned": pages_scanned,
                        "ads_seen": ads_seen,
                        "duplicate_ids": duplicate_ids,
                        "category_scope": "sitewide",
                    },
                }

            signature = tuple(page_ids)
            if not signature or signature in page_signatures:
                raise WebsitePublishedAdsError(
                    f"Haraj pagination did not advance after page {page}."
                )
            page_signatures.add(signature)
            if page < max_pages - 1:
                time.sleep(float(delay_seconds))

        raise WebsitePublishedAdsError(
            f"Haraj pagination exceeded the {max_pages}-page safety limit."
        )
    finally:
        if owned_session:
            http.close()


def build_website_published_ads_report(
    source_config: Any,
    listing_date: str,
    session: Optional[Any] = None,
) -> Dict[str, Any]:
    """Build the site-level report fields, failing closed on source errors."""
    source = (
        source_config.get("source")
        if isinstance(source_config, Mapping)
        else SOURCE_ID
    )
    report: Dict[str, Any] = {
        "website_published_ads": None,
        "website_published_ads_date": listing_date,
        "website_published_ads_source": source if isinstance(source, str) else SOURCE_ID,
        "website_published_ads_status": "error",
    }
    if not isinstance(source_config, Mapping):
        report["website_published_ads_error"] = (
            "Website published-ads configuration must be a mapping."
        )
        return report
    try:
        result = count_website_published_ads(source_config, listing_date, session=session)
    except WebsitePublishedAdsError as exc:
        report["website_published_ads_error"] = str(exc)[:300]
        return report

    report["website_published_ads"] = result["count"]
    report["website_published_ads_status"] = "ok"
    report["website_published_ads_diagnostics"] = result["diagnostics"]
    return report
