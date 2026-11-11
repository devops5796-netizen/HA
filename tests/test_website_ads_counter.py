import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import requests

from monitor.website_ads_counter import (
    SOURCE_ID,
    WebsitePublishedAdsError,
    build_website_published_ads_report,
    count_website_published_ads,
)


def yesterday_bounds():
    day = datetime.now(timezone.utc).date() - timedelta(days=1)
    start = int(datetime.combine(day, datetime.min.time(), timezone.utc).timestamp())
    return day.isoformat(), start, start + 86400


def source_config():
    return {
        "source": SOURCE_ID,
        "scope": "sitewide",
        "endpoint": "https://graphql.haraj.com.sa/",
        "client_id": "public-client-id",
        "page_size": 100,
        "max_pages": 10,
        "delay_seconds": 0,
    }


class FakeResponse:
    def __init__(self, payload, error=None):
        self.payload = payload
        self.error = error

    def raise_for_status(self):
        if self.error:
            raise self.error

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def post(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return next(self.responses)


def response(items, has_next=False):
    return FakeResponse({
        "data": {
            "posts": {
                "items": items,
                "pageInfo": {"hasNextPage": has_next},
            }
        }
    })


class WebsiteAdsCounterTests(unittest.TestCase):
    def test_filters_publication_date_deduplicates_ids_and_scans_pages(self):
        listing_date, start, end = yesterday_bounds()
        session = FakeSession([
            response([
                {"id": 101, "postDate": start},
                {"id": 102, "postDate": end - 1},
                {"id": 103, "postDate": end},
            ], has_next=True),
            response([
                {"id": "101", "postDate": start},
                {"id": 104, "postDate": end - 2},
            ]),
        ])

        with patch("monitor.website_ads_counter.time.sleep"):
            result = count_website_published_ads(source_config(), listing_date, session)

        self.assertEqual(result["count"], 3)
        self.assertEqual(result["diagnostics"]["pages_scanned"], 2)
        self.assertEqual(result["diagnostics"]["ads_seen"], 5)
        self.assertEqual(result["diagnostics"]["duplicate_ids"], 1)
        self.assertEqual([call[1]["json"]["variables"]["page"] for call in session.calls], [0, 1])
        for call in session.calls:
            kwargs = call[1]
            variables = kwargs["json"]["variables"]
            self.assertNotIn("tag", variables)
            self.assertEqual(variables["afterPostDate"], start - 1)
            self.assertEqual(variables["beforePostDate"], end + 1)

    def test_empty_success_is_zero(self):
        listing_date = yesterday_bounds()[0]
        session = FakeSession([response([])])

        result = build_website_published_ads_report(source_config(), listing_date, session)

        self.assertEqual(result["website_published_ads"], 0)
        self.assertEqual(result["website_published_ads_status"], "ok")
        self.assertNotIn("website_published_ads_error", result)

    def test_source_failure_reports_null_count_and_error_status(self):
        listing_date = yesterday_bounds()[0]
        failure = requests.Timeout("request timed out")
        session = FakeSession([FakeResponse({}, error=failure)])

        result = build_website_published_ads_report(source_config(), listing_date, session)

        self.assertIsNone(result["website_published_ads"])
        self.assertEqual(result["website_published_ads_status"], "error")
        self.assertIn("request failed", result["website_published_ads_error"])

    def test_repeated_page_fails_instead_of_returning_partial_total(self):
        listing_date, start = yesterday_bounds()[:2]
        repeated_page = response([{"id": 201, "postDate": start}], has_next=True)
        session = FakeSession([repeated_page, repeated_page])

        with patch("monitor.website_ads_counter.time.sleep"):
            with self.assertRaisesRegex(WebsitePublishedAdsError, "pagination did not advance"):
                count_website_published_ads(source_config(), listing_date, session)

    def test_unverified_historical_date_is_not_counted(self):
        historical_date = (datetime.now(timezone.utc).date() - timedelta(days=2)).isoformat()
        session = FakeSession([])

        with self.assertRaisesRegex(WebsitePublishedAdsError, "historical availability is unverified"):
            count_website_published_ads(source_config(), historical_date, session)

        self.assertEqual(session.calls, [])

    def test_invalid_source_configuration_is_reported_as_error(self):
        listing_date = yesterday_bounds()[0]

        result = build_website_published_ads_report([], listing_date)

        self.assertIsNone(result["website_published_ads"])
        self.assertEqual(result["website_published_ads_status"], "error")
        self.assertIn("configuration must be a mapping", result["website_published_ads_error"])


if __name__ == "__main__":
    unittest.main()
