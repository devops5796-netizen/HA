#!/usr/bin/env python3
"""
Finalize summaries: add workflow duration and upload to R2.
Mirrors finalize_summaries.py from DKSA but adapted for Haraj.

Sharded categories (e.g. Cars, split across N jobs via --shard-index /
--shard-count) each produce their OWN summary_placeholder_{cat}_shardK.json.
Before uploading, this script groups all placeholder files by their real
r2_path (e.g. "Cars") and MERGES same-category shards into a single summary
-- otherwise each shard's upload would just overwrite the previous one in R2
and we'd lose every shard's counts except the last one processed.
"""

import argparse
import json
import os
import glob
import io
from collections import defaultdict
from datetime import datetime, timezone

from r2_uploader import upload_buffer


def load_summary(filepath: str) -> dict:
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def merge_summaries(summaries: list[dict]) -> dict:
    """Merge one or more shard summaries that belong to the SAME category
    (same r2_path) into a single combined summary."""
    if len(summaries) == 1:
        return summaries[0]

    merged = dict(summaries[0])

    # Merge subcategories by name, summing listings_count and per-city counts
    subcat_order: list[str] = []
    subcat_map: dict[str, dict] = {}
    for s in summaries:
        for sub in s.get("subcategories", []):
            name = sub["name"]
            if name not in subcat_map:
                subcat_map[name] = {"name": name, "listings_count": 0, "cities": {}}
                subcat_order.append(name)
            entry = subcat_map[name]
            entry["listings_count"] += sub.get("listings_count", 0)
            for city, count in sub.get("cities", {}).items():
                entry["cities"][city] = entry["cities"].get(city, 0) + count

    merged["subcategories"] = [subcat_map[name] for name in subcat_order]
    merged["total_subcategories"] = len(merged["subcategories"])
    merged["total_listings"] = sum(sub["listings_count"] for sub in merged["subcategories"])

    # Sum request metrics across shards, then recompute the derived rates
    total_requests = sum((s.get("request_metrics", {}).get("requests_total") or 0) for s in summaries)
    total_failed = sum((s.get("request_metrics", {}).get("requests_failed") or 0) for s in summaries)
    total_duration = sum((s.get("request_metrics", {}).get("duration_sec") or 0) for s in summaries)

    merged["request_metrics"] = {
        "requests_total": total_requests,
        "requests_failed": total_failed,
        "duration_sec": total_duration,
        "requests_per_min": round(total_requests / (total_duration / 60.0), 2) if total_duration else 0,
        "requests_duration_sec": total_duration or None,
        "error_rate_pct": round(total_failed / total_requests * 100, 2) if total_requests else None,
    }

    # Keep the most recent scrape timestamp among the shards
    merged["scraped_at"] = max((s.get("scraped_at") or "" for s in summaries), default="")

    return merged


def finalize_summaries(summaries_dir: str, date_str: str, workflow_name: str = None):
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    workflow_duration = os.getenv("WORKFLOW_DURATION")
    if not workflow_duration:
        print("⚠️ WORKFLOW_DURATION not set. Using fallback.")
        workflow_duration = "0"

    try:
        duration_sec = float(workflow_duration)
    except ValueError:
        duration_sec = 0.0

    print(f"✅ Workflow duration: {duration_sec}s")

    pattern = os.path.join(summaries_dir, "summary_placeholder_*.json")
    summary_files = glob.glob(pattern)

    if not summary_files:
        print(f"No summary placeholder files found in {summaries_dir}")
        return

    # Group by the REAL category identity (r2_path), not by filename -- a
    # sharded category produces several files (Cars_shard0, Cars_shard1, ...)
    # that all share the same r2_path and must be merged, not each uploaded
    # separately (which would just clobber each other in R2).
    groups: dict[str, list[dict]] = defaultdict(list)
    for filepath in summary_files:
        basename = os.path.basename(filepath)
        fallback_category = basename.replace("summary_placeholder_", "").replace(".json", "")
        summary = load_summary(filepath)
        r2_path = (
            summary.get("category", {}).get("r2_path")
            or summary.get("category", {}).get("name_en")
            or fallback_category
        )
        groups[r2_path].append(summary)

    print(f"Found {len(summary_files)} summary file(s) across {len(groups)} categor(y/ies)")

    for r2_path, summaries in groups.items():
        shard_note = f" ({len(summaries)} shard(s) merged)" if len(summaries) > 1 else ""
        print(f"  Processing: {r2_path}{shard_note}")

        summary = merge_summaries(summaries)

        if "request_metrics" not in summary:
            summary["request_metrics"] = {}

        summary["request_metrics"]["duration_sec"] = duration_sec

        if workflow_name:
            summary["workflow_name"] = workflow_name

        summary_bytes = json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8")
        summary_key = upload_buffer(
            io.BytesIO(summary_bytes),
            filename="summary.json",
            r2_path=r2_path,
            file_type="summary",
            content_type="application/json",
            dt=dt,
        )
        print(f"    Uploaded: {summary_key}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Finalize summaries with workflow duration")
    parser.add_argument("--summaries-dir", default="summaries/", help="Directory with summary placeholders")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD data date")
    parser.add_argument("--workflow", default="haraj", help="Workflow name")
    args = parser.parse_args()

    finalize_summaries(args.summaries_dir, args.date, args.workflow)