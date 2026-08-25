import threading
import time
from collections import defaultdict


class RequestTracker:

    def __init__(self):
        self.lock = threading.Lock()
        self.records = []

    def log_request(self, source: str = "", success: bool = True, details: str = None):
        """
        Log a request attempt.

        Args:
            source: e.g. 'listing_pages', 'seller_profiles', 'images'
            success: True/False
            details: Optional string with error details (URL, status code, exception, etc.)
        """
        with self.lock:
            self.records.append({
                "worker": threading.current_thread().name,
                "source": source,
                "timestamp": time.time(),
                "success": success,
                "details": details or "",
            })

    def get_failed_requests(self) -> list[dict]:
        """Return all failed request records."""
        with self.lock:
            return [r for r in self.records if not r.get("success", True)]

    def print_failed_summary(self, max_per_source: int = 10):
        """
        Print a detailed summary of failed requests grouped by source.
        Shows unique error patterns to avoid spam.
        """
        failed = self.get_failed_requests()
        if not failed:
            print("  ✅ No failed requests!")
            return

        print(f"  ❌ Total failed requests: {len(failed)}")
        print()

        # Group by source
        by_source = defaultdict(list)
        for r in failed:
            by_source[r["source"]].append(r)

        for source, records in sorted(by_source.items(), key=lambda x: -len(x[1])):
            print(f"  📌 Source: '{source}' — {len(records)} failure(s)")

            # Show unique error details (deduped) with count
            detail_counts = defaultdict(int)
            for r in records:
                detail = r.get("details") or "Unknown error"
                detail_counts[detail] += 1

            # Print top distinct errors
            shown = 0
            for detail, count in sorted(detail_counts.items(), key=lambda x: -x[1]):
                if shown >= max_per_source:
                    remaining = len(detail_counts) - max_per_source
                    print(f"      ... and {remaining} more distinct error pattern(s)")
                    break
                prefix = f"      ({count}x)" if count > 1 else "      (1x)"
                print(f"{prefix} {detail}")
                shown += 1
            print()

    def summary(self) -> dict:
        with self.lock:
            if not self.records:
                return {"total_requests": 0, "per_worker": {}, "per_source": {}}

            per_worker = defaultdict(list)
            per_source_count = defaultdict(int)

            for r in self.records:
                per_worker[r["worker"]].append(r["timestamp"])
                per_source_count[r["source"] or "unknown"] += 1

            worker_stats = {}
            for worker, timestamps in per_worker.items():
                timestamps.sort()
                duration_min = (timestamps[-1] - timestamps[0]) / 60 if len(timestamps) > 1 else 0
                req_count = len(timestamps)
                req_per_min = req_count / duration_min if duration_min > 0 else req_count
                worker_stats[worker] = {
                    "requests": req_count,
                    "duration_min": round(duration_min, 2),
                    "req_per_min": round(req_per_min, 2)
                }

            all_ts = sorted(r["timestamp"] for r in self.records)
            total_duration_min = (all_ts[-1] - all_ts[0]) / 60 if len(all_ts) > 1 else 0
            total_req_per_min = len(all_ts) / total_duration_min if total_duration_min > 0 else len(all_ts)

            return {
                "total_requests": len(self.records),
                "total_duration_min": round(total_duration_min, 2),
                "total_req_per_min": round(total_req_per_min, 2),
                "per_worker": worker_stats,
                "per_source": dict(per_source_count)
            }

    def save(self, filepath: str):
        import json
        stats = self.summary()
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
        return stats


tracker = RequestTracker()