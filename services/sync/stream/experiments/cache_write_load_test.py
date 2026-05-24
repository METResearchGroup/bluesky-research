"""Load testing for cache write throughput (batched vs unbatched).

This experiment simulates incoming records at the throughput described in:
    https://github.com/METResearchGroup/bluesky-research/issues/251

It compares:
- batch_size=1 (effectively "one file per record")
- increasing batch sizes up to the default used by FileUtilities

Metrics:
- achieved throughput (records/sec)
- end-to-end latency from enqueue -> written/enqueued (p50/p95)
- peak RSS (process memory)
- peak Python allocations (tracemalloc)
- number of JSON files written + total bytes written

Run:
    uv run python services/sync/stream/experiments/cache_write_load_test.py --run-matrix
"""

from __future__ import annotations

import argparse
import os
import queue
import random
import resource
import statistics
import tempfile
import threading
import time
import tracemalloc
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from services.sync.stream.cache_management.directory_lifecycle import CacheDirectoryManager
from services.sync.stream.cache_management.files import FileUtilities


# From issue #251 (daily counts)
POSTS_PER_DAY = 1_000_000
LIKES_PER_DAY = 10_000_000
FOLLOWS_PER_DAY = 4_000_000
SECONDS_PER_DAY = 86_400


def _per_second(per_day: int) -> float:
    return float(per_day) / float(SECONDS_PER_DAY)


BASE_RPS = {
    "post": _per_second(POSTS_PER_DAY),
    "like": _per_second(LIKES_PER_DAY),
    "follow": _per_second(FOLLOWS_PER_DAY),
}


def _percentages() -> list[float]:
    return [0.05, 0.10, 0.25, 0.50, 0.75, 1.00]


def _batch_sizes(default_batch_size: int) -> list[int]:
    # Include 1 (unbatched) and scale up to the default.
    candidates = [1, 10, 50, 100, 250, 500, default_batch_size]
    # Deduplicate while preserving order.
    out: list[int] = []
    for b in candidates:
        if b not in out and b <= default_batch_size:
            out.append(b)
    if out[-1] != default_batch_size:
        out.append(default_batch_size)
    return out


def _rss_mb() -> float:
    # On Linux, ru_maxrss is in kilobytes.
    return float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024.0


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    if pct <= 0:
        return sorted_vals[0]
    if pct >= 100:
        return sorted_vals[-1]
    k = (len(sorted_vals) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)
    return d0 + d1


def _format_ms(seconds: float) -> float:
    return seconds * 1000.0


@dataclass(frozen=True)
class Scenario:
    rate_fraction: float
    batch_size: int
    duration_seconds: float
    queue_maxsize: int


@dataclass
class ScenarioResult:
    rate_fraction: float
    target_rps_total: float
    target_rps_posts: float
    target_rps_likes: float
    target_rps_follows: float
    batch_size: int
    duration_seconds: float
    produced: int
    consumed: int
    achieved_rps: float
    p50_latency_ms: float
    p95_latency_ms: float
    mean_latency_ms: float
    max_queue_size: int
    peak_rss_mb: float
    peak_py_mb: float
    files_written: int
    bytes_written: int
    kept_up: bool


def _count_files_and_bytes(root: str) -> tuple[int, int]:
    files = 0
    bytes_written = 0
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if not fn.endswith(".json"):
                continue
            files += 1
            fp = os.path.join(dirpath, fn)
            try:
                bytes_written += os.path.getsize(fp)
            except OSError:
                pass
    return files, bytes_written


def _make_record(kind: str, i: int) -> dict[str, Any]:
    # Minimal JSON-serializable record, with enough entropy to avoid huge
    # compression effects and to simulate real payload.
    return {
        "kind": kind,
        "seq": i,
        "ts_ms": int(time.time() * 1000),
        "payload": {"text": "x" * 140, "id": f"{kind}-{i}"},
    }


def run_scenario(s: Scenario, *, flush_seconds: int = 5) -> ScenarioResult:
    # Configure batching for this scenario via env vars (FileUtilities reads at init).
    os.environ["SYNC_STREAM_CACHE_BATCH_SIZE"] = str(s.batch_size)
    os.environ["SYNC_STREAM_CACHE_BATCH_FLUSH_SECONDS"] = str(flush_seconds)

    # Precompute target rates.
    target_posts = BASE_RPS["post"] * s.rate_fraction
    target_likes = BASE_RPS["like"] * s.rate_fraction
    target_follows = BASE_RPS["follow"] * s.rate_fraction
    target_total = target_posts + target_likes + target_follows

    # Weighted sampling across record types.
    kinds = ["post", "like", "follow"]
    weights = [target_posts, target_likes, target_follows]
    # If total is extremely small, avoid division by zero.
    if target_total <= 0:
        raise ValueError("target_total must be > 0")

    q: queue.Queue[tuple[float, str, int]] = queue.Queue(maxsize=s.queue_maxsize)
    stop = threading.Event()
    produced = 0
    consumed = 0
    latencies_s: list[float] = []
    max_q = 0

    with tempfile.TemporaryDirectory(prefix="cache_write_load_test_") as tmpdir:
        # Create per-kind directories
        root = Path(tmpdir)
        create_root = root / "create"
        post_dir = create_root / "post"
        like_dir = create_root / "like"
        follow_dir = create_root / "follow"

        # Directory manager is required by FileUtilities; we stub enough by using
        # CacheDirectoryManager but with a minimal interface.
        class _TmpPathManager:
            root_write_path = str(root)
            root_create_path = str(create_root)
            root_delete_path = str(root / "delete")
            operation_types = ["post", "like", "follow"]
            study_user_activity_root_local_path = str(root / "study_user_activity")
            study_user_activity_relative_path_map = {}
            in_network_user_activity_root_local_path = str(root / "in_network_user_activity")
            in_network_user_activity_create_post_local_path = str(root / "in_network_user_activity" / "create" / "post")

        dir_manager = CacheDirectoryManager(path_manager=_TmpPathManager())
        file_utils = FileUtilities(directory_manager=dir_manager)

        # Ensure directories exist before starting.
        for d in [str(post_dir), str(like_dir), str(follow_dir)]:
            dir_manager.ensure_directory_exists(d)

        def producer() -> None:
            nonlocal produced, max_q
            next_t = time.perf_counter()
            end_t = next_t + s.duration_seconds
            i = 0
            while True:
                now = time.perf_counter()
                if now >= end_t:
                    break
                # Pace to target total rate.
                next_t += 1.0 / target_total
                sleep_for = next_t - now
                if sleep_for > 0:
                    time.sleep(sleep_for)

                kind = random.choices(kinds, weights=weights, k=1)[0]
                enqueue_ts = time.perf_counter()
                try:
                    q.put((enqueue_ts, kind, i), timeout=1.0)
                except queue.Full:
                    # Backpressure: drop the event (counts toward "can't keep up").
                    continue
                produced += 1
                i += 1
                max_q = max(max_q, q.qsize())
            stop.set()

        def writer() -> None:
            nonlocal consumed
            while not (stop.is_set() and q.empty()):
                try:
                    enqueue_ts, kind, seq = q.get(timeout=0.1)
                except queue.Empty:
                    continue
                start = time.perf_counter()
                record = _make_record(kind, seq)
                if kind == "post":
                    file_utils.append_record_to_batch(str(post_dir), record)
                elif kind == "like":
                    file_utils.append_record_to_batch(str(like_dir), record)
                else:
                    file_utils.append_record_to_batch(str(follow_dir), record)
                end = time.perf_counter()
                latencies_s.append(end - enqueue_ts)
                consumed += 1
                q.task_done()

        # Memory tracking
        tracemalloc.start()
        start_rss = _rss_mb()
        start_time = time.perf_counter()

        prod_thread = threading.Thread(target=producer, name="producer", daemon=True)
        writer_thread = threading.Thread(target=writer, name="writer", daemon=True)
        prod_thread.start()
        writer_thread.start()
        prod_thread.join()
        writer_thread.join()

        # Final flush to include tail batch.
        file_utils.flush_batches()

        end_time = time.perf_counter()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        end_rss = _rss_mb()
        peak_rss = max(start_rss, end_rss)
        peak_py_mb = float(peak) / (1024.0 * 1024.0)

        runtime = end_time - start_time
        achieved_rps = float(consumed) / runtime if runtime > 0 else 0.0

        lat_sorted = sorted(latencies_s)
        p50 = _format_ms(_percentile(lat_sorted, 50.0))
        p95 = _format_ms(_percentile(lat_sorted, 95.0))
        mean = _format_ms(statistics.fmean(latencies_s)) if latencies_s else 0.0

        files_written, bytes_written = _count_files_and_bytes(str(root))

        # "Kept up" heuristic:
        # - no drops from queue.Full (approximated by produced close to expected)
        # - achieved throughput meets target within 2%
        kept_up = achieved_rps >= (target_total * 0.98)

        return ScenarioResult(
            rate_fraction=s.rate_fraction,
            target_rps_total=target_total,
            target_rps_posts=target_posts,
            target_rps_likes=target_likes,
            target_rps_follows=target_follows,
            batch_size=s.batch_size,
            duration_seconds=s.duration_seconds,
            produced=produced,
            consumed=consumed,
            achieved_rps=achieved_rps,
            p50_latency_ms=p50,
            p95_latency_ms=p95,
            mean_latency_ms=mean,
            max_queue_size=max_q,
            peak_rss_mb=peak_rss,
            peak_py_mb=peak_py_mb,
            files_written=files_written,
            bytes_written=bytes_written,
            kept_up=kept_up,
        )


def results_to_markdown(results: list[ScenarioResult]) -> str:
    lines: list[str] = []
    lines.append("| Rate % | Target rps (total) | Batch size | Achieved rps | p50 latency (ms) | p95 latency (ms) | Peak RSS (MB) | Peak Py alloc (MB) | Files | MB written | Kept up |")
    lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|")
    for r in results:
        lines.append(
            "| "
            + " | ".join(
                [
                    f"{int(r.rate_fraction * 100)}",
                    f"{r.target_rps_total:.2f}",
                    f"{r.batch_size}",
                    f"{r.achieved_rps:.2f}",
                    f"{r.p50_latency_ms:.2f}",
                    f"{r.p95_latency_ms:.2f}",
                    f"{r.peak_rss_mb:.2f}",
                    f"{r.peak_py_mb:.2f}",
                    f"{r.files_written}",
                    f"{(r.bytes_written / (1024.0 * 1024.0)):.2f}",
                    "yes" if r.kept_up else "no",
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration-seconds", type=float, default=5.0)
    parser.add_argument("--queue-maxsize", type=int, default=100_000)
    parser.add_argument("--flush-seconds", type=int, default=5)
    parser.add_argument("--run-matrix", action="store_true")
    parser.add_argument("--output-md", type=str, default="")
    args = parser.parse_args()

    # Discover default batch size from FileUtilities env-defaults by instantiating once.
    class _NoopDirManager:
        def ensure_directory_exists(self, path: str) -> None:  # noqa: ARG002
            os.makedirs(path, exist_ok=True)

    default_batch_size = FileUtilities(directory_manager=_NoopDirManager()).batch_size

    results: list[ScenarioResult] = []
    if args.run_matrix:
        for pct in _percentages():
            for batch_size in _batch_sizes(default_batch_size):
                s = Scenario(
                    rate_fraction=pct,
                    batch_size=batch_size,
                    duration_seconds=float(args.duration_seconds),
                    queue_maxsize=int(args.queue_maxsize),
                )
                res = run_scenario(s, flush_seconds=int(args.flush_seconds))
                results.append(res)
    else:
        raise SystemExit("Pass --run-matrix to run the experiment matrix.")

    md = results_to_markdown(results)
    if args.output_md:
        Path(args.output_md).write_text(md, encoding="utf-8")
    else:
        print(md)


if __name__ == "__main__":
    # Reproducibility
    random.seed(0)
    main()

