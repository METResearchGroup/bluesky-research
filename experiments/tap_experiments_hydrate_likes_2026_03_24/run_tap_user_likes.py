"""
Enumerate likes for seed DIDs via Tap (Phase A), hydrate liked posts via TapRepoSync.wait_for_posts (Phase B), write Parquet.

Requires Tap (indigo cmd/tap) running separately, e.g.:
  cd indigo && go run ./cmd/tap run --no-replay --disable-acks=true
Optional: TAP_COLLECTION_FILTERS=app.bsky.feed.like,app.bsky.feed.post
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import pandas as pd
import requests

SCRIPT_DIR = Path(__file__).resolve().parent
EXPERIMENTS_DIR = SCRIPT_DIR.parents[0]
TAP_CLIENT_DIR = EXPERIMENTS_DIR / "tap_experiments_2026_02_24"
if str(TAP_CLIENT_DIR) not in sys.path:
    sys.path.insert(0, str(TAP_CLIENT_DIR))

from tap_client import TapRepoSync, extract_did_from_post_uri  # noqa: E402

DEFAULT_SEED_DIDS = [
    "did:plc:w5mjarupsl6ihdrzwgnzdh4y",
    "did:plc:e4itbqoxctxwrrfqgs2rauga",
    "did:plc:gedsnv7yxi45a4g2gts37vyp",
    "did:plc:fbnm4hjnzu4qwg3nfjfkdhay",
]


def _load_seed_dids(path: Path | None) -> list[str]:
    if path is None:
        return list(DEFAULT_SEED_DIDS)
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    dids = data.get("dids")
    if not isinstance(dids, list) or not all(isinstance(d, str) for d in dids):
        raise ValueError("JSON must contain a 'dids' array of strings")
    return dids


def _check_tap_health(base_url: str) -> None:
    url = f"{base_url.rstrip('/')}/health"
    try:
        r = requests.get(url, timeout=5)
        r.raise_for_status()
        body = r.json()
        if body.get("status") != "ok":
            raise SystemExit(f"Unexpected health payload: {body}")
    except requests.RequestException as e:
        raise SystemExit(
            f"Tap health check failed ({url}). Start Tap first, e.g. "
            "`go run ./cmd/tap run --no-replay --disable-acks=true` in indigo."
        ) from e


async def _run(
    *,
    base_url: str,
    seed_dids: list[str],
    output_dir: Path,
    timeout_likes_sec: float,
    timeout_posts_sec: float,
) -> None:
    tap = TapRepoSync(base_url=base_url)

    print(f"Phase A: collecting likes for {len(seed_dids)} DIDs via Tap…", flush=True)
    likes_map = await tap.collect_likes_for_dids(
        seed_dids,
        max_wait_sec=timeout_likes_sec,
    )
    print(f"Phase A: distinct like records (post targets): {len(likes_map)}", flush=True)

    rows_likes = [
        {"user_did": t[0], "like_uri": uri, "liked_post_uri": t[1]}
        for uri, t in likes_map.items()
    ]
    df_likes = pd.DataFrame(rows_likes)
    likes_path = output_dir / "likes.parquet"
    df_likes.to_parquet(likes_path, index=False)
    print(f"Wrote {likes_path} ({len(df_likes)} rows)", flush=True)

    liked_post_uris = sorted({t[1] for t in likes_map.values()})
    if not liked_post_uris:
        print(
            "Phase B: no liked posts to hydrate; writing empty liked_posts.parquet",
            flush=True,
        )
        df_posts = pd.DataFrame(columns=["post_uri", "post_json"])
    else:
        poster_dids = list({extract_did_from_post_uri(u) for u in liked_post_uris})
        print(
            f"Phase B: hydrating {len(liked_post_uris)} distinct posts "
            f"({len(poster_dids)} poster DIDs)…",
            flush=True,
        )
        posts = await tap.wait_for_posts(
            set(liked_post_uris),
            timeout_sec=timeout_posts_sec,
            dids_to_add=poster_dids,
        )
        print(
            f"Phase B: received {len(posts)} of {len(liked_post_uris)} posts",
            flush=True,
        )
        df_posts = pd.DataFrame(
            [
                {"post_uri": uri, "post_json": json.dumps(rec, default=str)}
                for uri, rec in posts.items()
            ]
        )

    posts_path = output_dir / "liked_posts.parquet"
    df_posts.to_parquet(posts_path, index=False)
    print(f"Wrote {posts_path} ({len(df_posts)} rows)", flush=True)


def main() -> None:
    default_out = SCRIPT_DIR / "out"
    parser = argparse.ArgumentParser(
        description="Tap: enumerate likes for seed DIDs, hydrate liked posts, write Parquet."
    )
    parser.add_argument(
        "--tap-base-url",
        default=os.environ.get("TAP_BASE_URL", "http://localhost:2480"),
        help="Tap HTTP base URL (default TAP_BASE_URL or http://localhost:2480)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_out,
        help=f"Directory for likes.parquet and liked_posts.parquet (default: {default_out})",
    )
    parser.add_argument(
        "--user-dids-json",
        type=Path,
        default=None,
        help=f"Optional JSON {{\"dids\": [...]}} (default: built-in list or {SCRIPT_DIR / 'user_dids.json'})",
    )
    parser.add_argument(
        "--timeout-likes",
        type=float,
        default=3600.0,
        help="Max seconds for Phase A (like enumeration via Tap)",
    )
    parser.add_argument(
        "--timeout-posts",
        type=float,
        default=600.0,
        help="Max seconds for Phase B (TapRepoSync.wait_for_posts)",
    )
    parser.add_argument(
        "--skip-health-check",
        action="store_true",
        help="Do not GET /health before running (for dry imports only)",
    )
    args = parser.parse_args()

    seed_path = args.user_dids_json
    if seed_path is None:
        bundled = SCRIPT_DIR / "user_dids.json"
        if bundled.is_file():
            seed_path = bundled
    seed_dids = _load_seed_dids(seed_path)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if not args.skip_health_check:
        _check_tap_health(args.tap_base_url)

    asyncio.run(
        _run(
            base_url=args.tap_base_url,
            seed_dids=seed_dids,
            output_dir=args.output_dir,
            timeout_likes_sec=args.timeout_likes,
            timeout_posts_sec=args.timeout_posts,
        )
    )


if __name__ == "__main__":
    main()
