"""
Summarize likes.parquet: count distinct liked posts per user (liker DID).

Example:
  uv run python experiments/tap_experiments_hydrate_likes_2026_03_24/report_likes_per_user.py \\
    experiments/tap_experiments_hydrate_likes_2026_03_24/results/likes.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report total post likes per person from likes.parquet (one row per like record)."
    )
    parser.add_argument(
        "likes_parquet",
        type=Path,
        help="Path to likes.parquet (columns: user_did, like_uri, liked_post_uri)",
    )
    parser.add_argument(
        "--sort",
        choices=["count_desc", "did_asc"],
        default="count_desc",
        help="Sort order for the report (default: count_desc)",
    )
    args = parser.parse_args()

    if not args.likes_parquet.is_file():
        raise SystemExit(f"File not found: {args.likes_parquet}")

    df = pd.read_parquet(args.likes_parquet)
    if "user_did" not in df.columns:
        raise SystemExit(f"Expected column user_did; got {list(df.columns)}")
    counts = df.groupby("user_did", sort=False).size().reset_index(name="posts_liked")
    if args.sort == "count_desc":
        counts = counts.sort_values("posts_liked", ascending=False)
    else:
        counts = counts.sort_values("user_did")
    print(counts.to_string(index=False))
    print(f"\nTotal likers: {len(counts)}")
    print(f"Total like records (rows): {len(df)}")


if __name__ == "__main__":
    main()
