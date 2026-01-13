"""
Hydrate example feeds with constructive labels.

Inputs (in this directory):
- example_generated_feeds.csv: one row per feed, `feed` column is a JSON-dumped list
  of objects, each containing key `item` with the post URI.
- constructive_moral_outrage_2026_01_09.csv: labels keyed by `uri`, includes
  `prob_constructive`.

We do the core transforms in DuckDB SQL to keep the workflow close to what we'd run in
Athena/Presto (UNNEST + join + AVG).

Run:
    python services/calculate_analytics/analyses/constructive_moral_outrage_2026_01_09/experiment.py
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Iterable

import duckdb
import pandas as pd


BASE_DIR = os.path.dirname(__file__)
FEEDS_CSV = os.path.join(BASE_DIR, "example_generated_feeds.csv")
LABELS_CSV = os.path.join(BASE_DIR, "constructive_moral_outrage_2026_01_09.csv")


@dataclass(frozen=True)
class FeedRow:
    feed_id: str
    user: str
    bluesky_handle: str
    condition: str
    feed_generation_timestamp: str
    feed_json: str  # normalized JSON array string


def _iter_json_candidates(raw: str) -> Iterable[str]:
    """
    Generate a few candidate strings we can try json.loads() on.

    The feed column is double-escaped inside CSV, so we commonly see:
      [{\"item\":\"at://...\",\"is_in_network\":false}, ...]
    """
    s = raw.strip()

    # Handle pathological wrapping quotes.
    if len(s) >= 2 and s[0] == s[-1] and s[0] in {"'", '"'}:
        s = s[1:-1]

    # Common fixes.
    yield s
    yield s.replace('\\"', '"')
    yield s.replace("\\\\", "\\").replace('\\"', '"')

    # Sometimes python literals sneak in.
    yield (
        s.replace("\\\\", "\\")
        .replace('\\"', '"')
        .replace("None", "null")
        .replace("True", "true")
        .replace("False", "false")
    )

    # Try stripping extra quotes again after unescaping.
    s2 = s.replace("\\\\", "\\").replace('\\"', '"').strip()
    yield s2.strip('"')


def normalize_feed_json(raw: Any) -> str:
    """Return a canonical JSON array string for DuckDB/Athena-style JSON parsing."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):  # type: ignore[arg-type]
        return "[]"

    s = str(raw)
    last_err: Exception | None = None
    for cand in _iter_json_candidates(s):
        try:
            obj = json.loads(cand)
            if not isinstance(obj, list):
                raise ValueError(f"Expected list JSON, got {type(obj).__name__}")
            # Canonical JSON (no whitespace) for stable parsing.
            return json.dumps(obj, separators=(",", ":"))
        except Exception as e:  # noqa: BLE001 - we want to try a few strategies
            last_err = e
            continue

    raise ValueError(f"Could not parse feed JSON. Last error: {last_err}. Raw: {s[:200]!r}")


def extract_feed_posts(feed_rows: list[FeedRow]) -> pd.DataFrame:
    """Build the per-post rows: one row per (feed_id, uri)."""
    out: list[dict[str, Any]] = []
    for r in feed_rows:
        items = json.loads(r.feed_json)
        for item_obj in items:
            if not isinstance(item_obj, dict):
                continue
            uri = item_obj.get("item")
            if not uri:
                continue
            out.append(
                {
                    "feed_id": r.feed_id,
                    "user": r.user,
                    "bluesky_handle": r.bluesky_handle,
                    "condition": r.condition,
                    "feed_generation_timestamp": r.feed_generation_timestamp,
                    "uri": uri,
                }
            )
    return pd.DataFrame(out)


def main() -> None:
    # Load feeds with pandas because the `feed` JSON column is intentionally escaped.
    feeds_df = pd.read_csv(FEEDS_CSV)

    # Minimal normalization in Python; SQL does the relational work.
    feed_rows: list[FeedRow] = []
    for _, row in feeds_df.iterrows():
        feed_rows.append(
            FeedRow(
                feed_id=str(row["feed_id"]),
                user=str(row["user"]),
                bluesky_handle=str(row["bluesky_handle"]),
                condition=str(row["condition"]),
                feed_generation_timestamp=str(row["feed_generation_timestamp"]),
                feed_json=normalize_feed_json(row["feed"]),
            )
        )

    feed_posts_df = extract_feed_posts(feed_rows)
    if feed_posts_df.empty:
        raise RuntimeError("No feed posts extracted from example_generated_feeds.csv")

    # DuckDB: SQL-first transforms (join + aggregation).
    conn = duckdb.connect(":memory:")

    # Register the small, local tables.
    conn.register("feed_posts", feed_posts_df)

    # Create a temp table of URIs so we only keep label rows relevant to these example feeds.
    conn.execute(
        """
        CREATE TEMP TABLE distinct_uris AS
        SELECT DISTINCT uri
        FROM feed_posts
        WHERE uri IS NOT NULL
        """
    )

    # Load and filter labels (still scans the CSV once, but selects only needed columns).
    # NOTE: labels CSV has an unnamed first column (index), but we only select named cols.
    conn.execute(
        f"""
        CREATE TEMP TABLE labels_filtered AS
        SELECT
          uri,
          prob_constructive,
          try_strptime(label_timestamp, '%Y-%m-%d-%H:%M:%S') AS label_ts
        FROM read_csv(
          '{LABELS_CSV}',
          delim=',',
          header=true,
          quote='"',
          escape='"',
          strict_mode=false,
          ignore_errors=true,
          null_padding=true
        )
        WHERE uri IS NOT NULL
          AND prob_constructive IS NOT NULL
          AND uri IN (SELECT uri FROM distinct_uris)
        """
    )

    # Deduplicate labels per URI: keep most recent label_timestamp if multiple exist.
    conn.execute(
        """
        CREATE TEMP TABLE labels_dedup AS
        SELECT uri, prob_constructive
        FROM (
          SELECT
            uri,
            prob_constructive,
            row_number() OVER (PARTITION BY uri ORDER BY label_ts DESC NULLS LAST) AS rn
          FROM labels_filtered
        )
        WHERE rn = 1
        """
    )

    # Data structure 1: per-feed with URIs list
    feed_uris_df = conn.execute(
        """
        SELECT
          feed_id,
          user,
          bluesky_handle,
          condition,
          feed_generation_timestamp,
          list(uri) AS uris
        FROM feed_posts
        GROUP BY 1,2,3,4,5
        ORDER BY feed_generation_timestamp, feed_id
        """
    ).df()

    # Data structure 2: per-URI labels (restricted to URIs seen in the feeds)
    uri_labels_df = conn.execute(
        """
        SELECT uri, prob_constructive
        FROM labels_dedup
        ORDER BY uri
        """
    ).df()

    # Final: per-feed avg(prob_constructive), ignoring missing labels (AVG ignores NULL).
    feed_avg_df = conn.execute(
        """
        SELECT
          p.feed_id,
          p.bluesky_handle,
          p.condition,
          p.feed_generation_timestamp,
          avg(l.prob_constructive) AS avg_prob_constructive,
          count(*) AS n_posts_in_feed,
          count(l.prob_constructive) AS n_labeled_posts_in_feed
        FROM feed_posts p
        LEFT JOIN labels_dedup l
          USING (uri)
        GROUP BY 1,2,3,4
        ORDER BY feed_generation_timestamp, feed_id
        """
    ).df()

    # Print requested output (row-level per feed).
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", None)

    print("\n=== Feed+URIs (data structure #1) ===")
    print(feed_uris_df)

    print("\n=== URI labels (data structure #2) ===")
    print(uri_labels_df)

    print("\n=== Per-feed avg(prob_constructive) ===")
    print(
        feed_avg_df[
            [
                "feed_id",
                "bluesky_handle",
                "condition",
                "feed_generation_timestamp",
                "avg_prob_constructive",
            ]
        ]
    )

    print("\n=== Debug counts ===")
    print(feed_avg_df[["feed_id", "n_posts_in_feed", "n_labeled_posts_in_feed"]])


if __name__ == "__main__":
    main()


