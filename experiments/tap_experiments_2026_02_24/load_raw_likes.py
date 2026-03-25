"""
Load raw_sync like records and write ~20 unique liked post URIs to JSON.

Used as input for hydrate_likes.py (PDS or Tap). Reads from local parquet
under raw_sync/create/like, parses subject.uri for each like, deduplicates,
and takes the first 20 liked post URIs.
"""

import json
import os

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.db.models import StorageTier
from services.calculate_analytics.shared.constants import (
    STUDY_END_DATE,
    STUDY_START_DATE,
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "liked_post_uris.json")
NUM_URIS = 20


def load_liked_post_uris(
    start_partition_date: str = STUDY_START_DATE,
    end_partition_date: str = STUDY_END_DATE,
    n: int = NUM_URIS,
) -> list[str]:
    """Load raw_sync likes and return up to n unique liked post URIs."""
    df: pd.DataFrame = load_data_from_local_storage(
        service="raw_sync",
        storage_tiers=[StorageTier.CACHE, StorageTier.ACTIVE],
        start_partition_date=start_partition_date,
        end_partition_date=end_partition_date,
        custom_args={"record_type": "like"},
    )
    if df.empty:
        return []
    df = df.drop_duplicates(subset=["uri", "author"])
    df["liked_post_uri"] = df["subject"].apply(json.loads).apply(lambda x: x["uri"])
    unique_uris = df["liked_post_uri"].drop_duplicates().tolist()
    return unique_uris[:n]


def main() -> None:
    uris = load_liked_post_uris()
    with open(OUTPUT_PATH, "w") as f:
        json.dump(uris, f, indent=2)
    print(f"Wrote {len(uris)} liked post URIs to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
