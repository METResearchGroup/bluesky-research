"""Load data for IME classification."""

import os

import pandas as pd

from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.log.logger import get_logger
from services.ml_inference.ime.constants import root_cache_path
from services.ml_inference.helper import load_cached_jsons_as_df


dtypes_map = MAP_SERVICE_TO_METADATA["ml_inference_ime"]["dtypes_map"]

# drop fields that are added on export.
dtypes_map.pop("partition_date")

logger = get_logger(__name__)


def load_classified_posts_from_cache() -> dict:
    """Loads all the classified posts from cache into memory."""
    firehose_valid_path = os.path.join(root_cache_path, "firehose", "valid")
    most_liked_valid_path = os.path.join(root_cache_path, "most_liked", "valid")

    firehose_paths = []
    most_liked_paths = []

    for path in [firehose_valid_path, most_liked_valid_path]:
        if os.path.exists(path):
            paths = os.listdir(path)
        else:
            paths = []
        if path == firehose_valid_path:
            firehose_paths.extend([os.path.join(path, p) for p in paths])
        elif path == most_liked_valid_path:
            most_liked_paths.extend([os.path.join(path, p) for p in paths])

    firehose_df = load_cached_jsons_as_df(
        filepaths=firehose_paths, dtypes_map=dtypes_map
    )
    most_liked_df = load_cached_jsons_as_df(
        filepaths=most_liked_paths, dtypes_map=dtypes_map
    )

    if firehose_df is None or firehose_df.empty:
        logger.info("No labeled firehose posts found in cache.")
        firehose_df = pd.DataFrame(columns=dtypes_map.keys())
    if most_liked_df is None or most_liked_df.empty:
        logger.info("No labeled most liked posts found in cache.")
        most_liked_df = pd.DataFrame(columns=dtypes_map.keys())

    firehose_df = firehose_df.astype(dtypes_map)
    most_liked_df = most_liked_df.astype(dtypes_map)

    return {
        "firehose": firehose_df,
        "most_liked": most_liked_df,
    }
