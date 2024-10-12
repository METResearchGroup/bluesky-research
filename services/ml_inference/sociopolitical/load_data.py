"""Load data for sociopolitical LLM inference."""

import os

from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from services.ml_inference.models import SociopoliticalLabelsModel
from services.ml_inference.sociopolitical.constants import root_cache_path
from services.ml_inference.helper import load_cached_jsons_as_df


dtypes_map = MAP_SERVICE_TO_METADATA["ml_inference_sociopolitical"]["dtypes_map"]

# drop fields that are added on export.
dtypes_map.pop("partition_date")
dtypes_map.pop("source")

def load_classified_posts_from_cache() -> dict:
    """Loads all the classified posts from cache into memory.

    Note: shouldn't be too much memory, though it depends in practice on how
    often this service is run.
    """
    firehose_valid_path = os.path.join(root_cache_path, "firehose", "valid")
    most_liked_valid_path = os.path.join(root_cache_path, "most_liked", "valid")

    firehose_posts: list[SociopoliticalLabelsModel] = []
    most_liked_posts: list[SociopoliticalLabelsModel] = []

    firehose_paths = []
    most_liked_paths = []

    for path in [firehose_valid_path, most_liked_valid_path]:
        paths = os.listdir(path)
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

    firehose_dicts = (
        firehose_df.to_dict(orient="records") if firehose_df is not None else []
    )
    most_liked_dicts = (
        most_liked_df.to_dict(orient="records") if most_liked_df is not None else []
    )

    firehose_posts = [
        SociopoliticalLabelsModel(**post_dict) for post_dict in firehose_dicts
    ]
    most_liked_posts = [
        SociopoliticalLabelsModel(**post_dict) for post_dict in most_liked_dicts
    ]

    return {
        "firehose": firehose_posts,
        "most_liked": most_liked_posts,
    }
