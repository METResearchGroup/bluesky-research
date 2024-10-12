"""Load data for sociopolitical LLM inference."""

import json
import os

from services.ml_inference.models import SociopoliticalLabelsModel
from services.ml_inference.sociopolitical.constants import root_cache_path
from services.ml_inference.helper import load_cached_jsons_as_df


def load_classified_posts_from_cache() -> dict:
    """Loads all the classified posts from cache into memory.

    Note: shouldn't be too much memory, though it depends in practice on how
    often this service is run.
    """
    firehose_path = os.path.join(root_cache_path, "firehose")
    most_liked_path = os.path.join(root_cache_path, "most_liked")
    firehose_posts: list[SociopoliticalLabelsModel] = []
    most_liked_posts: list[SociopoliticalLabelsModel] = []

    firehose_paths = []
    most_liked_paths = []

    for path in [firehose_path, most_liked_path]:
        for filename in os.listdir(path):
            full_path = os.path.join(path, filename)
            if "firehose" in full_path:
                firehose_paths.append(full_path)
            elif "most_liked" in full_path:
                most_liked_paths.append(full_path)

    firehose_df = load_cached_jsons_as_df(firehose_paths)
    most_liked_df = load_cached_jsons_as_df(most_liked_paths)

    firehose_dicts = (
        firehose_df.to_dict(orient="records") if firehose_df is not None else []
    )
    most_liked_dicts = (
        most_liked_df.to_dict(orient="records") if most_liked_df is not None else []
    )

    firehose_posts = [SociopoliticalLabelsModel(**post_dict) for post_dict in firehose_dicts]
    most_liked_posts = [SociopoliticalLabelsModel(**post_dict) for post_dict in most_liked_dicts]

    return {
        "firehose": firehose_posts,
        "most_liked": most_liked_posts,
    }
