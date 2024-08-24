"""Load data for sociopolitical LLM inference."""

import json
import os

from services.ml_inference.models import SociopoliticalLabelsModel
from services.ml_inference.sociopolitical.constants import root_cache_path


def load_classified_posts_from_cache() -> dict:
    """Loads all the classified posts from cache into memory.

    Note: shouldn't be too much memory, though it depends in practice on how
    often this service is run.
    """
    firehose_path = os.path.join(root_cache_path, "firehose")
    most_liked_path = os.path.join(root_cache_path, "most_liked")
    firehose_posts: list[SociopoliticalLabelsModel] = []
    most_liked_posts: list[SociopoliticalLabelsModel] = []
    for path in [firehose_path, most_liked_path]:
        for filename in os.listdir(path):
            full_path = os.path.join(path, filename)
            with open(full_path, "r") as f:
                post = json.load(f)
                if "firehose" in full_path:
                    firehose_posts.append(SociopoliticalLabelsModel(**post))
                elif "most_liked" in full_path:
                    most_liked_posts.append(SociopoliticalLabelsModel(**post))
    return {
        "firehose": firehose_posts,
        "most_liked": most_liked_posts,
    }
