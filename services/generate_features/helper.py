"""Helper code for generating features from raw (filtered) data."""
import json
from typing import Optional

import pandas as pd

from lib.helper import track_function_runtime
from services.generate_features.ml_feature_generation import generate_ml_features # noqa
from services.generate_features.non_ml_feature_generation import generate_non_ml_features # noqa



def load_raw_posts(
    k: Optional[int] = None,
    only_recent_posts: Optional[bool] = False,
    recency_days_filter: Optional[int] = None
) -> list[dict]:
    """Loads raw firehose posts from database."""
    pass


def load_uris_of_posts_that_pass_filtering() -> set[str]:
    """Load the URIs of posts that pass filtering."""
    return set()


def load_uris_of_posts_with_features() -> set[str]:
    """Load the URIs of posts with features, so we don't regenerate it."""
    return set()


@track_function_runtime
def load_posts_to_generate_features(
    num_posts: Optional[int]=None,
    only_recent_posts: Optional[bool] = False,
    recency_days_filter: Optional[int] = None
) -> list[dict]:
    """Load posts that we will be generating features for."""
    # get raw posts (for now, we add a k filter so we only grab a subset, but
    # in reality we likely don't want to cap the # of raw posts to consider,
    # and the other filters like filtering, previous posts with features, and
    # recency filters, should naturally cut down the # of posts to consider)
    # NOTE: we could even calculate this step offline and cache the latest
    # posts that need features, since this implementation might get a little
    # inefficient, both in memory (loading a bunch of posts and URIs, etc.) and
    # runtime (for-loops). We could cache the URIs via something like Redis
    # or in a queue, and then update accordingly over time. As posts pass
    # filtering, for example, we could pass them into a queue for feature
    # generation, and this service could take batches of them.
    raw_posts: list[dict] = load_raw_posts(
        only_recent_posts=only_recent_posts,
        recency_days_filter=recency_days_filter
    )
    valid_post_uris: set[str] = load_uris_of_posts_that_pass_filtering()
    posts_with_features_uris: set[str] = load_uris_of_posts_with_features()
    posts_to_generate_features_for = []
    for post in raw_posts:
        if (
            post["uri"] in valid_post_uris
            and post["uri"] not in posts_with_features_uris
        ):
            posts_to_generate_features_for.append(post)
    return posts_to_generate_features_for[:num_posts]


@track_function_runtime
def generate_features_for_post(post: dict) -> dict:
    """Generate features for a post."""
    ml_features: dict = generate_ml_features(post)
    non_ml_features: dict = generate_non_ml_features(post)
    features = {**ml_features, **non_ml_features}
    return features


@track_function_runtime
def generate_features_for_posts(posts: list[dict]) -> list[dict]:
    """Generate features for a list of posts."""
    features = [generate_features_for_post(post) for post in posts]
    return features


@track_function_runtime
def write_features_to_db(features_dicts: list[dict]) -> None:
    """Writes features to the MongoDB database."""
    for features_dict in features_dicts:
        uid = features_dict.pop("uid")
        timestamp = pd.Timestamp.now()
        data = {
            "uid": uid,
            "timestamp": timestamp,
            "features": json.dumps(features_dict)
        }
        pass
    pass


@track_function_runtime
def generate_features() -> None:
    """Generat es features for posts."""
    posts: list[dict] = load_posts_to_generate_features()
    features_dicts: list[dict] = generate_features_for_posts(posts=posts)
    write_features_to_db(features_dicts)
