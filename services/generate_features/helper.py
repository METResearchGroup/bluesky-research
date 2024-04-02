"""Helper code for generating features from raw (filtered) data."""
import json

import pandas as pd

from lib.helper import track_function_runtime
from services.generate_features.ml_feature_generation import generate_ml_features # noqa
from services.generate_features.non_ml_feature_generation import generate_non_ml_features # noqa


@track_function_runtime
def load_latest_filtered_data() -> list[dict]:
    """Loads latest filtered data as a list of dictionaries.
    
    We load as a list of dictionaries since our operations are row-wise (so,
    using pandas is pretty slow).
    """
    # load firehose posts, as df
    # load IDs of filtered posts, as a set
    # filter for posts that are in the set (of posts that pass filtering)
    # return as a list of dicts
    pass


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
    posts: list[dict] = load_latest_filtered_data()
    features_dicts: list[dict] = generate_features_for_posts(posts=posts)
    write_features_to_db(features_dicts)
