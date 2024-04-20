"""Helper code for generating features from raw (filtered) data."""
from lib.helper import track_function_runtime


def load_filtered_posts() -> list[dict]:
    """Load posts that have passed filtering."""
    return []


def load_features():
    """Load features that we will be generating for posts."""
    return []


def load_posts_to_generate_features(features: list[str]) -> list[dict]:
    """Load posts that we will be generating features for."""
    posts: list[dict] = load_filtered_posts()
    # remove posts that we've already generated features for
    # (though we could put this behind a flag)
    posts = load_features(posts)
    # do we load all filtered posts or just the posts that were
    # filtered in the last batch or the posts that don't already have
    # features (or some combination?) We should do all filtering in this step.
    return posts


def get_or_create_feature(feature: str, post: dict) -> dict:
    """For a given post and feature, get the feature value(s)."""
    return {}


def validate_posts_and_features(
    posts_with_features: list[dict], features: list[str]
) -> None:
    """Performs validation on the posts and the features using Great Expectations."""  # noqa
    return


def write_to_feature_store(posts_with_features: list[dict]) -> None:
    """Writes the features to the feature store."""
    return


def validate_features_of_posts(features_dicts: list[dict]) -> None:
    """Validates the features generated for posts, using Great Expectations."""
    return


@track_function_runtime
def generate_features():
    """Generates features for posts."""
    features = load_features()
    posts = load_posts_to_generate_features(features)
    # do we want to run this service with streaming or do we want batching?
    # we can start with batching and then investigate streaming later, since
    # batching would be easier as a first pass.

    # let's assume that for each ML feature, we can either (1)
    # read from the database for the classification from that feature or
    # (2) run the ML feature API, write to database, and get the results
    posts_with_features = []
    for post in posts:
        for feature in features:
            # either get the existing value for a feature + post or ping the
            # ML feature API (whatever it may be, for that given post).
            # (maybe down the line I should create a single API interface to
            # interact with all ML portions of the project? That way, each
            # service will have a single interface for any ML? Unsure, will
            # have to think about it).
            feature_dict = get_or_create_feature(feature, post)
            for key, value in feature_dict.items():
                post[key] = value
            posts_with_features.append(post)

    # validate posts (i.e., via Great Expectations) and write to feature store
    validate_posts_and_features(
        posts_with_features=posts_with_features, features=features
    )
    write_to_feature_store(posts_with_features)
