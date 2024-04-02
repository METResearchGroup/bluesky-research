"""ML-powered feature generation."""


def example_ml_feature(post: dict) -> dict:
    """ML feature generation."""
    return {}


feature_funcs = [example_ml_feature]


def generate_ml_features(post: dict) -> dict:
    """Generates ML features for a post."""
    res = {}
    res["uid"] = post["uid"]
    res["text"] = post["text"]
    for func in feature_funcs:
        res[func.__name__] = func(post)
    return res
