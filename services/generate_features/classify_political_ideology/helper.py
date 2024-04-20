"""Classifies the political ideology of posts."""
from services.generate_features.classify_political_ideology.model import classify_posts  # noqa


# TODO: add appropriate rate limiting for endpoint
def classify_political_ideology_of_posts(posts: list[dict]) -> list[dict]:
    """Classifies the political ideology of posts."""
    return classify_posts(posts)
