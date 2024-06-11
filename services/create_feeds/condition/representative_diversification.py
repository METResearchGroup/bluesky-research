"""Logic for representative diversification feed creation."""
from services.create_feeds.score_posts import score_posts
from services.ml_inference.models import (
    PerspectiveApiLabelsModel, RecordClassificationMetadataModel
)


def create_representative_diversification_feeds(
    users: list,
    posts: list[RecordClassificationMetadataModel],
    labels: list[PerspectiveApiLabelsModel],
) -> list[RecordClassificationMetadataModel]:
    """Returns a feed sorted by representative diversification score."""
    post_scores: list[float] = score_posts(posts)
    posts = [post for _, post in sorted(zip(post_scores, posts), reverse=True)]
    return posts


if __name__ == "__main__":
    pass
