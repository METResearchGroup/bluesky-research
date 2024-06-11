"""Logic for engagement feed creation."""
from services.ml_inference.models import RecordClassificationMetadataModel


def create_engagement_feeds(
    posts: list[RecordClassificationMetadataModel]
) -> list[RecordClassificationMetadataModel]:
    """Returns a feed with posts sorted by likes."""
    posts = sorted(posts, key=lambda x: x.like_count, reverse=True)
    return posts


if __name__ == "__main__":
    pass
