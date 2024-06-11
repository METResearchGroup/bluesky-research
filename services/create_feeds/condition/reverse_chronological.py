"""Logic for reverse-chronological feed creation."""
from services.ml_inference.models import RecordClassificationMetadataModel


def create_reverse_chronological_feeds(
    posts: list[RecordClassificationMetadataModel]
) -> list[RecordClassificationMetadataModel]:
    """Returns a feed sorted by reverse chronological order."""
    posts = sorted(posts, key=lambda x: x.synctimestamp, reverse=True)
    return posts


if __name__ == "__main__":
    pass
