from lib.db.sql.ml_inference_database import get_metadata
from services.ml_inference.models import (
    PerspectiveApiLabelsModel,
    RecordClassificationMetadataModel,
)


def load_firehose_based_posts() -> list[RecordClassificationMetadataModel]:
    """Load post metadata from posts that come from the firehose."""
    post_metadata = get_metadata(source="firehose")
    return post_metadata


def load_most_liked_based_posts() -> list[RecordClassificationMetadataModel]:
    """Load post metadata from posts that come from the most liked feed."""
    post_metadata = get_metadata(source="most_liked")
    return post_metadata


def load_perspective_api_labels() -> list[PerspectiveApiLabelsModel]:
    pass
