"""Model for classifiying NSFW content.

We can use LLMs or Perspective API later on, but using user-applied labels
seems to work for now.
"""
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from services.preprocess_raw_data.classify_nsfw_content.constants import (
    LABELS_TO_FILTER
)


def classify(post: ConsolidatedPostRecordModel) -> bool:
    """Classifies if a post is NSFW or not."""
    labels = post.labels
    text = post.text
    labels = labels.split(",") if labels else []
    for label_to_filter in LABELS_TO_FILTER:
        if label_to_filter in labels or label_to_filter in text:
            return True
    return False
