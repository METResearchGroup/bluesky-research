"""Model for classifiying NSFW content.

We can use LLMs or Perspective API later on, but using user-applied labels
seems to work for now.
"""
from services.filter_raw_data.classify_nsfw_content.constants import (
    LABELS_TO_FILTER
)


def classify(post: dict) -> bool:
    """Classifies if a post is NSFW or not."""
    labels = post.get("labels", None)
    labels = labels.split(",") if labels else []
    for label in labels:
        if label in LABELS_TO_FILTER:
            return True
    return False
