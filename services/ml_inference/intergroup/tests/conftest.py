"""Shared test utilities and fixtures for intergroup tests.

This module provides helper functions and fixtures that are shared across
multiple test files in the intergroup tests directory.
"""

from lib.constants import timestamp_format
from datetime import datetime, timezone

from services.ml_inference.intergroup.models import IntergroupLabelModel
from services.ml_inference.models import LabelWithBatchId, PostToLabelModel


def create_post_dict(uri="uri_1", text="test post 1", batch_id=1, batch_metadata="{}"):
    """Create a post dictionary for testing.

    Args:
        uri: Post URI identifier
        text: Post text content
        batch_id: Batch ID
        batch_metadata: Batch metadata string

    Returns:
        Dictionary with post fields suitable for queue/classification input
    """
    return {
        "uri": uri,
        "text": text,
        "preprocessing_timestamp": "2024-01-01-12:00:00",
        "batch_id": batch_id,
        "batch_metadata": batch_metadata,
    }


def create_post_to_label_model(uri="uri_1", text="test post 1", batch_id=1, batch_metadata="{}"):
    """Create a PostToLabelModel instance for testing.

    Args:
        uri: Post URI identifier
        text: Post text content
        batch_id: Batch ID
        batch_metadata: Batch metadata string

    Returns:
        PostToLabelModel instance
    """
    return PostToLabelModel(
        uri=uri,
        text=text,
        preprocessing_timestamp="2024-01-01-12:00:00",
        batch_id=batch_id,
        batch_metadata=batch_metadata,
    )


def create_intergroup_label(
    uri="uri_1",
    text="test post 1",
    was_successfully_labeled=True,
    label=1,
    reason=None,
    label_timestamp=None,
):
    """Create an IntergroupLabelModel instance for testing.

    Args:
        uri: Post URI identifier
        text: Post text content
        was_successfully_labeled: Whether labeling succeeded
        label: Label value (0 or 1 for successful, -1 for failed)
        reason: Optional failure reason
        label_timestamp: Optional label timestamp (defaults to current UTC time)

    Returns:
        IntergroupLabelModel instance
    """
    if label_timestamp is None:
        label_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)
    kwargs = {
        "uri": uri,
        "text": text,
        "preprocessing_timestamp": "2024-01-01-12:00:00",
        "was_successfully_labeled": was_successfully_labeled,
        "label": label,
        "label_timestamp": label_timestamp,
    }
    if reason is not None:
        kwargs["reason"] = reason
    return IntergroupLabelModel(**kwargs)


def create_label_with_batch_id(
    batch_id=1,
    uri="uri_1",
    text="test post 1",
    was_successfully_labeled=True,
    label=1,
    reason=None,
):
    """Create a LabelWithBatchId instance for testing.

    Args:
        batch_id: Batch ID
        uri: Post URI identifier
        text: Post text content
        was_successfully_labeled: Whether labeling succeeded
        label: Label value (0 or 1 for successful, -1 for failed)
        reason: Optional failure reason

    Returns:
        LabelWithBatchId instance
    """
    kwargs = {
        "batch_id": batch_id,
        "uri": uri,
        "text": text,
        "preprocessing_timestamp": "2024-01-01-12:00:00",
        "was_successfully_labeled": was_successfully_labeled,
        "label": label,
    }
    if reason is not None:
        kwargs["reason"] = reason
    return LabelWithBatchId(**kwargs)

