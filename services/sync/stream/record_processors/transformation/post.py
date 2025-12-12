"""Post transformation helpers."""

import json

from services.consolidate_post_records.helper import consolidate_firehose_post
from services.consolidate_post_records.models import ConsolidatedPostRecordModel
from services.sync.stream.core.types import Operation
from transform.transform_raw_data import process_firehose_post


def transform_post(post: dict, operation: Operation) -> dict:
    """Transform raw firehose post to consolidated post model.

    Converts raw firehose post format to ConsolidatedPostRecordModel format.
    For CREATE operations, performs full transformation. For DELETE operations,
    returns the post dict as-is (DELETE operations only contain URI).

    Args:
        post: Raw firehose post dictionary
        operation: Operation type (CREATE or DELETE)

    Returns:
        For CREATE operations: Transformed post as dictionary
            (ConsolidatedPostRecordModel dict representation with embed JSON-dumped).
        For DELETE operations: Input post dict as-is (only contains URI).

    Raises:
        ValueError: If post format is invalid
        KeyError: If required fields are missing
    """
    if operation == Operation.CREATE:
        # we have a two-stage transformation process here (naming could be improved).
        # 1. Flatten (more or less) the firehose post
        # 2. Transform the post to a shared representation. Posts from the firehose
        # are different than posts from the Bluesky API (namely they're much more
        # sparse, whereas posts from the API have hydrated fields like number of counts).
        # We use both types of posts in the pipeline (though we should consolidate later),
        # so we need to transform both to the same 'ConsolidatedPostRecordModel' format.
        firehose_post = process_firehose_post(post)
        consolidated_post: ConsolidatedPostRecordModel = consolidate_firehose_post(
            firehose_post
        )
        consolidated_post_dict = consolidated_post.dict()

        # JSON-dump the embed to avoid complex dtype problems in the future.
        # NOTE: we'll consolidate this later as this means that the dict schema
        # has slightly diverged from ConsolidatedPostRecordModel. Too many things
        # downstream though rely on ConsolidatedPostRecordModel, so we'll leave it
        # as-is for now.
        consolidated_post_dict["embed"] = json.dumps(consolidated_post_dict["embed"])

        return consolidated_post_dict
    elif operation == Operation.DELETE:
        # DELETE operations only contain URI, return as-is
        return post
    else:
        raise ValueError(f"Unknown operation: {operation}")
