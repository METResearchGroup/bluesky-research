"""Post transformation helpers."""

import json

from services.consolidate_post_records.helper import consolidate_firehose_post
from services.consolidate_post_records.models import ConsolidatedPostRecordModel
from services.sync.stream.types import Operation
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
        Transformed post as dictionary (ConsolidatedPostRecordModel dict representation)

    Raises:
        ValueError: If post format is invalid
        KeyError: If required fields are missing
    """
    if operation == Operation.CREATE:
        # Transform firehose post to consolidated post
        firehose_post = process_firehose_post(post)
        consolidated_post: ConsolidatedPostRecordModel = consolidate_firehose_post(
            firehose_post
        )
        consolidated_post_dict = consolidated_post.dict()

        # JSON-dump the embed to avoid complex dtype problems in the future
        consolidated_post_dict["embed"] = json.dumps(consolidated_post_dict["embed"])

        return consolidated_post_dict
    elif operation == Operation.DELETE:
        # DELETE operations only contain URI, return as-is
        return post
    else:
        raise ValueError(f"Unknown operation: {operation}")


def build_post_filename(author_did: str, post_uri_suffix: str) -> str:
    """Build filename for post record.

    Args:
        author_did: Author DID
        post_uri_suffix: Post URI suffix

    Returns:
        Filename string
    """
    return f"author_did={author_did}_post_uri_suffix={post_uri_suffix}.json"


def build_delete_post_filename(post_uri_suffix: str) -> str:
    """Build filename for deleted post record.

    Args:
        post_uri_suffix: Post URI suffix

    Returns:
        Filename string
    """
    return f"post_uri_suffix={post_uri_suffix}.json"
