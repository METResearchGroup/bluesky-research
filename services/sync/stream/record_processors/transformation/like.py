"""Like transformation helpers."""

from atproto_client.models.app.bsky.feed.like import Record as LikeRecord

from lib.db.bluesky_models.raw import RawLike, RawLikeRecord
from services.sync.stream.core.types import Operation


def transform_like(like: dict, operation: Operation) -> dict:
    """Transform raw firehose like to RawLike model.

    Converts raw firehose like format to RawLike model format.
    For CREATE operations, performs full transformation. For DELETE operations,
    returns the like dict as-is (DELETE operations only contain URI).

    Args:
        like: Raw firehose like dictionary
        operation: Operation type (CREATE or DELETE)

    Returns:
        For CREATE operations: Transformed like as dictionary
            (RawLike dict representation).
        For DELETE operations: Input like dict as-is (only contains URI).

    Raises:
        ValueError: If like format is invalid
        KeyError: If required fields are missing
    """
    if operation == Operation.CREATE:
        raw_liked_record: LikeRecord = like["record"]

        # Convert created_at to createdAt for RawLikeRecord
        like_record_dict = raw_liked_record.dict()
        like_record_dict["createdAt"] = like_record_dict.pop(
            "created_at", like_record_dict.get("createdAt")
        )

        raw_liked_record_model = RawLikeRecord(**like_record_dict)

        like_model = RawLike(
            **{
                "author": like["author"],
                "cid": like["cid"],
                "record": raw_liked_record_model.dict(),
                "uri": like["uri"],
            }
        )

        return like_model.dict()
    elif operation == Operation.DELETE:
        # DELETE operations only contain URI, return as-is
        return like
    else:
        raise ValueError(f"Unknown operation: {operation}")


def extract_liked_post_uri(like_record_dict: dict) -> str:
    """Extract the URI of the post that was liked.

    Args:
        like_record_dict: Transformed like record dictionary

    Returns:
        URI of the liked post

    Raises:
        KeyError: If subject.uri is not found in record
    """
    return like_record_dict["record"]["subject"]["uri"]
