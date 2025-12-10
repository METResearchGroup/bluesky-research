"""Follow transformation helpers."""

from atproto_client.models.app.bsky.graph.follow import Record as FollowRecord

from lib.db.bluesky_models.raw import RawFollow, RawFollowRecord
from services.sync.stream.types import Operation


def transform_follow(follow: dict, operation: Operation) -> dict:
    """Transform raw firehose follow to RawFollow model.

    Converts raw firehose follow format to RawFollow model format.
    For CREATE operations, performs full transformation. For DELETE operations,
    returns the follow dict as-is (DELETE operations only contain URI).

    Args:
        follow: Raw firehose follow dictionary
        operation: Operation type (CREATE or DELETE)

    Returns:
        Transformed follow as dictionary (RawFollow dict representation)

    Raises:
        ValueError: If follow format is invalid
        KeyError: If required fields are missing
    """
    if operation == Operation.CREATE:
        raw_follow_record: FollowRecord = follow["record"]

        # Convert created_at to createdAt for RawFollowRecord
        follow_record_dict = raw_follow_record.dict()
        follow_record_dict["createdAt"] = follow_record_dict.pop(
            "created_at", follow_record_dict.get("createdAt")
        )

        raw_follow_record_model = RawFollowRecord(**follow_record_dict)

        follow_model = RawFollow(
            **{
                "uri": follow["uri"],
                "cid": follow["cid"],
                "record": raw_follow_record_model.dict(),
                "author": follow["author"],
                "follower_did": follow["author"],
                "followee_did": raw_follow_record_model.subject,
            }
        )

        return follow_model.dict()
    elif operation == Operation.DELETE:
        # DELETE operations only contain URI, return as-is
        return follow
    else:
        raise ValueError(f"Unknown operation: {operation}")


def extract_follow_uri_suffix(follow_uri: str) -> str:
    """Extract follow URI suffix from full URI.

    Example:
        Input: "at://did:plc:abc123/app.bsky.graph.follow/3kwcxduaskd2p"
        Output: "3kwcxduaskd2p"

    Args:
        follow_uri: Full follow URI

    Returns:
        Follow URI suffix (last component after final '/')
    """
    return follow_uri.split("/")[-1]


def build_follow_filename(follower_did: str, followee_did: str) -> str:
    """Build filename for follow record.

    Args:
        follower_did: Follower DID
        followee_did: Followee DID

    Returns:
        Filename string
    """
    return f"follower_did={follower_did}_followee_did={followee_did}.json"


def build_delete_follow_filename(follow_uri_suffix: str) -> str:
    """Build filename for deleted follow record.

    Args:
        follow_uri_suffix: Follow URI suffix

    Returns:
        Filename string
    """
    return f"follow_uri_suffix={follow_uri_suffix}.json"


def extract_follower_did(follow_dict: dict) -> str:
    """Extract follower DID from follow record.

    Args:
        follow_dict: Transformed follow record dictionary

    Returns:
        Follower DID
    """
    return follow_dict["follower_did"]


def extract_followee_did(follow_dict: dict) -> str:
    """Extract followee DID from follow record.

    Args:
        follow_dict: Transformed follow record dictionary

    Returns:
        Followee DID
    """
    return follow_dict["followee_did"]
