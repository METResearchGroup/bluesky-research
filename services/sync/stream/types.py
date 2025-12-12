"""Type definitions for sync stream operations and record types.

This module provides strongly-typed enums for operations, record types,
and related constants used throughout the sync stream system.
"""

from enum import Enum
from typing import Literal, TypedDict


class Operation(str, Enum):
    """Operation types for cache records."""

    CREATE = "create"
    DELETE = "delete"

    def __str__(self) -> str:
        return self.value


class RecordType(str, Enum):
    """Internal record types used for firehose records. A superset of the
    'GenericRecordType' enum derived from firehose data. We have additional
    fields such as 'like_on_user_post' and 'reply_to_user_post'."""

    POST = "post"
    LIKE = "like"
    FOLLOW = "follow"
    LIKE_ON_USER_POST = "like_on_user_post"
    REPLY_TO_USER_POST = "reply_to_user_post"

    def __str__(self) -> str:
        return self.value


class GenericRecordType(str, Enum):
    """Generic record types for firehose records (used in path manager)."""

    POST = "post"
    LIKE = "like"
    FOLLOW = "follow"

    def __str__(self) -> str:
        return self.value


class FollowStatus(str, Enum):
    """Follow relationship status."""

    FOLLOWER = "follower"
    FOLLOWEE = "followee"

    def __str__(self) -> str:
        return self.value


class UserPostType(str, Enum):
    """Type of user post relationship (for replies)."""

    ROOT = "root"
    PARENT = "parent"

    def __str__(self) -> str:
        return self.value


class HandlerKey(str, Enum):
    """Keys for handler registry (includes special cases).

    This enum provides type-safe keys for the handler registry.
    Note: IN_NETWORK_POST uses the same RecordType.POST but has a different handler.
    """

    POST = "post"
    LIKE = "like"
    FOLLOW = "follow"
    LIKE_ON_USER_POST = "like_on_user_post"
    REPLY_TO_USER_POST = "reply_to_user_post"
    IN_NETWORK_POST = "in_network_post"

    def __str__(self) -> str:
        return self.value


# Type definitions for operations_by_type dictionary structure
# This represents the structure of operations for a single record type
class RecordOperations(TypedDict):
    """Operations structure for a single record type.

    Contains lists of created and deleted records for a specific record type.
    """

    created: list[dict]
    deleted: list[dict]


# Type alias for the full operations_by_type dictionary
# Uses Literal union to restrict keys to known record types
# Note: This is a type hint - runtime validation should still be performed
OperationsByType = dict[
    Literal[
        "posts",
        "reposts",
        "likes",
        "follows",
        "lists",
        "blocks",
        "profiles",
    ],
    RecordOperations,
]
