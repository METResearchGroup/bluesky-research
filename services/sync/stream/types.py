"""Type definitions for sync stream operations and record types.

This module provides strongly-typed enums for operations, record types,
and related constants used throughout the sync stream system.
"""

from enum import Enum


class Operation(str, Enum):
    """Operation types for cache records."""

    CREATE = "create"
    DELETE = "delete"

    def __str__(self) -> str:
        return self.value


class RecordType(str, Enum):
    """Record types for study user activity."""

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
