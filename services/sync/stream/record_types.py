"""Centralized registry for Bluesky record types.

This module provides a single source of truth for all record types supported
by the firehose stream. It maps Bluesky NSIDs (namespace identifiers) to
internal record type strings and provides a registry pattern for managing
supported types.

The registry allows for:
- Easy addition/removal of record types
- Type-safe access to record type mappings
- Runtime discovery of supported types
- Clear documentation of all supported types
"""

from typing import Final

# Mapping from Bluesky NSID (namespace identifier) to internal record type string
# This is the authoritative mapping used throughout the codebase
NSID_TO_RECORD_TYPE: Final[dict[str, str]] = {
    "app.bsky.feed.post": "posts",
    "app.bsky.feed.like": "likes",
    "app.bsky.feed.repost": "reposts",
    "app.bsky.graph.follow": "follows",
    "app.bsky.graph.listitem": "lists",
    "app.bsky.graph.block": "blocks",
    "app.bsky.actor.profile": "profiles",
}

# Record types that are currently supported by processors
# This is a subset of all record types that can come from the firehose
SUPPORTED_RECORD_TYPES: Final[list[str]] = ["posts", "likes", "follows"]

# All record types that can be received from the firehose
# This includes types we track but don't yet process
ALL_RECORD_TYPES: Final[list[str]] = list(NSID_TO_RECORD_TYPE.values())


class RecordTypeRegistry:
    """Registry for managing Bluesky record types.

    Provides methods to register new record types, query supported types,
    and convert between NSIDs and record type strings.

    Example:
        registry = RecordTypeRegistry()
        registry.register_record_type("app.bsky.feed.post", "posts")
        record_type = registry.get_record_type("app.bsky.feed.post")
    """

    def __init__(self):
        """Initialize registry with default mappings."""
        self._nsid_to_record_type: dict[str, str] = NSID_TO_RECORD_TYPE.copy()
        self._supported_types: set[str] = set(SUPPORTED_RECORD_TYPES)

    def register_record_type(
        self, nsid: str, record_type: str, supported: bool = False
    ) -> None:
        """Register a new record type mapping.

        Args:
            nsid: Bluesky NSID (e.g., "app.bsky.feed.post")
            record_type: Internal record type string (e.g., "posts")
            supported: Whether this type is supported by processors

        Raises:
            ValueError: If nsid or record_type is empty
        """
        if not nsid:
            raise ValueError("nsid cannot be empty")
        if not record_type:
            raise ValueError("record_type cannot be empty")

        self._nsid_to_record_type[nsid] = record_type
        if supported:
            self._supported_types.add(record_type)

    def get_record_type(self, nsid: str) -> str | None:
        """Get record type string for a given NSID.

        Args:
            nsid: Bluesky NSID

        Returns:
            Record type string, or None if not found
        """
        return self._nsid_to_record_type.get(nsid)

    def get_nsid(self, record_type: str) -> str | None:
        """Get NSID for a given record type string.

        Args:
            record_type: Internal record type string

        Returns:
            NSID, or None if not found
        """
        for nsid, rt in self._nsid_to_record_type.items():
            if rt == record_type:
                return nsid
        return None

    def is_supported(self, record_type: str) -> bool:
        """Check if a record type is supported by processors.

        Args:
            record_type: Internal record type string

        Returns:
            True if supported, False otherwise
        """
        return record_type in self._supported_types

    def get_supported_types(self) -> list[str]:
        """Get list of all supported record types.

        Returns:
            Sorted list of supported record type strings
        """
        return sorted(self._supported_types)

    def get_all_types(self) -> list[str]:
        """Get list of all registered record types.

        Returns:
            Sorted list of all record type strings
        """
        return sorted(set(self._nsid_to_record_type.values()))

    def get_nsid_mapping(self) -> dict[str, str]:
        """Get copy of NSID to record type mapping.

        Returns:
            Dictionary mapping NSIDs to record type strings
        """
        return self._nsid_to_record_type.copy()


# Global registry instance (can be extended at runtime if needed)
_default_registry = RecordTypeRegistry()
