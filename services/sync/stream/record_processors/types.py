"""Type definitions for record processors."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from services.sync.stream.types import HandlerKey, FollowStatus


@dataclass(frozen=True)
class RoutingDecision:
    """Encapsulates all information needed to write a record via a handler.

    This dataclass contains all parameters required for handler.write_record(),
    allowing routing decisions to be made independently of handler execution.

    Attributes:
        handler_key: Which handler to use (HandlerKey enum value)
        author_did: Author DID for path construction
        filename: Filename for the record
        follow_status: Optional follow status (for follow records)
        metadata: Additional metadata for logging, debugging, etc.
    """

    handler_key: "HandlerKey"
    author_did: str
    filename: str
    follow_status: "FollowStatus | None" = None
    metadata: dict | None = None

    def __post_init__(self):
        """Validate routing decision after initialization."""
        if not self.author_did:
            raise ValueError("author_did cannot be empty")
        if not self.filename:
            raise ValueError("filename cannot be empty")
        if self.metadata is None:
            object.__setattr__(self, "metadata", {})
