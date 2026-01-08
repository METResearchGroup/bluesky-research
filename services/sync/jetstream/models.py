"""Models for the Jetstream connector.

This module provides Pydantic models for Bluesky firehose data ingested via Jetstream.
"""

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

from lib.datetime_utils import generate_current_datetime_str


class JetstreamRecord(BaseModel):
    """Represents a single record from the Bluesky firehose via Jetstream.

    Attributes:
        did: Repository DID identifier
        time_us: Timestamp in microseconds
        kind: Kind of event (commit, identity, account)
        commit_data: JSON commit data for 'commit' kinds
        identity_data: Identity update data for 'identity' kinds
        account_data: Account status update data for 'account' kinds
        collection: Collection type (e.g. app.bsky.feed.post)
        created_at: Timestamp when this record was processed
    """

    did: str
    time_us: str
    kind: Literal["commit", "identity", "account"]
    commit_data: Optional[dict[str, Any]] = None
    identity_data: Optional[dict[str, Any]] = None
    account_data: Optional[dict[str, Any]] = None
    collection: str = ""
    created_at: str = Field(
        default_factory=generate_current_datetime_str,
        description="Timestamp when the record was processed",
    )

    def to_queue_item(self) -> dict[str, Any]:
        """Convert the record to a dictionary suitable for queue storage.

        Returns:
            Dictionary representation of the record for queue storage
        """
        return self.model_dump()
