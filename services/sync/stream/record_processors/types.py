"""Type definitions for record processors."""

from typing import TYPE_CHECKING, Optional

from pydantic import BaseModel, root_validator

if TYPE_CHECKING:
    from services.sync.stream.types import FollowStatus, HandlerKey, RecordType


class RoutingDecision(BaseModel):
    """Encapsulates all information needed to write a record via a handler.

    This model contains all parameters required for handler.write_record(),
    allowing routing decisions to be made independently of handler execution.

    Attributes:
        handler_key: Which handler to use (HandlerKey enum value)
        author_did: Author DID for path construction
        filename: Filename for the record
        follow_status: Optional follow status (for follow records)
        metadata: Additional metadata for logging, debugging, etc.
    """

    handler_key: "HandlerKey | RecordType"
    author_did: str
    filename: str
    follow_status: "FollowStatus | None" = None
    metadata: Optional[dict] = None

    # @root_validator runs after the model is initialized,
    # allowing custom validation/coercion logic for the whole model.
    # Here it ensures required fields and fills default dict.
    @root_validator(pre=False)
    def check_required_fields(cls, values):
        if not values.get("author_did"):
            raise ValueError("author_did cannot be empty")
        if not values.get("filename"):
            raise ValueError("filename cannot be empty")
        # If metadata is None, set to empty dict for consistency.
        values["metadata"] = values.get("metadata") or {}
        return values

    # class Config lets you configure Pydantic model behavior.
    # Setting allow_mutation = False makes model instances immutable.
    class Config:
        allow_mutation = False
