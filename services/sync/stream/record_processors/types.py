"""Type definitions for record processors."""

from pydantic import BaseModel, Field, field_validator, model_validator
from services.sync.stream.core.types import FollowStatus, HandlerKey, RecordType


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

    handler_key: HandlerKey | RecordType
    author_did: str
    filename: str
    follow_status: FollowStatus | None = None
    metadata: dict = Field(default_factory=dict)

    @field_validator("metadata", mode="before")
    @classmethod
    def default_metadata_to_dict(cls, v):
        """Convert None to empty dict for metadata field."""
        if v is None:
            return {}
        return v

    # @model_validator runs after the model is initialized,
    # allowing custom validation/coercion logic for the whole model.
    # Here it ensures required fields are not empty.
    @model_validator(mode="after")
    def check_required_fields(self):
        if not self.author_did:
            raise ValueError("author_did cannot be empty")
        if not self.filename:
            raise ValueError("filename cannot be empty")
        return self

    # model_config lets you configure Pydantic model behavior.
    # Setting frozen = True makes model instances immutable.
    model_config = {"frozen": True}
