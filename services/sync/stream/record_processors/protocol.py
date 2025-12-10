"""Protocol definitions for record processors."""

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from services.sync.stream.context import CacheWriteContext
    from services.sync.stream.record_processors.types import RoutingDecision
    from services.sync.stream.types import Operation


class RecordProcessorProtocol(Protocol):
    """Protocol defining the interface for record processors.

    All record processors must implement this protocol to ensure consistent
    behavior across different record types (posts, likes, follows, etc.).

    The protocol separates transformation (converting raw firehose data to
    domain models) from routing (determining which handlers should process
    the record). This separation enables independent testing and clear
    separation of concerns.

    Example:
        class PostProcessor:
            def transform(self, record: dict, operation: Operation) -> dict:
                # Convert raw firehose post to ConsolidatedPostRecordModel
                ...

            def get_routing_decisions(
                self,
                transformed: dict,
                operation: Operation,
                context: CacheWriteContext,
            ) -> list[RoutingDecision]:
                # Determine routing paths (study user, in-network, reply, etc.)
                ...
    """

    def transform(self, record: dict, operation: "Operation") -> dict:
        """Transform raw firehose record to domain model.

        Converts the raw firehose record format into the appropriate domain
        model format (e.g., ConsolidatedPostRecordModel, RawLike, RawFollow).

        Args:
            record: Raw firehose record dictionary
            operation: Operation type (CREATE or DELETE)

        Returns:
            Transformed record as dictionary (domain model dict representation)

        Raises:
            ValueError: If record format is invalid
            KeyError: If required fields are missing
        """
        ...

    def get_routing_decisions(
        self,
        transformed: dict,
        operation: "Operation",
        context: "CacheWriteContext",
    ) -> list["RoutingDecision"]:
        """Determine all routing paths for a transformed record.

        Analyzes the transformed record and context to determine which handlers
        should process this record. A single record can route to multiple handlers
        (e.g., a post can be both a study user post AND trigger a reply handler).

        Args:
            transformed: Transformed record dictionary (from transform())
            operation: Operation type (CREATE or DELETE)
            context: Cache write context with dependencies (study_user_manager, etc.)

        Returns:
            List of RoutingDecision objects (empty list if no routes match)

        Note:
            For DELETE operations, routing logic may be simplified or skipped
            depending on the record type. Some record types only handle CREATE.
        """
        ...
