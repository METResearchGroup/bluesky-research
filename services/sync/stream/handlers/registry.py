"""Registry for record type handlers (strategy pattern)."""

from services.sync.stream.core.protocols import RecordHandlerProtocol


class RecordHandlerRegistry:
    """Registry for record type handlers.

    Stores handler instances directly for reuse.
    Handlers are stateless, so reusing instances is safe and efficient.
    """

    def __init__(self):
        """Initialize empty registry."""
        self._handlers: dict[str, RecordHandlerProtocol] = {}

    def register_handler(
        self,
        record_type: str,
        handler: RecordHandlerProtocol,
    ) -> None:
        """Register a handler instance for a record type.

        Args:
            record_type: Record type to register (e.g., "post", "like")
            handler: Handler instance to register
        """
        self._handlers[record_type] = handler

    def get_handler(self, record_type: str) -> RecordHandlerProtocol:
        """Get handler for record type.

        Args:
            record_type: Record type to get handler for

        Returns:
            Handler instance

        Raises:
            KeyError: If record type not registered
        """
        handler = self._handlers.get(record_type)
        if handler is None:
            available = list(self._handlers.keys())
            raise KeyError(
                f"Unknown record type: {record_type}. Available types: {available}"
            )
        return handler

    def list_record_types(self) -> list[str]:
        """List all registered record types."""
        return sorted(self._handlers.keys())

    def clear(self) -> None:
        """Clear all registered handlers (useful for testing)."""
        self._handlers.clear()
