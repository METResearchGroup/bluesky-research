"""Registry for record type handlers (strategy pattern)."""

from services.sync.stream.protocols import RecordHandlerProtocol


class RecordHandlerRegistry:
    """Registry for record type handlers.

    Stores handler instances directly for reuse.
    Handlers are stateless, so reusing instances is safe and efficient.
    """

    _handlers: dict[str, RecordHandlerProtocol] = {}

    @classmethod
    def register_handler(
        cls,
        record_type: str,
        handler: RecordHandlerProtocol,
    ) -> None:
        """Register a handler instance for a record type.

        Args:
            record_type: Record type to register (e.g., "post", "like")
            handler: Handler instance to register
        """
        cls._handlers[record_type] = handler

    @classmethod
    def get_handler(cls, record_type: str) -> RecordHandlerProtocol:
        """Get handler for record type.

        Args:
            record_type: Record type to get handler for

        Returns:
            Handler instance

        Raises:
            KeyError: If record type not registered
        """
        handler = cls._handlers.get(record_type)
        if handler is None:
            available = list(cls._handlers.keys())
            raise KeyError(
                f"Unknown record type: {record_type}. " f"Available types: {available}"
            )
        return handler

    @classmethod
    def list_record_types(cls) -> list[str]:
        """List all registered record types."""
        return sorted(cls._handlers.keys())

    @classmethod
    def clear(cls) -> None:
        """Clear all registered handlers (useful for testing)."""
        cls._handlers.clear()
