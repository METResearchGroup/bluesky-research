"""Registry for record type handlers (strategy pattern)."""

from typing import Callable
from services.sync.stream.protocols import RecordHandlerProtocol


class RecordHandlerRegistry:
    """Registry for record type handlers.

    Uses factory pattern to enable dependency injection.
    Factories are stored as callables that return handler instances.
    """

    _handler_factories: dict[str, Callable[[], RecordHandlerProtocol]] = {}

    @classmethod
    def register_factory(
        cls,
        record_type: str,
        factory: Callable[[], RecordHandlerProtocol],
    ) -> None:
        """Register a handler factory for a record type.

        Args:
            record_type: Record type to register (e.g., "post", "like")
            factory: Factory function that returns a handler instance
        """
        cls._handler_factories[record_type] = factory

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
        factory = cls._handler_factories.get(record_type)
        if factory is None:
            available = list(cls._handler_factories.keys())
            raise KeyError(
                f"Unknown record type: {record_type}. " f"Available types: {available}"
            )
        return factory()

    @classmethod
    def list_record_types(cls) -> list[str]:
        """List all registered record types."""
        return sorted(cls._handler_factories.keys())

    @classmethod
    def clear(cls) -> None:
        """Clear all registered factories (useful for testing)."""
        cls._handler_factories.clear()
