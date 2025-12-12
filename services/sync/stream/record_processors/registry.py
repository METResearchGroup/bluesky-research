"""Registry for record processors."""

from services.sync.stream.record_processors.protocol import RecordProcessorProtocol


class ProcessorRegistry:
    """Registry for record type processors.

    Centralized lookup for processors by record type. Maps record type strings
    (e.g., "posts", "likes", "follows") to processor instances.

    Processors are stateless, so instances can be safely reused. The registry
    stores instances directly (not factories) for efficiency.

    Example:
        registry = ProcessorRegistry()
        registry.register_processor("posts", PostProcessor(context))
        processor = registry.get_processor("posts")
    """

    def __init__(self):
        """Initialize empty registry."""
        self._processors: dict[str, RecordProcessorProtocol] = {}

    def register_processor(
        self,
        record_type: str,
        processor: RecordProcessorProtocol,
    ) -> None:
        """Register a processor instance for a record type.

        Args:
            record_type: Record type to register (e.g., "posts", "likes", "follows")
            processor: Processor instance to register

        Raises:
            ValueError: If record_type is empty or processor is None
        """
        if not record_type:
            raise ValueError("record_type cannot be empty")
        if processor is None:
            raise ValueError("processor cannot be None")
        self._processors[record_type] = processor

    def get_processor(self, record_type: str) -> RecordProcessorProtocol:
        """Get processor for record type.

        Args:
            record_type: Record type to get processor for

        Returns:
            Processor instance

        Raises:
            KeyError: If record type not registered
        """
        processor = self._processors.get(record_type)
        if processor is None:
            available = list(self._processors.keys())
            raise KeyError(
                f"Unknown record type: {record_type}. Available types: {available}"
            )
        return processor

    def list_processors(self) -> list[str]:
        """List all registered record types.

        Returns:
            Sorted list of registered record type strings
        """
        return sorted(self._processors.keys())

    def clear(self) -> None:
        """Clear all registered processors (useful for testing)."""
        self._processors.clear()
