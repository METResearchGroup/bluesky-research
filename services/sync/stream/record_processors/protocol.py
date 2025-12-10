from typing import Protocol


class RecordProcessorProtocol(Protocol):
    """Protocol for record processors."""

    def process_record(self, record: dict) -> dict:
        """Process a record."""
        ...
