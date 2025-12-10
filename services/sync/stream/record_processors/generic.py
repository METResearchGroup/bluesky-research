from services.sync.stream.record_processors.protocol import RecordProcessorProtocol


class GenericRecordProcessor(RecordProcessorProtocol):
    """Processor for generic records."""

    def process_record(self, record: dict) -> dict:
        """Process a generic record."""
        return record
