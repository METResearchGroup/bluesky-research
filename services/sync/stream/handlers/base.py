"""Base handler and protocol implementations."""

from services.sync.stream.protocols import (
    PathManagerProtocol,
    FileWriterProtocol,
    FileReaderProtocol,
)


class BaseRecordHandler:
    """Base implementation for record handlers.

    Provides common functionality that all handlers can use.
    """

    def __init__(
        self,
        path_manager: PathManagerProtocol,
        file_writer: FileWriterProtocol,
        file_reader: FileReaderProtocol,
        record_type: str,
    ):
        """Initialize base handler.

        Args:
            path_manager: Path manager for constructing paths
            file_writer: File writer for writing records
            file_reader: File reader for reading records
            record_type: Type of record this handler manages
        """
        self.path_manager = path_manager
        self.file_writer = file_writer
        self.file_reader = file_reader
        self._record_type = record_type

    def get_record_type(self) -> str:
        """Get the record type this handler manages."""
        return self._record_type
