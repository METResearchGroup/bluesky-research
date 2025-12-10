"""Generic record handler implementation."""

import os
from services.sync.stream.handlers.config import HandlerConfig
from services.sync.stream.protocols import (
    PathManagerProtocol,
    FileUtilitiesProtocol,
    RecordHandlerProtocol,
)
from services.sync.stream.types import (
    Operation,
    FollowStatus,
    RecordType,
    GenericRecordType,
)


class GenericRecordHandler(RecordHandlerProtocol):
    """Generic handler for record types using configuration.

    This replaces the need for separate handler classes for each record type.
    Behavior is controlled via HandlerConfig.
    """

    def __init__(
        self,
        config: HandlerConfig,
        path_manager: PathManagerProtocol,
        file_writer: FileUtilitiesProtocol,
        file_reader: FileUtilitiesProtocol,
    ):
        """Initialize generic handler.

        Args:
            config: Handler configuration defining behavior
            path_manager: Path manager for constructing paths
            file_writer: File writer for writing records
            file_reader: File reader for reading records
        """
        self.config = config
        self.path_manager = path_manager
        self.file_writer = file_writer
        self.file_reader = file_reader

    def write_record(
        self,
        record: dict,
        operation: Operation | str,
        author_did: str,
        filename: str,
        follow_status: FollowStatus | str | None = None,
        **kwargs,
    ) -> None:
        """Write record to cache using configured path strategy.

        Args:
            record: Record data to write
            operation: Operation type (create/delete)
            author_did: Author DID
            filename: Filename for the record
            follow_status: Follow status (required for follow records)
            **kwargs: Additional arguments
        """
        # Convert string to enum if needed
        if isinstance(operation, str):
            try:
                operation = Operation(operation)
            except ValueError as e:
                raise ValueError(
                    f"Invalid operation value: {operation}. "
                    f"Must be one of: {[op.value for op in Operation]}"
                ) from e
        if isinstance(follow_status, str):
            try:
                follow_status = FollowStatus(follow_status)
            except ValueError as e:
                raise ValueError(
                    f"Invalid follow_status value: {follow_status}. "
                    f"Must be one of: {[fs.value for fs in FollowStatus]}"
                ) from e

        # Validate follow_status if required
        if self.config.requires_follow_status and follow_status is None:
            raise ValueError(
                f"follow_status is required for {self.config.record_type} records"
            )

        # Get base path using path strategy
        # Pass author_did and follow_status in kwargs for path strategies that need them
        base_path = self.config.path_strategy(
            self.path_manager,
            operation,
            self.config.record_type,
            follow_status=follow_status,
            author_did=author_did,
        )

        # Apply nested path if configured
        if self.config.nested_path_extractor:
            nested_path = self.config.nested_path_extractor(record)
            base_path = os.path.join(base_path, nested_path)

        # Construct full path
        full_path = os.path.join(base_path, filename)

        # Write record
        self.file_writer.write_json(full_path, record)

    def read_records(self, base_path: str) -> tuple[list[dict], list[str]]:
        """Read all records from cache using configured read strategy.

        Args:
            base_path: Base path to read from (operation-level path)

        Returns:
            Tuple of (records, filepaths)
        """
        if self.config.read_strategy:
            return self.config.read_strategy(
                self.file_reader, base_path, self.config.record_type
            )
        else:
            # Default: read all JSON files in record type directory
            record_dir = os.path.join(base_path, self.config.record_type.value)
            return self.file_reader.read_all_json_in_directory(record_dir)

    def get_record_type(self) -> RecordType | GenericRecordType:
        """Get the record type this handler manages.

        TODO: When we reconsider changing the data models, we should consolidate
        RecordType and GenericRecordType to have a single source of truth for
        record type definitions. This will allow us to simplify this return type
        and reduce the need for stringly-typed routing.
        """
        return self.config.record_type
