"""Thin orchestrator for firehose operations callback.

This module provides the operations_callback function that is passed to
the firehose stream. It orchestrates record processing using the
record processor architecture.
"""

from lib.log.logger import get_logger

from services.sync.stream.core.context import CacheWriteContext
from services.sync.stream.record_processors.router import route_decisions
from services.sync.stream.core.record_types import SUPPORTED_RECORD_TYPES
from services.sync.stream.core.types import Operation, OperationsByType

logger = get_logger(__name__)


def _validate_operations_structure(operations_by_type: dict) -> None:
    """Validate the structure of operations_by_type dictionary.

    Ensures that the dictionary has the expected structure:
    - Keys are record type strings
    - Values are dictionaries with 'created' and 'deleted' keys
    - 'created' and 'deleted' are lists

    Args:
        operations_by_type: Dictionary to validate

    Raises:
        ValueError: If structure is invalid
    """
    if not isinstance(operations_by_type, dict):
        raise ValueError("operations_by_type must be a dictionary")

    for record_type, operations in operations_by_type.items():
        if not isinstance(operations, dict):
            raise ValueError(
                f"Operations for {record_type} must be a dictionary, got {type(operations)}"
            )

        if "created" not in operations or "deleted" not in operations:
            raise ValueError(
                f"Operations for {record_type} must have 'created' and 'deleted' keys"
            )

        if not isinstance(operations["created"], list):
            raise ValueError(
                f"'created' for {record_type} must be a list, got {type(operations['created'])}"
            )

        if not isinstance(operations["deleted"], list):
            raise ValueError(
                f"'deleted' for {record_type} must be a list, got {type(operations['deleted'])}"
            )


def operations_callback(
    operations_by_type: OperationsByType | dict, context: CacheWriteContext
) -> bool:
    """Callback for managing records during firehose stream.

    This function takes as input a dictionary of the format:
    {
        'posts': {'created': [], 'deleted': []},
        'reposts': {'created': [], 'deleted': []},
        'likes': {'created': [], 'deleted': []},
        'follows': {'created': [], 'deleted': []},
        'lists': {'created': [], 'deleted': []},
        'blocks': {'created': [], 'deleted': []},
        'profiles': {'created': [], 'deleted': []},
    }

    It processes each record type using the appropriate processor, which:
    1. Transforms the raw firehose record to a domain model
    2. Determines routing decisions (which handlers should process it)
    3. Executes routing decisions via handlers

    Args:
        operations_by_type: Dictionary mapping record types to created/deleted lists.
            Should conform to OperationsByType structure, but accepts dict for
            backward compatibility and runtime flexibility.
        context: Cache write context with dependencies

    Returns:
        True if processing succeeded, False otherwise

    Raises:
        ValueError: If operations_by_type structure is invalid
        Exception: If processing fails (maintains current behavior)
    """
    try:
        # Validate structure at runtime
        _validate_operations_structure(operations_by_type)

        processor_registry = context.processor_registry

        # Use centralized list of supported types
        supported_types = SUPPORTED_RECORD_TYPES

        for record_type in supported_types:
            if record_type not in operations_by_type:
                continue

            records = operations_by_type[record_type]
            processor = processor_registry.get_processor(record_type)

            for record in records.get("created", []):
                try:
                    transformed = processor.transform(record, Operation.CREATE)
                    decisions = processor.get_routing_decisions(
                        transformed, Operation.CREATE, context
                    )
                    route_decisions(decisions, transformed, Operation.CREATE, context)
                except Exception as e:
                    logger.error(f"Error processing {record_type} record (CREATE): {e}")
                    # we choose to make exceptions non-fatal as this is a real-time
                    # streaming pipeline, so we continue processing other records.
                    # NOTE: could be something to revisit, i.e., the line between
                    # graceful degradation and outright failure, especially if there
                    # are a lot of errors at once. For now, can just have telemetry
                    # monitoring the errors.
                    continue

            for record in records.get("deleted", []):
                try:
                    transformed = processor.transform(record, Operation.DELETE)
                    decisions = processor.get_routing_decisions(
                        transformed, Operation.DELETE, context
                    )
                    route_decisions(decisions, transformed, Operation.DELETE, context)
                except Exception as e:
                    logger.error(f"Error processing {record_type} record (DELETE): {e}")
                    continue
        return True
    except Exception as e:
        logger.error(f"Error processing firehose operations: {e}")
        raise e
