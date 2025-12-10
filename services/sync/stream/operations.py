"""Thin orchestrator for firehose operations callback.

This module provides the operations_callback function that is passed to
the firehose stream. It orchestrates record processing using the
record processor architecture.
"""

from lib.log.logger import get_logger

from services.sync.stream.context import CacheWriteContext
from services.sync.stream.record_processors.factories import create_all_processors
from services.sync.stream.record_processors.router import route_decisions
from services.sync.stream.types import Operation

logger = get_logger(__name__)


def operations_callback(operations_by_type: dict, context: CacheWriteContext) -> bool:
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
        operations_by_type: Dictionary mapping record types to created/deleted lists
        context: Cache write context with dependencies

    Returns:
        True if processing succeeded, False otherwise

    Raises:
        Exception: If processing fails (maintains current behavior)
    """
    try:
        # Create processor registry with all processors
        processor_registry = create_all_processors(context)

        # Process each record type
        record_type_mapping = {
            "posts": "posts",
            "likes": "likes",
            "follows": "follows",
        }

        for firehose_type, processor_type in record_type_mapping.items():
            if firehose_type not in operations_by_type:
                continue

            records = operations_by_type[firehose_type]
            processor = processor_registry.get_processor(processor_type)

            # Process created records
            for record in records.get("created", []):
                try:
                    transformed = processor.transform(record, Operation.CREATE)
                    decisions = processor.get_routing_decisions(
                        transformed, Operation.CREATE, context
                    )
                    route_decisions(decisions, transformed, Operation.CREATE, context)
                except Exception as e:
                    logger.error(
                        f"Error processing {firehose_type} record (CREATE): {e}"
                    )
                    # Continue processing other records
                    continue

            # Process deleted records
            for record in records.get("deleted", []):
                try:
                    transformed = processor.transform(record, Operation.DELETE)
                    decisions = processor.get_routing_decisions(
                        transformed, Operation.DELETE, context
                    )
                    route_decisions(decisions, transformed, Operation.DELETE, context)
                except Exception as e:
                    logger.error(
                        f"Error processing {firehose_type} record (DELETE): {e}"
                    )
                    # Continue processing other records
                    continue

        # Handle record types that don't have processors yet (placeholders)
        # These are currently no-ops but maintain compatibility
        for record_type in ["reposts", "lists", "blocks", "profiles"]:
            if record_type in operations_by_type:
                records = operations_by_type[record_type]
                if records.get("created") or records.get("deleted"):
                    logger.debug(
                        f"Skipping {record_type} - processor not yet implemented"
                    )

        return True
    except Exception as e:
        logger.error(f"Error in exporting latest writes to cache: {e}")
        raise e
