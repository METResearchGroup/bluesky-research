"""Router function for executing routing decisions via handlers."""

from lib.log.logger import get_logger

from services.sync.stream.context import CacheWriteContext
from services.sync.stream.record_processors.types import RoutingDecision
from services.sync.stream.types import Operation

logger = get_logger(__name__)


def route_decisions(
    decisions: list[RoutingDecision],
    record: dict,
    operation: Operation,
    context: CacheWriteContext,
) -> None:
    """Execute routing decisions by calling appropriate handlers.

    Takes a list of routing decisions and executes each one by:
    1. Getting the appropriate handler from the handler registry
    2. Calling handler.write_record() with the correct parameters
    3. Logging each execution
    4. Handling errors gracefully (log and continue with other decisions)

    Args:
        decisions: List of RoutingDecision objects to execute
        record: Record dictionary to write (transformed record)
        operation: Operation type (CREATE or DELETE)
        context: Cache write context with handler registry and dependencies

    Note:
        If a handler is not found or write_record() fails, the error is logged
        but processing continues with remaining decisions. This ensures that
        one failed decision doesn't prevent other valid routing paths from
        being executed.
    """
    if not decisions:
        return

    handler_registry = context.handler_registry

    for decision in decisions:
        try:
            # Get handler from registry
            handler = handler_registry.get_handler(decision.handler_key.value)

            # Log routing decision execution
            logger.info(
                f"Routing record to handler {decision.handler_key.value} "
                f"for author {decision.author_did} with filename {decision.filename}"
            )

            # Call handler.write_record() with decision parameters
            handler.write_record(
                record=record,
                operation=operation,
                author_did=decision.author_did,
                filename=decision.filename,
                follow_status=decision.follow_status,
            )

        except KeyError as e:
            # Handler not found in registry
            logger.error(
                f"Handler {decision.handler_key.value} not found in registry: {e}. "
                f"Skipping routing decision for {decision.filename}"
            )
            continue

        except Exception as e:
            # Handler.write_record() failed
            logger.error(
                f"Error writing record via handler {decision.handler_key.value} "
                f"for {decision.filename}: {e}. Continuing with other decisions."
            )
            continue
