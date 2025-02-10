"""
This script is used to write cache buffer queues to their corresponding databases.
"""

from lib.log.logger import get_logger
from services.write_cache_buffers_to_db.helper import (
    write_all_cache_buffer_queues_to_dbs,
    write_cache_buffer_queue_to_db,
    SERVICES_TO_WRITE,
)

logger = get_logger(__file__)


def write_cache_buffers_to_db(payload: dict):
    """Writes cache buffer queues to their corresponding databases based on the provided payload.

    Args:
        payload (dict): Configuration for the write process. Expected format:
            {
                "service": str,  # Name of specific service to migrate, or "all" to migrate all services
                "clear_queue": bool,  # Optional. Whether to clear the queue after writing. Defaults to False.
            }

            If service="all", migrates all services defined in MAP_SERVICE_TO_IO
            If service is a specific service name, migrates only that service's cache buffer

    Example:
        # Migrate all services
        payload = {
            "service": "all",
            "clear_queue": False
        }

        # Migrate specific service and clear queue
        payload = {
            "service": "ml_inference_perspective_api",
            "clear_queue": True
        }

    Raises:
        KeyError: If required 'service' field is missing from payload
        ValueError: If service name is not 'all' and not in SERVICES_TO_WRITE
        TypeError: If clear_queue is provided but not a boolean
    """
    if "service" not in payload:
        logger.error("Missing required 'service' field in payload")
        raise KeyError("Missing required 'service' field in payload")

    service = payload["service"]
    clear_queue = payload.get("clear_queue", False)

    # Validate clear_queue type
    if not isinstance(clear_queue, bool):
        logger.error(f"Invalid clear_queue type: {type(clear_queue)}. Must be boolean.")
        raise TypeError("clear_queue must be a boolean")

    logger.info(f"Processing write request for service: {service}")

    if service == "all":
        logger.info("Writing all cache buffer queues to databases")
        write_all_cache_buffer_queues_to_dbs(clear_queue=clear_queue)
    elif service in SERVICES_TO_WRITE:
        logger.info(f"Writing cache buffer queue for service: {service}")
        write_cache_buffer_queue_to_db(service=service, clear_queue=clear_queue)
    else:
        error_msg = (
            f"Invalid service: {service}. Must be 'all' or one of: {SERVICES_TO_WRITE}"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)
