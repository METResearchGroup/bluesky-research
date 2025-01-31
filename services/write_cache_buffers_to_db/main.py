"""
This script is used to write cache buffer queues to their corresponding databases.
"""

from services.write_cache_buffers_to_db.helper import (
    write_all_cache_buffer_queues_to_dbs,
    write_cache_buffer_queue_to_db,
    MAP_SERVICE_TO_IO,
)


def write_cache_buffers_to_db(payload: dict):
    """Writes cache buffer queues to their corresponding databases based on the provided payload.

    Args:
        payload (dict): Configuration for the write process. Expected format:
            {
                "service": str,  # Name of specific service to migrate, or "all" to migrate all services
            }

            If service="all", migrates all services defined in MAP_SERVICE_TO_IO
            If service is a specific service name, migrates only that service's cache buffer

    Example:
        # Migrate all services
        payload = {
            "service": "all"
        }

        # Migrate specific service
        payload = {
            "service": "ml_inference_perspective_api"
        }
    """
    service = payload.get("service")

    if service == "all":
        write_all_cache_buffer_queues_to_dbs()
    elif service in MAP_SERVICE_TO_IO:
        io_dict = MAP_SERVICE_TO_IO[service]
        write_cache_buffer_queue_to_db(
            service=service,
            input_queue=io_dict["input"],
            output_db=io_dict["output"],
        )
    else:
        raise ValueError(
            f"Invalid service: {service}. Must be 'all' or one of: {list(MAP_SERVICE_TO_IO.keys())}"
        )
