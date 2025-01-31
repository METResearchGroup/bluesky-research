"""
Helper functions for the write_cache_buffers_to_db service.
"""

from lib.log.logger import get_logger

logger = get_logger(__name__)

MAP_SERVICE_TO_IO = {
    "ml_inference_perspective_api": {
        "input": "",  # the input queue.
        "output": "",  # the DB.
    }
}


def write_cache_buffer_queue_to_db(
    service: str,
    input_queue: str,
    output_db: str,
):
    pass


def write_all_cache_buffer_queues_to_dbs():
    for service, io_dict in MAP_SERVICE_TO_IO.items():
        logger.info(f"Migrating cache buffer queue for service {service}...")
        write_cache_buffer_queue_to_db(
            service=service,
            input_queue=io_dict["input"],
            output_db=io_dict["output"],
        )
