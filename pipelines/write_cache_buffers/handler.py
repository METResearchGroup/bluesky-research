import json
import traceback

from lib.log.logger import get_logger
from services.write_cache_buffers_to_db.main import write_cache_buffers_to_db

logger = get_logger(__name__)


def lambda_handler(event, context):
    """Lambda handler for migrating cache buffer queues to databases.

    Args:
        event (dict): Lambda event containing configuration payload. Expected format:
            {
                "payload": {
                    "service": str,  # Name of specific service to migrate, or "all" to migrate all services
                    "clear_queue": bool,  # Optional. Whether to clear the queue after writing. Defaults to False.
                    "bypass_write": bool,  # Optional. If True, skips writing to DB and only clears cache. Defaults to False.
                }
            }
        context: Lambda context object (unused)

    Returns:
        dict: Response object with status code and message
            Success: {"statusCode": 200, "body": "Cache buffer write completed successfully"}
            Error: {"statusCode": 500, "body": "Error message"}

    Raises:
        Exception: Propagates any exceptions that occur during write
        KeyError: If required 'service' field is missing from payload
        ValueError: If service name is not 'all' and not in SERVICES_TO_WRITE
        TypeError: If clear_queue is provided but not a boolean

    Example:
        # Migrate all services
        event = {
            "payload": {
                "service": "all",
                "clear_queue": False
            }
        }

        # Migrate specific service and clear queue
        event = {
            "payload": {
                "service": "ml_inference_perspective_api",
                "clear_queue": True
            }
        }
    """
    try:
        logger.info("Starting cache buffer write process.")
        payload = event.get("payload", {})
        write_cache_buffers_to_db(payload)
        logger.info("Completed cache buffer write process.")
        return {
            "statusCode": 200,
            "body": json.dumps("Cache buffer write completed successfully"),
        }
    except Exception as e:
        logger.error(f"Error in cache buffer write process: {e}")
        logger.error(traceback.format_exc())
        logger.error(
            json.dumps(
                {
                    "statusCode": 500,
                    "body": json.dumps(
                        f"Error in cache buffer write process: {str(e)}"
                    ),
                }
            )
        )
        raise


if __name__ == "__main__":
    lambda_handler(
        {
            "payload": {
                "service": "all"  # Migrate all services
            }
        },
        None,
    )
