import json
import traceback

from lib.log.logger import get_logger
from services.compact_all_services.helper import compact_all_local_services

logger = get_logger(__name__)


def lambda_handler(event, context):
    try:
        # if not event:
        #     event = {
        #         "backfill_period": None,  # either "days" or "hours"
        #         "backfill_duration": None,
        #         "fetch_all_keys": False,
        #     }
        # backfill_period = event.get("backfill_period", None)
        # backfill_duration = event.get("backfill_duration", None)
        # if backfill_duration is not None:
        #     backfill_duration = int(backfill_duration)
        logger.info("Starting compact all services.")
        compact_all_local_services()
        logger.info("Completed compact all services.")
        return {
            "statusCode": 200,
            "body": json.dumps("Compact dedupe preprocessing completed successfully"),
        }
    except Exception as e:
        logger.error(f"Error in compact dedupe preprocessing pipeline: {e}")
        logger.error(traceback.format_exc())
        logger.error(
            json.dumps(
                {
                    "statusCode": 500,
                    "body": json.dumps(
                        f"Error in compact dedupe preprocessing pipeline: {str(e)}"
                    ),
                }
            )
        )
        raise


if __name__ == "__main__":
    lambda_handler(None, None)
