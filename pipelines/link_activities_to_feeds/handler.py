import json
import traceback

from lib.log.logger import Logger
from services.link_activities_to_feeds.helper import link_activities_to_feeds

logger = Logger(__name__)


def lambda_handler(event: dict, context: dict):
    try:
        if not event:
            event = {
                "backfill_period": None,  # either "days" or "hours"
                "backfill_duration": None,
            }
        logger.info("Starting link activities to feeds process.")
        backfill_period = event.get("backfill_period", None)
        backfill_duration = event.get("backfill_duration", None)
        if backfill_duration is not None:
            backfill_duration = int(backfill_duration)
        link_activities_to_feeds(
            backfill_period=backfill_period, backfill_duration=backfill_duration
        )
        logger.info("Completed link activities to feeds process.")
        return {
            "statusCode": 200,
            "body": json.dumps(
                "Link activities to feeds process completed successfully"
            ),
        }
    except Exception as e:
        logger.error(f"Error in link activities to feeds process: {e}")
        logger.error(traceback.format_exc())
        logger.error(
            json.dumps(
                {
                    "statusCode": 500,
                    "body": json.dumps(
                        f"Error in link activities to feeds process: {str(e)}"
                    ),
                }
            )
        )
        raise


if __name__ == "__main__":
    lambda_handler(None, None)
