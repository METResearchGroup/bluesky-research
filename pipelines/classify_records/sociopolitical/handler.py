import json
import traceback

from lib.log.logger import get_logger
from services.ml_inference.sociopolitical.sociopolitical import classify_latest_posts

logger = get_logger(__name__)


def lambda_handler(event, context):
    try:
        if not event:
            event = {
                "backfill_period": None,  # either "days" or "hours"
                "backfill_duration": None,
            }
        backfill_period = event.get("backfill_period", None)
        backfill_duration = event.get("backfill_duration", None)
        if backfill_duration is not None:
            backfill_duration = int(backfill_duration)
        logger.info("Starting classification of latest sociopolitical posts.")
        classify_latest_posts(
            backfill_period=backfill_period, backfill_duration=backfill_duration
        )
        logger.info("Completed classification of latest sociopolitical posts.")
        return {
            "statusCode": 200,
            "body": json.dumps(
                "Classification of latest sociopolitical posts completed successfully"
            ),
        }
    except Exception as e:
        logger.error(f"Error in classification of latest sociopolitical posts: {e}")
        logger.error(traceback.format_exc())
        logger.error(
            json.dumps(
                {
                    "statusCode": 500,
                    "body": json.dumps(
                        f"Error in classification of latest sociopolitical posts: {str(e)}"
                    ),
                }
            )
        )
        raise


if __name__ == "__main__":
    lambda_handler(None, None)
