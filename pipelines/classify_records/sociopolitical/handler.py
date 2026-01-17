import json
import traceback

from lib.constants import INTEGRATION_RUN_METADATA_TABLE_NAME
from lib.datetime_utils import generate_current_datetime_str
from lib.log.logger import get_logger
from services.ml_inference.models import ClassificationSessionModel
from services.ml_inference.sociopolitical.sociopolitical import classify_latest_posts

logger = get_logger(__name__)


def lambda_handler(event, context) -> dict:
    try:
        if not event:
            event = {
                "backfill_period": None,  # either "days" or "hours"
                "backfill_duration": None,
            }
        logger.info("Starting classification of latest posts.")
        backfill_period = event.get("backfill_period", None)
        backfill_duration = event.get("backfill_duration", None)
        if backfill_duration is not None:
            backfill_duration = int(backfill_duration)
        session_metadata_model: ClassificationSessionModel = classify_latest_posts(
            backfill_period=backfill_period,
            backfill_duration=backfill_duration,
            run_classification=True,
            event=event,
        )
        # Convert to dict for compatibility with existing code
        session_metadata = session_metadata_model.model_dump()
        logger.info("Completed classification of latest posts.")
        session_status_metadata = {
            "service": "ml_inference_sociopolitical",
            "timestamp": session_metadata["inference_timestamp"],
            "status_code": 200,
            "body": json.dumps("Classification of latest posts completed successfully"),
            "metadata_table_name": INTEGRATION_RUN_METADATA_TABLE_NAME,
            "metadata": json.dumps(session_metadata),
        }
        return session_status_metadata
    except Exception as e:
        logger.error(f"Error in classification of latest posts: {e}")
        logger.error(traceback.format_exc())
        logger.error(
            json.dumps(
                {
                    "status_code": 500,
                    "body": json.dumps(
                        f"Error in classification of latest posts: {str(e)}"
                    ),
                }
            )
        )
        session_status_metadata = {
            "service": "ml_inference_sociopolitical",
            "timestamp": generate_current_datetime_str(),
            "status_code": 500,
            "body": json.dumps(f"Error in classification of latest posts: {str(e)}"),
            "metadata_table_name": INTEGRATION_RUN_METADATA_TABLE_NAME,
            "metadata": json.dumps(traceback.format_exc()),
        }
        return session_status_metadata


if __name__ == "__main__":
    lambda_handler(None, None)
