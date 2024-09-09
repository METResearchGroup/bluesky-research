import json
import traceback

from lib.log.logger import Logger
from services.consolidate_enrichment_integrations.helper import (
    do_consolidate_enrichment_integrations,
)  # noqa

logger = Logger(__name__)


def lambda_handler(event: dict, context: dict):
    try:
        if not event:
            event = {
                "backfill_period": None,  # either "days" or "hours"
                "backfill_duration": None,
            }
        logger.info("Starting enrichment consolidation.")
        backfill_period = event.get("backfill_period", None)
        backfill_duration = event.get("backfill_duration", None)
        if backfill_duration is not None:
            backfill_duration = int(backfill_duration)
        do_consolidate_enrichment_integrations(
            backfill_period=backfill_period, backfill_duration=backfill_duration
        )
        logger.info("Completed enrichment consolidation.")
        return {
            "statusCode": 200,
            "body": json.dumps("Enrichment consolidation completed successfully"),
        }
    except Exception as e:
        logger.error(f"Error in enrichment consolidation pipeline: {e}")
        logger.error(traceback.format_exc())
        logger.error(
            json.dumps(
                {
                    "statusCode": 500,
                    "body": json.dumps(
                        f"Error in enrichment consolidation pipeline: {str(e)}"
                    ),
                }
            )
        )
        raise


if __name__ == "__main__":
    lambda_handler(None, None)
