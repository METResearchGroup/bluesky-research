import json
import traceback

from lib.log.logger import Logger
from services.consolidate_enrichment_integrations.helper import (
    do_consolidate_enrichment_integrations,
)  # noqa

logger = Logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info("Starting enrichment consolidation.")
        do_consolidate_enrichment_integrations()
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
