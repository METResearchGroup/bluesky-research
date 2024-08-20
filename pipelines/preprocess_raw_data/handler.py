import json
import sys
import traceback

from lib.log.logger import Logger
from services.preprocess_raw_data.helper import preprocess_latest_raw_data

logger = Logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info("Starting preprocessing pipeline in Lambda.")
        preprocess_latest_raw_data()
        logger.info("Completed preprocessing pipeline in Lambda.")
        return {
            'statusCode': 200,
            'body': json.dumps('Preprocessing completed successfully')
        }
    except Exception as e:
        logger.error(f"Error in preprocessing pipeline: {e}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error in preprocessing pipeline: {str(e)}')
        }


if __name__ == "__main__":
    lambda_handler(None, None)
