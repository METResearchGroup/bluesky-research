import json
import traceback

from lib.log.logger import Logger
from services.generate_vector_embeddings.helper import do_vector_embeddings

logger = Logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info("Starting vector embedding generation.")
        do_vector_embeddings()
        logger.info("Completed vector embedding generation.")
        return {
            "statusCode": 200,
            "body": json.dumps("Vector generation completed successfully"),
        }
    except Exception as e:
        logger.error(f"Error in vector generation pipeline: {e}")
        logger.error(traceback.format_exc())
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error in vector generation pipeline: {str(e)}"),
        }


if __name__ == "__main__":
    lambda_handler(None, None)
