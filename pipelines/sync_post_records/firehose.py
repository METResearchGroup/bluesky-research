from lib.log.logger import Logger
from services.sync.stream.app import start_app


logger = Logger(__name__)


def get_posts() -> None:
    logger.info("Getting posts from the firehose.")
    try:
        start_app()
        logger.info("Successfully fetched posts from the firehose.")
    except Exception as e:
        logger.error(f"Error getting posts from the firehose: {e}")
        raise
