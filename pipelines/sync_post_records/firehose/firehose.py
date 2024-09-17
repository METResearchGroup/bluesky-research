from lib.log.logger import Logger
from services.sync.stream.app import start_app


logger = Logger(__name__)


def get_posts() -> None:
    logger.info("Getting posts from the firehose.")
    try:
        # NOTE: should I restart this cursor? It's always being
        # recorded, never being used. Will have to consider.
        start_app(restart_cursor=True)
        logger.info("Successfully fetched posts from the firehose.")
    except Exception as e:
        logger.error(f"Error getting posts from the firehose: {e}")
        raise


if __name__ == "__main__":
    get_posts()
