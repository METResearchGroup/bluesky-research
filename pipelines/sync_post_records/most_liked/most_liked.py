from lib.log.logger import Logger
from services.sync.most_liked_posts.helper import main as get_most_liked_posts

logger = Logger(__name__)


def get_posts() -> None:
    logger.info("Getting posts from the most liked feed.")
    try:
        args = {
            "use_latest_local": False,
            "store_local": False,
            "store_remote": True,
            "feeds": ["today"],
        }
        get_most_liked_posts(**args)
        logger.info("Successfully got posts from the most liked feed.")
    except Exception as e:
        logger.error(f"Error getting posts from the most liked feed: {e}")
        raise


if __name__ == "__main__":
    get_posts()
