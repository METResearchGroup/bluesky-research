from lib.log.logger import Logger
from services.sync.most_liked_posts.helper import main as get_most_liked_posts

logger = Logger(__name__)


def lambda_handler(event, context):
    logger.info("Getting posts from the most liked feed.")
    try:
        args = {
            "use_latest_local": True,
            "store_local": True,
            "store_remote": True,
            "feeds": ["today"]
        }
        get_most_liked_posts(**args)
        logger.info("Successfully got posts from the most liked feed.")
        return {
            'statusCode': 200,
            'body': 'Successfully synced most liked posts'
        }
    except Exception as e:
        logger.error(f"Error getting posts from the most liked feed: {e}")
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
