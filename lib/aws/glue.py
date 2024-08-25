"""Wrapper client for AWS Glue."""

from botocore.exceptions import CrawlerRunningException

from lib.aws.helper import create_client
from lib.log.logger import get_logger

logger = get_logger(__name__)


class Glue:
    """Wrapper class for all Glue interactions."""

    def __init__(self):
        self.client = create_client("glue")

    def start_crawler(self, crawler_name: str):
        """Start a crawler."""
        logger.info(f"Starting crawler {crawler_name}.")
        try:
            self.client.start_crawler(Name=crawler_name)
            logger.info(f"Crawler {crawler_name} started.")
        except CrawlerRunningException as e:
            logger.info(f"Crawler {crawler_name} is already running (this is OK): {e}")  # noqa
        except Exception as e:
            logger.error(f"Failed to start crawler {crawler_name}: {e}")
            raise e
