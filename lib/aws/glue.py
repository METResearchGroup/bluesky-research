"""Wrapper client for AWS Glue."""

from botocore.exceptions import ClientError

from lib.aws.helper import create_client
from lib.log.logger import get_logger

logger = get_logger(__name__)


class Glue:
    """Wrapper class for all Glue interactions."""

    def __init__(self):
        self.client = create_client("glue")

    def get_crawler_status(self, crawler_name: str) -> str:
        """Get the status of a crawler."""
        try:
            response = self.client.get_crawler(Name=crawler_name)
            return response["Crawler"]["State"]
        except Exception as e:
            logger.error(f"Failed to get status for crawler {crawler_name}: {e}")
            raise e

    def wait_for_crawler_completion(self, crawler_name: str, check_interval: int = 30):
        """Wait for a crawler to complete its run."""
        import time

        logger.info(f"Waiting for crawler {crawler_name} to complete...")
        while True:
            status = self.get_crawler_status(crawler_name)
            if status == "READY":
                logger.info(f"Crawler {crawler_name} has completed.")
                break
            elif status == "RUNNING":
                logger.info(
                    f"Crawler {crawler_name} is still running. Waiting {check_interval} seconds..."
                )
                time.sleep(check_interval)
            else:
                logger.warning(
                    f"Crawler {crawler_name} is in an unexpected state: {status}"
                )
                break

    def start_crawler(self, crawler_name: str):
        """Start a crawler."""
        logger.info(f"Starting crawler {crawler_name}.")
        try:
            self.client.start_crawler(Name=crawler_name)
            logger.info(f"Crawler {crawler_name} started.")
        except ClientError as e:
            if e.response["Error"]["Code"] == "CrawlerRunningException":
                logger.info(
                    f"Crawler {crawler_name} is already running (this is OK): {e}"
                )  # noqa
            else:
                raise e
        except Exception as e:
            logger.error(f"Failed to start crawler {crawler_name}: {e}")
            raise e
