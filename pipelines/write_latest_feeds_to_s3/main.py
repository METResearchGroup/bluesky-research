"""This service takes the latest feeds for each user and writes them to S3."""
import sys
import traceback

from lib.log.logger import Logger
from services.write_latest_feeds_to_s3.helper import write_latest_feeds_to_s3

logger = Logger(__name__)


def main():
    try:
        logger.info("Started writing latest feeds to S3.")
        write_latest_feeds_to_s3()
        logger.info("Completed writing latest feeds to S3.")
    except Exception as e:
        logger.error(f"Error in writing latest feeds to S3: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
