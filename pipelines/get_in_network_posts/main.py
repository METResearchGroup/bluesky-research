"""This service updates our database with the latest in-network posts that are
available.
"""
import sys
import traceback

from lib.log.logger import Logger


logger = Logger(__name__)


def main():
    try:
        logger.info("Starting obtaining in-network posts.")

        logger.info("Completed obtaining in-network posts.")
    except Exception as e:
        logger.error(f"Error in obtaining in-network posts: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
