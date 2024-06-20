"""This service updates our database with the follows/followers for a given
user in the study."""
import sys
import traceback

from lib.log.logger import Logger
from services.update_network_connections.helper import update_network_connections  # noqa

logger = Logger(__name__)


def main():
    try:
        logger.info("Starting updating latest user engagement metrics.")
        update_network_connections()
        logger.info("Completed updating latest user engagement metrics.")
    except Exception as e:
        logger.error(f"Error in updating latest user engagement metrics: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
