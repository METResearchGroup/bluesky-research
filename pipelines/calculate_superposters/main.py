"""Pipeline for calculating superposters."""
import sys

from lib.log.logger import Logger
from services.calculate_superposters.helper import calculate_latest_superposters  # noqa

logger = Logger(__name__)


def main():
    try:
        logger.info("Starting calculation of superposters.")
        calculate_latest_superposters()
        logger.info("Completed calculation of superposters.")
    except Exception as e:
        logger.error(f"Error in superposter calculation pipeline: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
