"""Pipline for updating our database with records of the
likes/comments/reshares that our study users have on Bluesky.

We care about tracking the following metrics per user:
- Likes
- Comments
- Reshares
- Total follower count
- Total following count
- Total posts written

This pipeline is only for updating our database with the latest counts
of these metrics for our study users, not doing any calculations or
analysis on them.
"""
import sys
import traceback

from lib.log.logger import Logger
from services.update_user_bluesky_engagement.helper import update_latest_user_engagement_metrics  # noqa

logger = Logger(__name__)


def main():
    try:
        logger.info("Starting calculation of superposters.")
        update_latest_user_engagement_metrics()
        logger.info("Completed calculation of superposters.")
    except Exception as e:
        logger.error(f"Error in superposter calculation pipeline: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
