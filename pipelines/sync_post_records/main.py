"""Pipeline for getting posts from Bluesky.

Can either fetch raw posts from the firehose or from the "Most Liked" feed.

Run via `typer`: https://pypi.org/project/typer/

Example usage:
>>> python main.py --sync-type firehose
"""
import sys
import typer
from typing_extensions import Annotated

from firehose import get_posts as get_firehose_posts
from lib.log.logger import Logger
from most_liked import get_posts as get_most_liked_posts

from enum import Enum

logger = Logger(__name__)


class SyncType(str, Enum):
    firehose = "firehose"
    most_liked = "most_liked"


def main(
    sync_type: Annotated[
        SyncType, typer.Option(help="Type of sync")
    ]
):
    try:
        logger.info(f"Starting sync with type: {sync_type}")
        if sync_type == "firehose":
            get_firehose_posts()
        elif sync_type == "most_liked":
            get_most_liked_posts()
        else:
            print("Invalid sync type.")
            logger.error("Invalid sync type provided.")
            sys.exit(1)
        logger.info(f"Completed sync with type: {sync_type}")
    except Exception as e:
        logger.error(f"Error in sync pipeline: {e}")
        sys.exit(1)


if __name__ == "__main__":
    typer.run(main)
