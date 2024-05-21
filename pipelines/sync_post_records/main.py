"""Pipeline for getting posts from Bluesky.

Can either fetch raw posts from the firehose or from the "Most Liked" feed.

Run via `typer`: https://pypi.org/project/typer/

Example usage:
>>> python main.py --sync-type firehose
"""
import sys
import typer
from typing_extensions import Annotated

from most_liked import get_posts as get_most_liked_posts
from firehose import get_posts as get_firehose_posts

from enum import Enum


class SyncType(str, Enum):
    firehose = "firehose"
    most_liked = "most_liked"


def main(
    sync_type: Annotated[
        SyncType, typer.Option(help="Type of sync")
    ]
):
    sync_type = sync_type.value
    if sync_type == "firehose":
        get_firehose_posts()
    elif sync_type == "most_liked":
        get_most_liked_posts()
    else:
        print("Invalid sync type.")
        sys.exit(1)


if __name__ == "__main__":
    typer.run(main)
