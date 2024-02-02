from datetime import datetime
import os
from typing import Literal

from atproto_client.models.app.bsky.feed.defs import FeedViewPost
import pandas as pd

from lib.helper import track_function_runtime
from services.classify.helper import preprocess_posts_for_inference
from services.classify.inference import perform_inference
from services.sync.search.helper import load_author_feeds_from_file
from services.transform.transform_raw_data import (
    hydrate_feed_view_post, FlattenedFeedViewPost
)


current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
classifications_filename = f"classified_posts_{current_datetime}.jsonl"
current_file_directory = os.path.dirname(os.path.abspath(__file__))
classifications_fp = os.path.join(current_file_directory, classifications_filename) # noqa


def load_posts_from_author_feeds(limit: int = None) -> list[FeedViewPost]:
    """Get individual posts from the author feeds."""
    all_author_feeds = load_author_feeds_from_file()
    posts: list[FeedViewPost] = []
    for author_feed in all_author_feeds:
        feed = author_feed["feed"]
        for feed_post in feed:
            post = hydrate_feed_view_post(feed_post)
            posts.append(post)

    return posts[:limit] if limit else posts


def export_classified_posts(
    classified_posts: list[dict],
    export_format: Literal["json", "csv"]="json"
) -> None:
    if export_format == "json":
        with open(classifications_fp, "w") as f:
            for post in classified_posts:
                f.write(post + "\n")
        print(f"Wrote {len(classified_posts)} classified posts to {classifications_filename}") # noqa
    else:
        df_filename = f"classified_posts_{current_datetime}.csv"
        df_fp = os.path.join(current_file_directory, df_filename)
        df = pd.DataFrame(classified_posts)
        df.to_csv(df_fp, index=False)
        print(f"Wrote {len(classified_posts)} classified posts to {df_filename}") # noqa


@track_function_runtime
def main(event: dict, context: dict) -> int:
    """Run analyses"""
    raw_posts: list[FeedViewPost] = load_posts_from_author_feeds(
        limit=event.get("limit")
    )
    posts: list[FlattenedFeedViewPost] = preprocess_posts_for_inference(raw_posts)
    classified_posts: list[dict] = perform_inference(posts)
    export_classified_posts(
        classified_posts=classified_posts,
        export_format=event.get("export_format")
    )
    return 0


# TODO: do I need to include rate-limiting for the Google API inference?
if __name__ == "__main__":
    event = {"limit": 250, "export_format": "csv"}
    context = {}
    main(event=event, context=context)
