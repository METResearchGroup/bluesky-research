from datetime import datetime
import os
from typing import Literal

import pandas as pd

from services.classify.inference import classify_texts_toxicity
from services.sync.search.helper import load_author_feeds_from_file

current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
classifications_filename = f"classified_posts_{current_datetime}.jsonl"
current_file_directory = os.path.dirname(os.path.abspath(__file__))
classifications_fp = os.path.join(current_file_directory, classifications_filename) # noqa


# unpack this into a load_post_from_author_feed function
# and then call that function repeatedly.
def load_posts_from_author_feeds(limit: int = None) -> list[str]:
    """Get post text from author feeds."""
    all_author_feeds = load_author_feeds_from_file()
    author_feeds = [
        feed_list["feed"] for feed_list in all_author_feeds
    ]
    lists_of_posts = [post_list for post_list in author_feeds]
    posts_texts = [
        post["post"]["record"]["text"]
        for post_list in lists_of_posts for post in post_list
    ]
    return posts_texts[:limit] if limit else post_texts


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


def main(event: dict, context: dict) -> int:
    """Run analyses"""
    posts: list[dict] = load_posts_from_author_feeds(
        limit=event.get("limit")
    )
    classified_posts: list[dict] = classify_texts_toxicity(posts)
    export_classified_posts(
        classified_posts=classified_posts,
        export_format=event.get("export_format")
    )
    return 0


if __name__ == "__main__":
    event = {"limit": 10, "export_format": "csv"}
    context = {}
    main(event=event, context=context)
