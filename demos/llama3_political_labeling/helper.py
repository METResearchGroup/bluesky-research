"""Helper functions for labeling political posts with Llama3."""
import json
import os
from typing import Literal

import pandas as pd
from pymongo.errors import DuplicateKeyError

from lib.constants import current_datetime
from lib.db.mongodb import (
    chunk_insert_posts, get_mongodb_collection, load_collection
)
from services.sync.most_liked_posts.helper import full_sync_dir

DEFAULT_POST_LIMIT = 100
DEFAULT_BATCH_SIZE = 5

task_name = "llm_political_labeling"
mongo_collection_name, mongo_collection = get_mongodb_collection(
    task=task_name
)

current_file_dir = os.path.dirname(os.path.abspath(__file__))
current_datetime_str = current_datetime.strftime("%Y-%m-%d-%H:%M:%S")
label_dir = "labeled_data"
urls_dir = "post_urls"
labeled_data_fp = os.path.join(current_file_dir, label_dir, f"labeled_posts_{current_datetime_str}.jsonl")
urls_fp = os.path.join(current_file_dir, urls_dir, f"urls_{current_datetime_str}.csv")


def load_posts_local() -> list[dict]:
    """Loads the most recent synced posts from local storage."""
    sync_files = os.listdir(full_sync_dir)
    most_recent_filename = sorted(sync_files)[-1]
    sync_fp = os.path.join(full_sync_dir, most_recent_filename)
    print(f"Loading most recent sync file {sync_fp}")
    with open(sync_fp, "r") as f:
        posts: list[dict] = [json.loads(line) for line in f]
    return posts


def load_posts_from_mongodb(limit: int = DEFAULT_POST_LIMIT) -> list[dict]:
    """Loads posts from MongoDB."""
    posts = load_collection(collection=mongo_collection, limit=limit)
    return posts


def load_posts(
    source=Literal["local", "remote"], limit: int = DEFAULT_POST_LIMIT
) -> list[dict]:
    """Load posts for labeling."""
    if source == "local":
        return load_posts_local()
    elif source == "remote":
        return load_posts_from_mongodb(limit=limit)
    else:
        raise ValueError(f"Invalid source valid: {source}")


def create_post_batches(posts: list[dict]) -> list[list[dict]]:
    """Given a list of posts, create batches of posts."""
    pass


def label_batch_posts(batch: list[dict]) -> list[dict]:
    """Label a batch of posts."""
    pass


def export_batch_posts_local(batch: list[dict]) -> None:
    pass


def export_batch_posts_remote(
    batch: list[dict],
    bulk_write_remote: bool = True,
    bulk_chunk_size: int = 100
) -> None:
    """Batch exports posts to MongoDB."""
    duplicate_key_count = 0
    total_successful_inserts = 0
    total_posts = len(batch)
    print(f"Inserting {total_posts} posts to MongoDB collection {mongo_collection_name}")  # noqa
    formatted_posts_mongodb = [
        {"_id": post["post"]["uri"], **post}
        for post in batch
    ]
    if bulk_write_remote:
        print("Inserting into MongoDB in bulk...")
        total_successful_inserts, duplicate_key_count = chunk_insert_posts(
            posts=formatted_posts_mongodb,
            mongo_collection=mongo_collection,
            chunk_size=bulk_chunk_size
        )
        print("Finished bulk inserting into MongoDB.")
    else:
        for idx, post in enumerate(batch):
            if idx % 100 == 0:
                print(f"Inserted {idx}/{total_posts} posts")
            try:
                post_uri = post["post"]["uri"]
                # set the URI as the primary key.
                # NOTE: if this doesn't work, check if the IP address has
                # permission to access the database.
                mongo_collection.insert_one(
                    {"_id": post_uri, **post},
                )
                total_successful_inserts += 1
            except DuplicateKeyError:
                duplicate_key_count += 1

    if duplicate_key_count > 0:
        print(f"Skipped {duplicate_key_count} duplicate posts")
    print(f"Inserted {total_successful_inserts} posts to remote MongoDB collection {mongo_collection_name}")  # noqa


def export_batch_posts(
    batch: list[dict],
    store_local: bool = True,
    store_remote: bool = True,
    bulk_write_remote: bool = True,
    bulk_chunk_size: int = 100
):
    """Export batch of labeled posts"""
    if store_local:
        export_batch_posts_local(batch=batch)

    if store_remote:
        total_posts = len(batch)
        print(f"Inserting {total_posts} posts to MongoDB collection {mongo_collection_name}")  # noqa
        formatted_posts_mongodb = [
            {"_id": post["post"]["uri"], **post}
            for post in batch
        ]
        if bulk_write_remote:
            print("Inserting into MongoDB in bulk...")
            chunk_insert_posts(
                posts=formatted_posts_mongodb,
                mongo_collection=mongo_collection,
                chunk_size=bulk_chunk_size
            )
            print("Finished bulk inserting into MongoDB.")


def label_and_export_batch_posts(
    batch: list[dict],
    store_local: bool = True,
    store_remote: bool = True,
    bulk_write_remote: bool = True
) -> None:
    """Label and export a batch of posts."""
    labeled_batch = label_batch_posts(batch=batch)
    export_batch_posts(
        batch=labeled_batch,
        store_local=store_local,
        store_remote=store_remote,
        bulk_write_remote=bulk_write_remote,
        bulk_chunk_size=len(batch)
    )


def label_and_export_posts(
    posts_to_label: list[dict],
    store_local: bool = True,
    store_remote: bool = True,
    bulk_write_remote: bool = True
) -> None:
    """Label posts and export them."""
    batches: list[list[dict]] = create_post_batches(posts=posts_to_label)
    total_batches = len(batches)
    for idx, batch in enumerate(batches):
        print(f"Classiying batch {idx} out of {total_batches} batches...")
        label_and_export_batch_posts(
            batch=batch,
            store_local=store_local,
            store_remote=store_remote,
            bulk_write_remote=bulk_write_remote
        )
        print(f"Finished classifying and exporting batch {idx} out of {total_batches} batches...")  # noqa
    print("Finished labeling and exporting labeled posts in batch.")


def load_and_label_posts() -> list[dict]:
    """Load posts and label them."""
    posts_to_label: list[dict] = load_posts()
    labeled_posts: list[dict] = label_and_export_posts(
        posts_to_label=posts_to_label
    )
    return labeled_posts


def export_urls_of_posts(posts: list[dict], limit: int = None) -> None:
    """Exports the URLs of the posts to be labeled.

    We first want all of the ones that are from the weekly feed, as those will
    have more likes, and then backfill the rest with the hottest posts from
    the past 24 hours.
    """
    # TODO: should I also store these in MongoDB? So we can track which
    # posts have been labeled?
    df = pd.DataFrame(posts)
    df['source_feed'] = df['metadata'].apply(lambda x: x.get('source_feed', None))  # noqa
    sort_order = {'week': 0, 'today': 1}
    df['sort_key'] = df['source_feed'].map(sort_order)
    df.sort_values('sort_key', inplace=True)
    df_dicts = df.to_dict("records")
    posts_to_label_list: list[dict] = [
        {
            "linkid": idx + 1,
            "link": post["url"],
            "sociopolitical": "",
            "ideology": "",
            "partisan": "",
            # metadata is for me, but needs to be deleted.
            "metadata": post["metadata"],
            "source_feed": post["source_feed"]
        }
        for idx, post in enumerate(df_dicts)
    ]
    if limit:
        posts_to_label_list = posts_to_label_list[:limit]
    urls_df = pd.DataFrame(posts_to_label_list)
    urls_df.to_csv(urls_fp, index=False)
    print(f"Successfully exported {len(posts_to_label_list)} URLS to {urls_fp}")  # noqa


if __name__ == "__main__":
    posts_to_label: list[dict] = load_posts(source="local")
    export_urls_of_posts(posts=posts_to_label, limit=1000)
