"""Helper code for testing with the Perspective API."""
import json
import os

from demos.perspective_api_testing.models import PerspectiveAPIClassification
from lib.constants import current_datetime_str
from lib.db.bluesky_models.transformations import (
    FeedViewPostMetadata,
    TransformedFeedViewPostModel,
    TransformedProfileViewBasicModel,
    TransformedRecordModel
)
from lib.db.mongodb import (
    chunk_insert_posts, get_mongodb_collection, load_collection
)
from ml_tooling.perspective_api.model import classify
from transform.transform_raw_data import (
    process_langs, process_tags
)

source_task_name = "get_most_liked_posts"
label_task_name = "perspective_api_labels"
_, source_mongodb_collection = get_mongodb_collection(source_task_name)
label_collection_name, label_mongodb_collection = (
    get_mongodb_collection(label_task_name)
)
# for posts that were synced without the pydantic models
old_format_placeholder = "<OLD_FORMAT>"


perspective_api_testing_dir = os.path.dirname(os.path.abspath(__file__))
full_label_dir = os.path.join(perspective_api_testing_dir, "labels")
full_label_fp = os.path.join(full_label_dir, f"perspective_api_labels_{current_datetime_str}.jsonl")  # noqa
DEFAULT_INSERT_CHUNK_SIZE = 100


def transform_feedviewpost_dict(post: dict) -> TransformedFeedViewPostModel:
    """Transforms a feed view post dictionary into a TransformedFeedViewPostModel.

    The transformation code from before assumes that the post is in the
    "Record" format that Bluesky has, and not a dictionary, so we'll have
    to work around those limitations.
    """
    metadata_dict: dict = post["metadata"]
    metadata_dict["url"] = post["url"]
    metadata: FeedViewPostMetadata = FeedViewPostMetadata(**metadata_dict)

    author_dict: dict = post["post"]["author"]
    author_dict_transformed = {
        "did": author_dict["did"],
        "handle": author_dict["handle"],
        "avatar": author_dict["avatar"],
        "display_name": author_dict["display_name"],
        "py_type": author_dict["py_type"]
    }
    author: TransformedProfileViewBasicModel = TransformedProfileViewBasicModel(
        **author_dict_transformed
    )

    # some fields can only be accessed when we have the old Record object.
    # instead of trying to access them or fundamentally changing our
    # transformation code to support a one-off case, we'll just use a
    # placeholder, especially since we can do analysis without these fields.
    record: dict = post["post"]["record"]
    record_dict = {
        "created_at": record["created_at"],
        "text": record["text"],
        "embed": None,
        "entities": old_format_placeholder,
        "facets": old_format_placeholder,
        "labels": old_format_placeholder,
        "langs": process_langs(record["langs"]),
        "reply_parent": old_format_placeholder,
        "reply_root": old_format_placeholder,
        "tags": process_tags(record["tags"]),
        "py_type": record["py_type"]
    }
    record: TransformedRecordModel = TransformedRecordModel(**record_dict)

    feedviewpost_dict = {
        "metadata": metadata,
        "author": author,
        "cid": post["post"]["cid"],
        "indexed_at": post["post"]["indexed_at"],
        "record": record,
        "uri": post["post"]["uri"],
        "like_count": post["post"]["like_count"],
        "reply_count": post["post"]["reply_count"],
        "repost_count": post["post"]["repost_count"]
    }
    feedviewpost: TransformedFeedViewPostModel = TransformedFeedViewPostModel(
        **feedviewpost_dict
    )
    return feedviewpost


def load_posts() -> list[TransformedFeedViewPostModel]:
    """Loads posts from the MongoDB collection."""
    posts: list[dict] = load_collection(
        collection=source_mongodb_collection, limit=None
    )
    transformed_posts = []
    # some posts won't have the proper format, but let's transform those
    # into the new format.
    for post in posts:
        try:
            transformed_posts.append(
                TransformedFeedViewPostModel(**post)
            )
        except Exception:
            transformed_feedviewpost = transform_feedviewpost_dict(post)
            transformed_posts.append(transformed_feedviewpost)
    print(f"Loaded {len(transformed_posts)} posts from MongoDB.")
    return transformed_posts


def classify_post(
    post: TransformedFeedViewPostModel
) -> PerspectiveAPIClassification:
    """Classifies post with the Perspective API.

    We'll set the text to just be the text of the post itself, even if it
    doesn't have much text. We can do filtering as part of postprocessing.
    """
    uri = post.uri
    text = post.record.text
    classifications: dict = classify(text=text)
    return PerspectiveAPIClassification(
        uri=uri,
        text=text,
        classifications=classifications
    )


def classify_posts(posts: list[dict]) -> list[PerspectiveAPIClassification]:
    classifications = [
        classify_post(post) for post in posts
    ]
    return classifications


def load_and_classify_posts() -> list[PerspectiveAPIClassification]:
    posts: list[TransformedFeedViewPostModel] = load_posts()
    classifications: list[PerspectiveAPIClassification] = classify_posts(posts)
    return classifications


def export_posts_local(posts: list[dict]):
    print(f"Writing {len(posts)} posts to {full_label_fp}")
    with open(full_label_fp, "w") as f:
        for post in posts:
            post_json = json.dumps(post)
            f.write(post_json + "\n")
    num_posts = len(posts)
    print(f"Wrote {num_posts} posts locally to {full_label_fp}")
    pass


def export_posts_remote(posts: list[dict]):
    duplicate_key_count = 0
    total_successful_inserts = 0
    total_posts = len(posts)
    print(f"Inserting {total_posts} posts to MongoDB collection {label_collection_name}")  # noqa
    formatted_posts_mongodb = [
        {"_id": post["uri"], **post}
        for post in posts
    ]
    print("Inserting into MongoDB in bulk...")
    total_successful_inserts, duplicate_key_count = chunk_insert_posts(
        posts=formatted_posts_mongodb,
        mongo_collection=label_mongodb_collection,
        chunk_size=DEFAULT_INSERT_CHUNK_SIZE
    )
    if duplicate_key_count > 0:
        print(f"Skipped {duplicate_key_count} duplicate posts")
    print(f"Inserted {total_successful_inserts} posts to remote MongoDB collection {label_collection_name}")  # noqa
    print("Finished bulk inserting into MongoDB.")


def export_classified_posts(
    classified_posts: list[PerspectiveAPIClassification],
    store_local: bool = True,
    store_remote: bool = True
):
    """Exports the classified posts."""
    posts: list[dict] = [post.dict() for post in classified_posts]
    if store_local:
        export_posts_local(posts)
    if store_remote:
        export_posts_remote(posts)


def main():
    classified_posts: list[PerspectiveAPIClassification] = load_and_classify_posts()  # noqa
    kwargs = {
        "classified_posts": classified_posts,
        "store_local": True,
        "store_remote": True
    }
    export_classified_posts(**kwargs)


if __name__ == "__main__":
    main()
