import pandas as pd

from services.preprocess_raw_data.classify_nsfw_content.constants import (
    LABELS_TO_FILTER,
)
from services.preprocess_raw_data.classify_nsfw_content.helper import (
    bsky_handles_to_exclude, bsky_dids_to_exclude,
    filter_post_content_nsfw, filter_post_author_nsfw
)


posts_with_nsfw_texts = pd.Series([label for label in LABELS_TO_FILTER])
posts_with_nsfw_labels = pd.Series([label for label in LABELS_TO_FILTER])
posts_with_nsfw_author_dids = pd.Series([did for did in bsky_dids_to_exclude])
posts_with_nsfw_author_handles = pd.Series([handle for handle in bsky_handles_to_exclude])

posts_with_no_nsfw_texts = pd.Series(["This is a test post", "Hello, world!"])
posts_with_no_nsfw_labels = pd.Series(["This is a test post", "Hello, world!"])
posts_with_no_nsfw_author_dids = pd.Series(["author1", "author2", "author3"])
posts_with_no_nsfw_author_handles = pd.Series(["author1", "author2", "author3"])

posts_with_nsfw_content_and_authors = pd.concat(
    [posts_with_nsfw_texts, posts_with_nsfw_labels, posts_with_nsfw_author_dids, posts_with_nsfw_author_handles]
)
posts_with_no_nsfw_content_and_authors = pd.concat(
    [posts_with_no_nsfw_texts, posts_with_no_nsfw_labels, posts_with_no_nsfw_author_dids, posts_with_no_nsfw_author_handles]
)

post_texts = pd.concat([posts_with_nsfw_texts, posts_with_no_nsfw_texts])
post_labels = pd.concat([posts_with_nsfw_labels, posts_with_no_nsfw_labels])
author_dids = pd.concat([posts_with_nsfw_author_dids, posts_with_no_nsfw_author_dids])
author_handles = pd.concat([posts_with_nsfw_author_handles, posts_with_no_nsfw_author_handles])


def test_nsfw_posts_filtered():
    filtered_posts = filter_post_content_nsfw(
        texts=posts_with_nsfw_texts,
        labels=posts_with_nsfw_labels
    )
    assert sum(filtered_posts) == len(posts_with_nsfw_texts)

def test_nsfw_posts_not_filtered():
    filtered_posts = filter_post_content_nsfw(
        texts=posts_with_no_nsfw_texts,
        labels=posts_with_no_nsfw_labels
    )
    assert sum(filtered_posts) == 0

def test_nsfw_authors_filtered():
    filtered_posts = filter_post_author_nsfw(
        author_dids=posts_with_nsfw_author_dids,
        author_handles=posts_with_nsfw_author_handles
    )
    assert sum(filtered_posts) == len(posts_with_nsfw_author_dids)

def test_nsfw_authors_not_filtered():
    filtered_posts = filter_post_author_nsfw(
        author_dids=posts_with_no_nsfw_author_dids,
        author_handles=posts_with_no_nsfw_author_handles
    )
    assert sum(filtered_posts) == 0


def test_posts_for_nsfw_content():
    filtered_posts = filter_post_content_nsfw(
        texts=post_texts,
        labels=post_labels
    )
    assert sum(filtered_posts) == len(posts_with_nsfw_texts)


def test_posts_for_nsfw_authors():
    filtered_posts = filter_post_author_nsfw(
        author_dids=author_dids,
        author_handles=author_handles
    )
    assert sum(filtered_posts) == len(posts_with_nsfw_author_dids)
