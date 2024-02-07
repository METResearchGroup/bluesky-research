"""Filters data from firehose stream.

Based on https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/data_filter.py
"""  # noqa
from datetime import datetime, timedelta, timezone

from services.sync.stream.database import db, Post
from services.transform.transform_raw_data import flatten_firehose_post


BLOCKED_AUTHORS = [] # likely that we will want to filter out some authors (e.g., spam accounts)
NUM_DAYS_POST_RECENCY = 3 # we only want recent posts (Bluesky docs recommend 3 days, see )
current_datetime = current_datetime = datetime.now(timezone.utc)


def manage_post_creation(posts_to_create: list[dict]) -> None:
    """Manage post insertion into DB."""
    with db.atomic():
        for post_dict in posts_to_create:
            Post.create(**post_dict)
    print(f'Added to feed: {len(posts_to_create)}')


def manage_post_deletes(posts: list[dict]) -> None:
    """Manage post deletion from DB."""
    posts_to_delete = [p['uri'] for p in posts['posts']['deleted']]
    Post.delete().where(Post.uri.in_(posts_to_delete))
    print(f'Deleted from feed: {len(posts_to_delete)}')


def filter_created_post(post: dict) -> dict:
    """Filters any post that we could write to our DB. Also flattens our
    dictionary."""
    record = post['record']
    # get only posts that are English-language
    if "en" not in record.langs:
        return

    # filter out posts from blocked authors
    if record.author in BLOCKED_AUTHORS:
        return

    # filter out posts that don't pass our recency filter:
    date_object = datetime.strptime(
        record["created_at"],
        "%Y-%m-%dT%H:%M:%S.%fZ"
    ).replace(tzinfo=timezone.utc)
    time_difference = current_datetime - date_object
    time_threshold = timedelta(days=NUM_DAYS_POST_RECENCY)
    if time_difference > time_threshold:
        return

    # flatten post
    post_dict = flatten_firehose_post(post)

    return post_dict


def enrich_post(post: dict) -> dict:
    """Enriches post with additional metadata."""
    return post


def filter_incoming_posts(operations_by_type: dict) -> list[dict]:
    """Performs filtering on incoming posts and determines which posts have
    to be created or deleted.
    
    Returns a dictionary of the format:
    {
        "posts_to_create": list[dict],
        "posts_to_delete": list[dict]
    }
    """
    posts_to_create = []
    posts_to_delete = [p['uri'] for p in operations_by_type['posts']['deleted']] # noqa

    for created_post in operations_by_type['posts']['created']:
        filtered_post = filter_created_post(created_post)
        if filtered_post:
            enriched_post = enrich_post(filtered_post)
            post_text = enriched_post["record"]["text"]
            print(f"Writing post  with text: {post_text}")
            posts_to_create.append(enriched_post)

    return {
        "posts_to_create": posts_to_create,
        "posts_to_delete": posts_to_delete
    }


def operations_callback(operations_by_type: dict) -> None:
    """Callback for managing posts during stream. We perform the first pass
    of filtering here.

    This function takes as input a dictionary of the format
    {
        'posts': {'created': [], 'deleted': []},
        'reposts': {'created': [], 'deleted': []},
        'likes': {'created': [], 'deleted': []},
        'follows': {'created': [], 'deleted': []},
    }

    which tells us what operations should be added to our database.

    We also manage the logic for saving the posts to our DB as well as deleting
    any posts from our DB that no longer exist.

    Example object:
    {
        'posts': {
            'created': [
                {
                    'record': Main(
                        created_at='2024-02-07T05:10:02.159Z',
                        text='こんなポストするとBANされそうで怖いです',
                        embed=Main(
                            record=Main(
                                cid='bafyreidy6bxkwxbjvw6mqfxivp7rjywk3gpnzvbg2vaks2qhljzs6manyq',
                                uri='at://did:plc:sjeosezgc7mpqn6sfc7neabg/app.bsky.feed.post/3kksirfddwa2z',
                                py_type='com.atproto.repo.strongRef'
                            ),
                            py_type='app.bsky.embed.record'
                        ),
                        entities=None,
                        facets=None,
                        labels=None,
                        langs=['ja'],
                        reply=None,
                        tags=None,
                        py_type='app.bsky.feed.post'
                    ),
                    'uri': 'at://did:plc:sjeosezgc7mpqn6sfc7neabg/app.bsky.feed.post/3kksiuknorv2u',
                    'cid': 'bafyreidmb5wsupl6iz5wo2xjgusjpsrduug6qkpytjjckupdttot6jrbna',
                    'author': 'did:plc:sjeosezgc7mpqn6sfc7neabg'
                }
            ],
            'deleted': []
        },
        'reposts': {'created': [],'deleted': []},
        'likes': {'created': [], 'deleted': []},
        'follows': {'created': [], 'deleted': []}
    }
    """
    post_updates = filter_incoming_posts(operations_by_type)
    manage_post_deletes(post_updates["posts_to_delete"])
    manage_post_creation(post_updates["posts_to_update"])
