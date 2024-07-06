"""Filters data from firehose stream.

Based on https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/data_filter.py
"""  # noqa
import os
from typing import Literal

from atproto_client.models.app.bsky.graph.follow import Record as FollowRecord
from atproto_client.models.app.bsky.feed.like import Record as LikeRecord  # noqa

from lib.db.bluesky_models.raw import (
    RawFollow, RawFollowRecord, RawLike, RawLikeRecord
)
from lib.db.bluesky_models.transformations import TransformedRecordWithAuthorModel  # noqa
from services.sync.stream.export_data import (
    export_filepath_map, write_data_to_json
)
from services.consolidate_post_records.helper import consolidate_firehose_post
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from transform.transform_raw_data import process_firehose_post


def manage_like(like: dict, operation: Literal["create", "delete"]) -> None:
    """For a like that was created/deleted, insert the record in local cache.

    We'll write the record to S3 as part of our normal batch update.
    """
    if operation == "create":
        raw_liked_record: LikeRecord = like["record"]
        raw_liked_record_model = RawLikeRecord(**raw_liked_record.dict())
        like_model = RawLike(
            **{
                "author": like["author"],
                "cid": like["cid"],
                "record": raw_liked_record_model.dict(),
                "uri": like["uri"]
            }
        )
        like_model_dict = like_model.dict()
        author_did = like_model.author
        uri = like_model.uri.split('/')[-1]
        # we save this using both the DID and like URI so that then we can
        # easily parse this and write to the correct place in S3. We'll
        # manage tracking the posts liked later, as long as we know who
        # created the like.
        filename = f"author_did={author_did}_uri={uri}.json"
    elif operation == "delete":
        uri = like["uri"].split('/')[-1]
        filename = f"uri={uri}.json"
        like_model_dict = like

    folder_path = export_filepath_map[operation]["like"]
    full_path = os.path.join(folder_path, filename)
    write_data_to_json(like_model_dict, full_path)


def manage_likes(likes: dict[str, list]) -> dict:
    """Manages the likes and follows.

    We'll build this in later, but this is a placeholder function for when we
    do manage the likes. We only want to do this when it involves a user in our
    study (e.g., if a user likes a post)

    Each like is a dictionary of the format:
    {
        "created": [], "deleted": []
    }

    Example:
    {
        'created': [
            {
                'author': 'did:plc:aq45jcquopr4joswmfdpsfnh',
                'cid': 'bafyreihus4wvodsdmhsschvb57dn7qsl6wxanu5fv6httkq2njd7zqadri',
                'record': Record(
                    created_at='2024-07-02T14:05:23.807Z',
                    subject=Main(
                        cid='bafyreif2ijylrc3cativstjcrbbcvtaa3xtptx23kkiqimq5y6hk2amdiy',
                        uri='at://did:plc:ucfj5xnywoxbdaxqelvpzyqz/app.bsky.feed.post/3kvkbi7yfb22z',
                        py_type='com.atproto.repo.strongRef'
                    ),
                    py_type='app.bsky.feed.like'
                ),
                'uri': 'at://did:plc:aq45jcquopr4joswmfdpsfnh/app.bsky.feed.like/3kwckubmt342n'
            }
        ],
        'deleted': []
    }
    """  # noqa
    for like in likes["created"]:
        manage_like(like=like, operation="create")
    for like in likes["deleted"]:
        manage_like(like=like, operation="delete")


def manage_follow(follow: dict, operation: Literal["create", "delete"]) -> None:  # noqa
    """For a follow that was created/deleted, insert the record in local cache.

    We'll write the record to S3 as part of our normal batch update.
    """
    if operation == "create":
        raw_follow_record: FollowRecord = follow["record"]
        raw_follow_record_model = RawFollowRecord(**raw_follow_record.dict())
        follow_model = RawFollow(
            **{
                "uri": follow["uri"],
                "cid": follow["cid"],
                "record": raw_follow_record_model.dict(),
                "author": follow["author"],
                "follower_did": follow["author"],
                "follow_did": raw_follow_record_model.subject
            }
        )
        follow_model_dict = follow_model.dict()
        follower_did = follow_model.follower_did
        follow_did = follow_model.follow_did
        filename = f"follower_did={follower_did}_follow_did={follow_did}.json"
    elif operation == "delete":
        follow_uri = follow["uri"].split('/')[-1]
        filename = f"follow_uri={follow_uri}.json"
        follow_model_dict = follow

    folder_path = export_filepath_map[operation]["follow"]
    full_path = os.path.join(folder_path, filename)
    write_data_to_json(follow_model_dict, full_path)


def manage_follows(follows: dict[str, list]) -> dict:
    """Manages the follows.

    We'll build this in later, but this is a placeholder function for when we
    do manage the follows. We only want to do this when it involves a user in
    our study (e.g., if a user follows an account).

    Each follow is a dictionary of the format:
    {
        "created": [], "deleted": []
    }

    Example:

    {
        'created': [
            {
                'record': Record(
                    created_at='2024-07-02T17:48:48.627Z',
                    subject='did:plc:vjoaculzgxuqa3gdtqkmqawn',
                    py_type='app.bsky.graph.follow'
                ),
                'uri': 'at://did:plc:qqdx6sgha4cqqhxs564g43zq/app.bsky.graph.follow/3kwcxduaskd2p',
                'cid': 'bafyreibwn4kwlezxabt2bzpopwfh7lbo56n4xb62wlbm5moqliwl4pzum4',
                'author': 'did:plc:qqdx6sgha4cqqhxs564g43zq'
            }
        ],
        'deleted': []
    }

    The author is the entity who is following, and the record.subject is the
    user who is being followed. For example, if A follows B, then the author is
    the DID of A and the record.subject is the DID of B.
    """  # noqa
    for follow in follows["created"]:
        manage_follow(follow=follow, operation="create")
    for follow in follows["deleted"]:
        manage_follow(follow=follow, operation="delete")


def manage_post(post: dict, operation: Literal["create", "delete"]):
    """For a post that was created/deleted, insert the record in local cache.

    We'll write the record to S3 as part of our normal batch update.
    """
    if operation == "create":
        firehose_post: TransformedRecordWithAuthorModel = process_firehose_post(post)  # noqa
        consolidated_post: ConsolidatedPostRecordModel = consolidate_firehose_post(firehose_post)  # noqa
        consolidated_post_dict = consolidated_post.dict()
        author_did = consolidated_post_dict["author_did"]
        # e.g., full URI = at://did:plc:iphiwbyfi2qhid2mbxmvl3st/app.bsky.feed.post/3kwd3wuubke2i # noqa
        # so we only want a small portion.
        # URI takes the form at://<author DID>/<collection>/<post URI>
        post_uri = consolidated_post_dict["uri"].split('/')[-1]  # e.g., 3kwd3wuubke2i # noqa
        filename = f"author_did={author_did}_post_uri={post_uri}.json"
    elif operation == "delete":
        post_uri = post["uri"].split('/')[-1]
        filename = f"post_uri={post_uri}.json"
        consolidated_post_dict = post

    folder_path = export_filepath_map[operation]["post"]
    full_path = os.path.join(folder_path, filename)
    write_data_to_json(consolidated_post_dict, full_path)


def manage_posts(posts: dict[str, list]) -> dict:
    """Manages which posts to create or delete.

    We want to track any new posts in our database.
    """
    for post in posts["created"]:
        manage_post(post=post, operation="create")
    for post in posts["deleted"]:
        manage_post(post=post, operation="delete")


def operations_callback(operations_by_type: dict) -> bool:
    """Callback for managing posts during stream.

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
    """  # noqa
    try:
        manage_posts(posts=operations_by_type["posts"])
        manage_likes(likes=operations_by_type["likes"])
        manage_follows(follows=operations_by_type["follows"])
        return True
    except Exception as e:
        print(f"Error in exporting latest writes to cache: {e}")
        raise e
