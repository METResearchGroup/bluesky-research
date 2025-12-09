"""Filters data from firehose stream.

Based on https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/data_filter.py
"""  # noqa

import json
from typing import Optional

from atproto_client.models.app.bsky.graph.follow import Record as FollowRecord
from atproto_client.models.app.bsky.feed.like import Record as LikeRecord  # noqa

from lib.db.bluesky_models.raw import RawFollow, RawFollowRecord, RawLike, RawLikeRecord
from lib.db.bluesky_models.transformations import TransformedRecordWithAuthorModel  # noqa
from lib.log.logger import get_logger
from services.consolidate_post_records.helper import consolidate_firehose_post
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from services.sync.stream.context import SyncExportContext
from services.sync.stream.types import Operation, RecordType, FollowStatus
from transform.transform_raw_data import process_firehose_post


logger = get_logger(__name__)


def manage_like(like: dict, operation: Operation, context: SyncExportContext) -> None:
    """For a like that was created/deleted, insert the record in local cache.

    We'll write the record to S3 as part of our normal batch update.
    """
    study_user_manager = context.study_user_manager

    if operation == Operation.CREATE:
        raw_liked_record: LikeRecord = like["record"]
        raw_liked_record_model = RawLikeRecord(**raw_liked_record.dict())
        like_model = RawLike(
            **{
                "author": like["author"],
                "cid": like["cid"],
                "record": raw_liked_record_model.dict(),
                "uri": like["uri"],
            }
        )
        like_model_dict = like_model.dict()
        like_author_did = like_model.author
        uri_suffix = like_model.uri.split("/")[-1]
        # we save this using both the DID and like URI so that then we can
        # easily parse this and write to the correct place in S3. We'll
        # manage tracking the posts liked later, as long as we know who
        # created the like.
        filename = (
            f"like_author_did={like_author_did}_like_uri_suffix={uri_suffix}.json"
        )
    elif operation == Operation.DELETE:
        uri_suffix = like["uri"].split("/")[-1]
        filename = f"like_uri_suffix={uri_suffix}.json"
        like_model_dict = like

    # NOTE: here in case we want to revisit writing all data to S3. Right now
    # we're only writing in-network posts to S3.
    # folder_path = export_filepath_map[operation]["like"]
    # full_path = os.path.join(folder_path, filename)
    # write_data_to_json(like_model_dict, full_path)

    # we care about the likes they create, not the ones they delete.
    # plus, deleted likes only have the URI of the original like and not
    # author info. Pretty edge-case scenario where we'd need to track study
    # user's deleted likes.
    if operation == Operation.CREATE:
        handler_registry = context.handler_registry

        # Case 1: the user is the one who likes a post.
        is_study_user = study_user_manager.is_study_user(user_did=like_author_did)
        if is_study_user:
            logger.info(f"Exporting like data for user {like_author_did}")
            handler = handler_registry.get_handler(RecordType.LIKE.value)
            handler.write_record(
                record=like_model_dict,
                operation=operation,
                author_did=like_author_did,
                filename=filename,
            )

        # Case 2: the user is the one who created the post that was liked.
        # NOTE: this doesn't backfill with a user's past posts, so we only
        # have posts that were created after the user was added to the study
        # and the firehose was run.
        liked_post_is_study_user_post: Optional[str] = (
            study_user_manager.is_study_user_post(
                post_uri=raw_liked_record_model.subject.uri
            )
        )  # checks the author of a post that was liked and checks to see if it's a user in the study # noqa
        if liked_post_is_study_user_post:
            logger.info(
                f"Exporting like data for post {raw_liked_record_model.subject.uri}"
            )
            handler = handler_registry.get_handler(RecordType.LIKE_ON_USER_POST.value)
            handler.write_record(
                record=like_model_dict,
                operation=operation,
                author_did=liked_post_is_study_user_post,  # the author of the liked post, which should be a user in the study # noqa
                filename=filename,
            )


def manage_likes(likes: dict[str, list], context: SyncExportContext) -> dict:
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
        manage_like(like=like, operation=Operation.CREATE, context=context)
    for like in likes["deleted"]:
        manage_like(like=like, operation=Operation.DELETE, context=context)


def manage_follow(
    follow: dict,
    operation: Operation,
    context: SyncExportContext,
) -> None:  # noqa
    """For a follow that was created/deleted, insert the record in local cache.

    We'll write the record to S3 as part of our normal batch update.
    """
    study_user_manager = context.study_user_manager

    if operation == Operation.CREATE:
        raw_follow_record: FollowRecord = follow["record"]
        raw_follow_record_model = RawFollowRecord(**raw_follow_record.dict())
        follow_model = RawFollow(
            **{
                "uri": follow["uri"],
                "cid": follow["cid"],
                "record": raw_follow_record_model.dict(),
                "author": follow["author"],
                "follower_did": follow["author"],
                "followee_did": raw_follow_record_model.subject,
            }
        )
        follow_model_dict = follow_model.dict()
        follower_did = follow_model.follower_did
        followee_did = follow_model.followee_did
        filename = f"follower_did={follower_did}_followee_did={followee_did}.json"
    elif operation == Operation.DELETE:
        follow_uri_suffix = follow["uri"].split("/")[-1]
        filename = f"follow_uri_suffix={follow_uri_suffix}.json"
        follow_model_dict = follow

    # NOTE: here in case we want to revisit writing all data to S3. Right now
    # we're only writing in-network posts to S3.
    # folder_path = export_filepath_map[operation]["follow"]
    # full_path = os.path.join(folder_path, filename)
    # write_data_to_json(follow_model_dict, full_path)

    # we only care about the follows they create, not the ones they delete.
    # plus, deleted follows only have the URI of the original like and not
    # author info. Pretty edge-case scenario where we'd need to track study
    # user's deleted follows.
    if operation == Operation.CREATE:
        handler_registry = context.handler_registry

        user_is_follower = study_user_manager.is_study_user(user_did=follower_did)  # noqa
        user_is_followee = study_user_manager.is_study_user(user_did=followee_did)  # noqa
        if user_is_follower or user_is_followee:
            handler = handler_registry.get_handler(RecordType.FOLLOW.value)
            # someone can follow someone else in the study, in which case both
            # the follower and followee need to be registered.
            if user_is_follower:
                logger.info(
                    f"User {follower_did} followed a new account, {followee_did}."
                )  # noqa
                handler.write_record(
                    record=follow_model_dict,
                    operation=operation,
                    author_did=follower_did,
                    filename=filename,
                    follow_status=FollowStatus.FOLLOWER,
                )
            if user_is_followee:
                logger.info(
                    f"User {followee_did} was followed by a new account, {follower_did}."
                )  # noqa
                handler.write_record(
                    record=follow_model_dict,
                    operation=operation,
                    author_did=followee_did,
                    filename=filename,
                    follow_status=FollowStatus.FOLLOWEE,
                )
        else:
            logger.error("User is neither follower nor followee.")


def manage_follows(follows: dict[str, list], context: SyncExportContext) -> dict:
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
        manage_follow(follow=follow, operation=Operation.CREATE, context=context)
    for follow in follows["deleted"]:
        manage_follow(follow=follow, operation=Operation.DELETE, context=context)


def manage_post(post: dict, operation: Operation, context: SyncExportContext):
    """For a post that was created/deleted, insert the record in local cache.

    We'll write the record to S3 as part of our normal batch update.
    """
    study_user_manager = context.study_user_manager

    if operation == Operation.CREATE:
        firehose_post: TransformedRecordWithAuthorModel = process_firehose_post(post)  # noqa
        consolidated_post: ConsolidatedPostRecordModel = consolidate_firehose_post(
            firehose_post
        )  # noqa
        consolidated_post_dict = consolidated_post.dict()
        # JSON-dump the embed to avoid complex dtype problems in the future.
        consolidated_post_dict["embed"] = json.dumps(consolidated_post_dict["embed"])
        author_did = consolidated_post_dict["author_did"]
        # e.g., full URI = at://did:plc:iphiwbyfi2qhid2mbxmvl3st/app.bsky.feed.post/3kwd3wuubke2i # noqa
        # so we only want a small portion.
        # URI takes the form at://<author DID>/<collection>/<post URI>
        post_uri_suffix = consolidated_post_dict["uri"].split("/")[
            -1
        ]  # e.g., 3kwd3wuubke2i # noqa
        filename = f"author_did={author_did}_post_uri_suffix={post_uri_suffix}.json"  # noqa
    elif operation == Operation.DELETE:
        post_uri_suffix = post["uri"].split("/")[-1]
        filename = f"post_uri_suffix={post_uri_suffix}.json"
        consolidated_post_dict = post

    # NOTE: here in case we want to revisit writing all data to S3. Right now
    # we're only writing in-network posts to S3.

    # folder_path = export_filepath_map[operation]["post"]
    # full_path = os.path.join(folder_path, filename)
    # write_data_to_json(consolidated_post_dict, full_path)

    if operation == Operation.CREATE:
        handler_registry = context.handler_registry

        # Case 1: Check if the post was written by the study user.
        is_study_user = study_user_manager.is_study_user(user_did=author_did)
        if is_study_user:
            logger.info(
                f"Study user {author_did} created a new post: {post_uri_suffix}"
            )  # noqa
            handler = handler_registry.get_handler(RecordType.POST.value)
            handler.write_record(
                record=consolidated_post_dict,
                operation=operation,
                author_did=author_did,
                filename=filename,
            )
            # Update StudyUserManager for posts (maintains old behavior)
            study_user_manager.insert_study_user_post(
                post_uri=consolidated_post_dict["uri"], user_did=author_did
            )

        # Case 2: Check if the post is a repost of a post written by the study
        # user. TODO: come back to this. Unsure if this can be tracked from
        # the raw firehose object itself? I don't think it can be. We can track
        # if it is a repost if it's a feedviewpost though.

        # Case 3: post is a reply to a post written by the study user.
        if (
            consolidated_post_dict["reply_parent"]
            or consolidated_post_dict["reply_root"]
        ):
            reply_parent_is_user_study_post: Optional[str] = (
                study_user_manager.is_study_user_post(  # noqa
                    post_uri=consolidated_post_dict["reply_parent"]
                )
            )
            reply_root_is_user_study_post: Optional[str] = (
                study_user_manager.is_study_user_post(  # noqa
                    post_uri=consolidated_post_dict["reply_root"]
                )
            )
            post_is_reply_to_study_user_post = (
                reply_parent_is_user_study_post is not None
                or reply_root_is_user_study_post is not None
            )

            if post_is_reply_to_study_user_post:
                logger.info(
                    f"Post {post_uri_suffix} is a reply to a post by a study user."
                )
                handler = handler_registry.get_handler(
                    RecordType.REPLY_TO_USER_POST.value
                )
                handler.write_record(
                    record=consolidated_post_dict,
                    operation=operation,
                    # should return author DID of the post being replied to,
                    # since this is the post that is by the study user.
                    author_did=(
                        reply_parent_is_user_study_post
                        if reply_parent_is_user_study_post
                        else reply_root_is_user_study_post
                    ),
                    filename=filename,
                )

        # Case 4: post is written by an in-network user.
        is_in_network_user: bool = study_user_manager.is_in_network_user(
            user_did=author_did
        )  # noqa
        if is_in_network_user:
            logger.info(
                f"In-network user {author_did} created a new post: {post_uri_suffix}"
            )  # noqa
            # Use handler for in-network posts
            handler = handler_registry.get_handler("in_network_post")
            handler.write_record(
                record=consolidated_post_dict,
                operation=operation,
                author_did=author_did,
                filename=filename,
            )


def manage_posts(posts: dict[str, list], context: SyncExportContext) -> dict:
    """Manages which posts to create or delete.

    We want to track any new posts in our database.
    """
    for post in posts["created"]:
        manage_post(post=post, operation=Operation.CREATE, context=context)
    for post in posts["deleted"]:
        manage_post(post=post, operation=Operation.DELETE, context=context)


def manage_repost(repost: dict, operation: Operation) -> None:
    pass


def manage_reposts(reposts: dict[str, list]) -> dict:
    for repost in reposts["created"]:
        manage_repost(repost=repost, operation=Operation.CREATE)
    for repost in reposts["deleted"]:
        manage_repost(repost=repost, operation=Operation.DELETE)


def manage_list(list: dict, operation: Operation) -> None:
    pass


def manage_lists(lists: dict[str, list]) -> dict:
    for list in lists["created"]:
        manage_list(list=list, operation=Operation.CREATE)
    for list in lists["deleted"]:
        manage_list(list=list, operation=Operation.DELETE)


def manage_block(block: dict, operation: Operation) -> None:
    pass


def manage_blocks(blocks: dict[str, list]) -> dict:
    for block in blocks["created"]:
        manage_block(block=block, operation=Operation.CREATE)
    for block in blocks["deleted"]:
        manage_block(block=block, operation=Operation.DELETE)


def manage_profile(profile: dict, operation: Operation) -> None:
    pass


def manage_profiles(profiles: dict[str, list]) -> dict:
    for profile in profiles["created"]:
        manage_profile(profile=profile, operation=Operation.CREATE)
    for profile in profiles["deleted"]:
        manage_profile(profile=profile, operation=Operation.DELETE)


def operations_callback(operations_by_type: dict, context: SyncExportContext) -> bool:
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
        'follows': {'created': [], 'deleted': []},
        'lists': {'created': [], 'deleted': []},
        'blocks': {'created': [], 'deleted': []},
        'profiles': {'created': [], 'deleted': []},
    }
    """  # noqa
    try:
        manage_posts(posts=operations_by_type["posts"], context=context)
        manage_likes(likes=operations_by_type["likes"], context=context)
        manage_follows(follows=operations_by_type["follows"], context=context)
        manage_reposts(reposts=operations_by_type["reposts"])
        manage_lists(lists=operations_by_type["lists"])
        manage_blocks(blocks=operations_by_type["blocks"])
        manage_profiles(profiles=operations_by_type["profiles"])
        return True
    except Exception as e:
        logger.info(f"Error in exporting latest writes to cache: {e}")
        raise e
