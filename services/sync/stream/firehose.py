"""Firehose stream service.

Based on https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/data_stream.py
"""  # noqa
import sys
from typing import Optional

from atproto import (
    AtUri, CAR, firehose_models, FirehoseSubscribeReposClient, models,
    parse_subscribe_repos_message
)
from atproto.exceptions import FirehoseError

from lib.constants import current_datetime_str
from lib.db.bluesky_models.raw import FirehoseSubscriptionStateCursorModel
from lib.helper import ThreadSafeCounter
from services.sync.stream.export_data import (
    load_cursor_state_s3, update_cursor_state_s3, write_batch_to_s3
)

# number of events to stream before exiting
# (should be ~10,000 posts, a 1:5 ratio of posts:events)
# stream_limit = 50000
# 50,000 posts, took about 2 hours to stream (3pm-5pm Eastern Time)
# stream_limit = 250000
# 150,000 posts expected.
stream_limit = 750000

# how often to (1) write to S3 and (2) update the cursor state
cursor_update_frequency = 250


def _get_ops_by_type(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> dict:  # noqa
    operation_by_type = {
        'posts': {'created': [], 'deleted': []},
        # NOTE: is it possible to track reposts?
        'reposts': {'created': [], 'deleted': []},
        'likes': {'created': [], 'deleted': []},
        'follows': {'created': [], 'deleted': []},
    }

    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        uri = AtUri.from_str(f'at://{commit.repo}/{op.path}')

        if op.action == 'update':
            # not supported yet
            continue

        if op.action == 'create':
            if not op.cid:
                continue

            create_info = {'uri': str(uri), 'cid': str(
                op.cid), 'author': commit.repo}

            record_raw_data = car.blocks.get(op.cid)
            if not record_raw_data:
                continue

            record = models.get_or_create(record_raw_data, strict=False)
            if (
                uri.collection == models.ids.AppBskyFeedLike
                and models.is_record_type(record, models.AppBskyFeedLike)
            ):
                operation_by_type['likes']['created'].append(
                    {'record': record, **create_info})
            elif (
                uri.collection == models.ids.AppBskyFeedPost
                and models.is_record_type(record, models.AppBskyFeedPost)
            ):
                operation_by_type['posts']['created'].append(
                    {'record': record, **create_info})
            elif (
                uri.collection == models.ids.AppBskyGraphFollow
                and models.is_record_type(record, models.AppBskyGraphFollow)
            ):
                operation_by_type['follows']['created'].append(
                    {'record': record, **create_info})

        if op.action == 'delete':
            if uri.collection == models.ids.AppBskyFeedLike:
                operation_by_type['likes']['deleted'].append({'uri': str(uri)})
            if uri.collection == models.ids.AppBskyFeedPost:
                operation_by_type['posts']['deleted'].append({'uri': str(uri)})
            if uri.collection == models.ids.AppBskyGraphFollow:
                operation_by_type['follows']['deleted'].append(
                    {'uri': str(uri)})

    return operation_by_type


def run(name, operations_callback, stream_stop_event=None):
    while stream_stop_event is None or not stream_stop_event.is_set():
        try:
            _run(name, operations_callback, stream_stop_event)
        except FirehoseError as e:
            # here we can handle different errors to reconnect to firehose
            raise e


def _run(name, operations_callback, stream_stop_event=None):  # noqa: C901
    """Run firehose stream."""
    state: Optional[FirehoseSubscriptionStateCursorModel] = (
        load_cursor_state_s3(service_name=name)
    )

    if state:
        params = models.ComAtprotoSyncSubscribeRepos.Params(cursor=state.cursor)  # noqa
    else:
        params = None

    firehose_client = FirehoseSubscribeReposClient(params)

    counter = ThreadSafeCounter()

    def on_message_handler(message: firehose_models.MessageFrame) -> None:
        # stop on next message if requested
        if stream_stop_event and stream_stop_event.is_set():
            firehose_client.stop()
            return

        # possible types of messages: https://github.com/bluesky-social/atproto/blob/main/packages/api/src/client/lexicons.ts#L3545 # noqa
        if message.type == "#identity":
            return
        commit = parse_subscribe_repos_message(message)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return

        if not commit.blocks:
            return

        ops_by_type: dict = _get_ops_by_type(commit)

        has_written_data: dict = operations_callback(ops_by_type)

        # we assume that the write to DB has succeeded, though we may want to
        # validate this check (i.e., has_written_data is always True, but
        # we may want to see if this is actually the case.)
        if has_written_data:
            counter.increment()
            counter_value = counter.get_value()
            if counter_value % cursor_update_frequency == 0:
                print(f"Counter: {counter_value}")
                print("Writing cached records to S3 and resetting cache...")
                write_batch_to_s3()
                print(f"Updating cursor state with cursor={counter_value}...")  # noqa
                cursor_state = {
                    "service": name,
                    "cursor": commit.seq,
                    "timestamp": current_datetime_str
                }
                cursor_state_model = (
                    FirehoseSubscriptionStateCursorModel(**cursor_state)
                )
                update_cursor_state_s3(cursor_state_model)
            if counter.get_value() > stream_limit:
                sys.exit(0)

    firehose_client.start(on_message_handler)
