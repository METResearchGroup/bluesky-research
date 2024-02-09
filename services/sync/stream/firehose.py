"""Firehose stream service.

Based on https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/data_stream.py
"""
import time

from atproto import AtUri, CAR, firehose_models, FirehoseSubscribeReposClient, models, parse_subscribe_repos_message
from atproto.exceptions import FirehoseError

from lib.aws.s3 import S3, POST_BATCH_SIZE
from lib.helper import ThreadSafeCounter
from services.sync.stream.constants import tmp_data_dir
from services.sync.stream.database import SubscriptionState

s3_client = S3()


def _get_ops_by_type(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> dict:  # noqa: C901
    operation_by_type = {
        'posts': {'created': [], 'deleted': []},
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

            create_info = {'uri': str(uri), 'cid': str(op.cid), 'author': commit.repo}

            record_raw_data = car.blocks.get(op.cid)
            if not record_raw_data:
                continue

            record = models.get_or_create(record_raw_data, strict=False)
            if (uri.collection == models.ids.AppBskyFeedLike
                    and models.is_record_type(record, models.AppBskyFeedLike)):
                operation_by_type['likes']['created'].append({'record': record, **create_info})
            elif (uri.collection == models.ids.AppBskyFeedPost
                  and models.is_record_type(record, models.AppBskyFeedPost)):
                operation_by_type['posts']['created'].append({'record': record, **create_info})
            elif (uri.collection == models.ids.AppBskyGraphFollow
                  and models.is_record_type(record, models.AppBskyGraphFollow)):
                operation_by_type['follows']['created'].append({'record': record, **create_info})

        if op.action == 'delete':
            if uri.collection == models.ids.AppBskyFeedLike:
                operation_by_type['likes']['deleted'].append({'uri': str(uri)})
            if uri.collection == models.ids.AppBskyFeedPost:
                operation_by_type['posts']['deleted'].append({'uri': str(uri)})
            if uri.collection == models.ids.AppBskyGraphFollow:
                operation_by_type['follows']['deleted'].append({'uri': str(uri)})

    return operation_by_type


def run(name, operations_callback, stream_stop_event=None):
    while stream_stop_event is None or not stream_stop_event.is_set():
        try:
            _run(name, operations_callback, stream_stop_event)
        except FirehoseError as e:
            # here we can handle different errors to reconnect to firehose
            raise e


def _run(name, operations_callback, stream_stop_event=None):
    state = SubscriptionState.select(SubscriptionState.service == name).first()

    params = None
    if state:
        params = models.ComAtprotoSyncSubscribeRepos.Params(cursor=state.cursor)

    client = FirehoseSubscribeReposClient(params)

    if not state:
        SubscriptionState.create(service=name, cursor=0)

    counter = ThreadSafeCounter()

    def on_message_handler(message: firehose_models.MessageFrame) -> None:
        # stop on next message if requested
        if stream_stop_event and stream_stop_event.is_set():
            client.stop()
            return

        commit = parse_subscribe_repos_message(message)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return

        # every batch size, we collect all the posts written to local storage
        # and write to s3.
        current_counter_value = counter.get_value()
        if (
            current_counter_value > 0
            and current_counter_value % POST_BATCH_SIZE == 0
        ):
            print(f"Writing batch of posts to S3...")
            timestamp = str(int(time.time()))
            filename = f"posts_{timestamp}.jsonl"
            s3_client.write_local_jsons_to_s3(
                dir=tmp_data_dir, filename=filename
            )
            counter.reset()

        # update stored state every ~20 events
        if commit.seq % 20 == 0:
            print(f'Updated cursor for {name} to {commit.seq}')
            client.update_params(models.ComAtprotoSyncSubscribeRepos.Params(cursor=commit.seq))
            SubscriptionState.update(cursor=commit.seq).where(SubscriptionState.service == name).execute()

        if not commit.blocks:
            return

        has_written_data = operations_callback(_get_ops_by_type(commit))

        if has_written_data:
            # we only increment when there has been successful writes, since
            # not all commits from the firehose will lead to a corresponding
            # write of data to local storage.
            counter.increment()
            print(f"Counter: {counter.get_value()}")

    client.start(on_message_handler)
