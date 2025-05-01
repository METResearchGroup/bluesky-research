"""Experiments for transforming backfill records."""
import copy
from pprint import pprint
import traceback

from services.backfill.core.transform import transform_backfilled_record

example_post = {
    'cid': 'bafyreie4dsgulhqlzuamhqhvxcwdt5lstyqgb4wz5vjqebyhl6jo4jicr4',
    'uri': 'at://did:plc:nocdee65l47cxmmchz76ldzk/app.bsky.feed.post/3lbqs7wqdkk2j',
    'value': {
        '$type': 'app.bsky.feed.post',
        'createdAt': '2024-11-25T05:31:35.658Z',
        'langs': ['en'],
        'reply': {
            'parent': {
                'cid': 'bafyreibmapyc2vun5rbejgk7wo2d3xoo4kytpzbixo424jow2tybjvy3le',
                'uri': 'at://did:plc:vocg5apjrlm7dfptvy6qbrfy/app.bsky.feed.post/3lbl6lfctts2a'
            },
            'root': {
                'cid': 'bafyreibmapyc2vun5rbejgk7wo2d3xoo4kytpzbixo424jow2tybjvy3le',
                'uri': 'at://did:plc:vocg5apjrlm7dfptvy6qbrfy/app.bsky.feed.post/3lbl6lfctts2a'
            }
        },
        'text': 'Dis is fuvking crazy. 🤣🤣🤣😂🤣'
    }
}

example_repost = {
    'cid': 'bafyreifntsea7owcnze4pqfcmrutuiaawf3jpujmsa6pss2i4vhcu4ofju',
    'uri': 'at://did:plc:nocdee65l47cxmmchz76ldzk/app.bsky.feed.repost/3lbqs5yo4fj2k',
    'value': {
        '$type': 'app.bsky.feed.repost',
        'createdAt': '2024-11-25T05:30:29.738Z',
        'subject': {
            'cid': 'bafyreibmapyc2vun5rbejgk7wo2d3xoo4kytpzbixo424jow2tybjvy3le',
            'uri': 'at://did:plc:vocg5apjrlm7dfptvy6qbrfy/app.bsky.feed.post/3lbl6lfctts2a'
        }
    }
}

example_like = {
    'cid': 'bafyreiaiji745x37w3olrt4c7brzwyl7lr7rovpg5i5fuxuagznlti6smi',
    'uri': 'at://did:plc:nocdee65l47cxmmchz76ldzk/app.bsky.feed.like/3lbqsadqggu2b',
    'value': {
        '$type': 'app.bsky.feed.like',
        'createdAt': '2024-11-25T05:31:48.776Z',
        'subject': {
            'cid': 'bafyreibmapyc2vun5rbejgk7wo2d3xoo4kytpzbixo424jow2tybjvy3le',
            'uri': 'at://did:plc:vocg5apjrlm7dfptvy6qbrfy/app.bsky.feed.post/3lbl6lfctts2a'
        }
    }
}

example_follow = {
    'cid': 'bafyreidrxn5j4rpsqr6ujgnxe4eif57fqxxljplmxdn7rik4663ejexuwu',
    'uri': 'at://did:plc:nocdee65l47cxmmchz76ldzk/app.bsky.graph.follow/3l76rnoza372z',
    'value': {
        '$type': 'app.bsky.graph.follow',
        'createdAt': '2024-10-23T14:43:01.588Z',
        'subject': 'did:plc:5xhf4b3jipzvehrdsz46pjs3'
    }
}

example_block = {
    'cid': 'bafyreiddjubvz2mr652k32auampwj77xqckque7gj2imwnmcmtx5v6eyye',
    'uri': 'at://did:plc:6twttdrifmr53i5jhah7qnkr/app.bsky.graph.block/3k3jhzpnrho2p',
    'value': {
        '$type': 'app.bsky.graph.block',
        'createdAt': '2023-07-27T17:43:08.307Z',
        'subject': 'did:plc:czweyt74iloviohize3mazwr'
    }
}

# default params
did = "test_did"
start_timestamp = '2022-01-01'
end_timestamp = '2025-12-31'

def transform_post():
    record = copy.deepcopy(example_post)
    record["value"].pop("reply")
    record_type = 'post'
    transformed_record = transform_backfilled_record(
        did=did,
        record=record,
        record_type=record_type,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    return transformed_record

def transform_reply():
    record = example_post
    record_type = 'reply'
    transformed_record = transform_backfilled_record(
        did=did,
        record=record,
        record_type=record_type,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    return transformed_record

def transform_repost():
    record = example_repost
    record_type = 'repost'
    transformed_record = transform_backfilled_record(
        did=did,
        record=record,
        record_type=record_type,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    return transformed_record

def transform_like():
    record = example_like
    record_type = 'like'
    transformed_record = transform_backfilled_record(
        did=did,
        record=record,
        record_type=record_type,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    return transformed_record

def transform_follow():
    record = example_follow
    record_type = 'follow'
    transformed_record = transform_backfilled_record(
        did=did,
        record=record,
        record_type=record_type,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    return transformed_record

def transform_block():
    record = example_block  
    record_type = 'block'
    transformed_record = transform_backfilled_record(
        did=did,
        record=record,
        record_type=record_type,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    return transformed_record

if __name__ == '__main__':
    try:
        print("-------- POST --------")
        pprint(transform_post())
        print("-------- REPLY --------")
        pprint(transform_reply())
        print("-------- REPOST --------")
        pprint(transform_repost())
        print("-------- LIKE --------")
        pprint(transform_like())
        print("-------- FOLLOW --------")
        pprint(transform_follow())
        print("-------- BLOCK --------")
        pprint(transform_block())
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        breakpoint()
