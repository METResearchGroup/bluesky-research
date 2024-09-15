"""Delete old feeds.

Should see something like this:
HTTP Request: POST https://boletus.us-west.host.bsky.network/xrpc/com.atproto.repo.deleteRecord "HTTP/1.1 200 OK"
Response(commit=CommitMeta(cid='bafyreidxkt25o6wcjly7wcs3wu6mbmhkxkirvofhgca6ecqnbwampeakj4', rev='3l46jloawm626', py_type='com.atproto.repo.defs#commitMeta'))
"""

from atproto_client.models.com.atproto.repo.delete_record import Data

from lib.helper import get_client

old_feed_uris = [
    "at://did:plc:dupygefpurstnheocpdfi2qd/app.bsky.feed.generator/bsky-feed-2",
    "at://did:plc:dupygefpurstnheocpdfi2qd/app.bsky.feed.generator/bsky-feed-4",
    "at://did:plc:dupygefpurstnheocpdfi2qd/app.bsky.feed.generator/bsky-feed-test",
    "at://did:plc:dupygefpurstnheocpdfi2qd/app.bsky.feed.generator/nw-feed-algos",
    "at://did:plc:dupygefpurstnheocpdfi2qd/app.bsky.feed.generator/nwestern-bsky",
]

client = get_client()

data = {
    "collection": "app.bsky.feed.generator",
    "repo": "did:plc:dupygefpurstnheocpdfi2qd",
    "rkey": "bsky-feed-2",
}

for uri in old_feed_uris:
    data_obj = {
        "collection": "app.bsky.feed.generator",
        "repo": "did:plc:dupygefpurstnheocpdfi2qd",
        "rkey": uri.split("/")[-1],
    }
    data = Data(**data_obj)
    client.com.atproto.repo.delete_record(data=data)
