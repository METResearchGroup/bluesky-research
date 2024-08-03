"""Mock firehose data, useful for unit testing."""
from atproto_client.models.app.bsky.graph.follow import Record as FollowRecord
from atproto_client.models.app.bsky.feed.like import Record as LikeRecord
from atproto_client.models.app.bsky.feed.post import Main as Record, ReplyRef
from atproto_client.models.com.atproto.repo.strong_ref import Main as StrongRef


mock_user_dids = set([
    "did:plc:study-user-1",
    "did:plc:study-user-2",
    "did:plc:study-user-3",
])
mock_post_uri_to_user_did_map = {
    "at://did:plc:did-1/app.bsky.feed.post/post-uri-1": "did:plc:study-user-1",
    "at://did:plc:did-2/app.bsky.feed.post/post-uri-2": "did:plc:study-user-2",
    "at://did:plc:did-3/app.bsky.feed.post/post-uri-3": "did:plc:study-user-3",
}

mock_follow_records = [
    {
        'record': FollowRecord(
            created_at='2024-07-02T17:48:48.627Z',
            subject='did:plc:generic-user-1',
            py_type='app.bsky.graph.follow'
        ),
        'uri': 'at://did:plc:follow-record/app.bsky.graph.follow/random-hash',
        'cid': 'bafyreibwn4kwlezxabt2bzpopwfh7lbo56n4xb62wlbm5moqliwl4pzum4',
        'author': 'did:plc:generic-user-2'
    },  # generic follow record
    {
        'uri': 'at://did:plc:follow-record/app.bsky.graph.follow/random-hash'
    },  # generic deleted record
    {
        'record': FollowRecord(
            created_at='2024-07-02T17:48:48.627Z',
            subject='did:plc:study-user-1',
            py_type='app.bsky.graph.follow'
        ),
        'uri': 'at://did:plc:follow-record/app.bsky.graph.follow/random-hash',
        'cid': 'bafyreibwn4kwlezxabt2bzpopwfh7lbo56n4xb62wlbm5moqliwl4pzum4',
        'author': 'did:plc:generic-user-1'
    },  # someone follows a study user (user is a followee)
    {
        'record': FollowRecord(
            created_at='2024-07-02T17:48:48.627Z',
            subject='did:plc:generic-user-1',
            py_type='app.bsky.graph.follow'
        ),
        'uri': 'at://did:plc:follow-record/app.bsky.graph.follow/random-hash',
        'cid': 'bafyreibwn4kwlezxabt2bzpopwfh7lbo56n4xb62wlbm5moqliwl4pzum4',
        'author': 'did:plc:study-user-1'
    },  # study user follows someone (user is a follower)
]

mock_like_records = [
    {
        'author': 'did:plc:generic-user-1',
        'cid': 'bafyreihus4wvodsdmhsschvb57dn7qsl6wxanu5fv6httkq2njd7zqadri',
        'record': LikeRecord(
            created_at='2024-07-02T14:05:23.807Z',
            subject=StrongRef(
                cid='bafyreif2ijylrc3cativstjcrbbcvtaa3xtptx23kkiqimq5y6hk2amdiy',
                uri='at://did:plc:ucfj5xnywoxbdaxqelvpzyqz/app.bsky.feed.post/3kvkbi7yfb22z',
                py_type='com.atproto.repo.strongRef'
            ),
            py_type='app.bsky.feed.like'
        ),
        'uri': 'at://did:plc:like-record-1/app.bsky.feed.like/like-record-suffix-123'  # noqa
    },  # generic like record
    {
        'uri': 'at://did:plc:like-record-1/app.bsky.feed.like/like-record-suffix-123'
    },  # generic deleted like record
    {
        'author': 'did:plc:study-user-1',
        'cid': 'bafyreihus4wvodsdmhsschvb57dn7qsl6wxanu5fv6httkq2njd7zqadri',
        'record': LikeRecord(
            created_at='2024-07-02T14:05:23.807Z',
            subject=StrongRef(
                cid='bafyreif2ijylrc3cativstjcrbbcvtaa3xtptx23kkiqimq5y6hk2amdiy',
                uri="at://did:plc:did-1/app.bsky.feed.post/generic-post-uri-1",
                py_type='com.atproto.repo.strongRef'
            ),
            py_type='app.bsky.feed.like'
        ),
        'uri': 'at://did:plc:like-record-2/app.bsky.feed.like/like-record-suffix-456'  # noqa
    },  # a study user likes a post
    {
        'author': 'did:plc:generic-user-1',
        'cid': 'bafyreihus4wvodsdmhsschvb57dn7qsl6wxanu5fv6httkq2njd7zqadri',
        'record': LikeRecord(
            created_at='2024-07-02T14:05:23.807Z',
            subject=StrongRef(
                cid='bafyreif2ijylrc3cativstjcrbbcvtaa3xtptx23kkiqimq5y6hk2amdiy',
                uri="at://did:plc:did-2/app.bsky.feed.post/post-uri-2",
                py_type='com.atproto.repo.strongRef'
            ),
            py_type='app.bsky.feed.like'
        ),
        'uri': 'at://did:plc:aq45jcquopr4joswmfdpsfnh/app.bsky.feed.like/like-record-suffix-789'
    }  # a study user's post is liked.
]

mock_post_records = [
    {
        'record': Record(
            created_at='2024-02-07T05:10:02.159Z',
            text='<Test post>',
            embed=None,
            entities=None,
            facets=None,
            labels=None,
            langs=['en'],
            reply=None,
            tags=None,
            py_type='app.bsky.feed.post'
        ),
        'uri': 'at://did:plc:sjeosezgc7mpqn6sfc7neabg/app.bsky.feed.post/post-uri-1',
        'cid': 'bafyreidmb5wsupl6iz5wo2xjgusjpsrduug6qkpytjjckupdttot6jrbna',
        'author': 'did:plc:generic-user-1'
    },  # generic post record
    {
        'uri': 'at://did:plc:sjeosezgc7mpqn6sfc7neabg/app.bsky.feed.post/post-uri-1'
    },  # generic deleted post record
    {
        'record': Record(
            created_at='2024-02-07T05:10:02.159Z',
            text='<Test post>',
            embed=None,
            entities=None,
            facets=None,
            labels=None,
            langs=['en'],
            reply=None,
            tags=None,
            py_type='app.bsky.feed.post'
        ),
        "uri": "at://did:plc:did-1/app.bsky.feed.post/post-uri-1",
        'cid': 'bafyreidmb5wsupl6iz5wo2xjgusjpsrduug6qkpytjjckupdttot6jrbna',
        'author': "did:plc:study-user-1"
    },  # a post from a study user
    {
        'record': Record(
            created_at='2024-02-07T05:10:02.159Z',
            text='<Test post>',
            embed=None,
            entities=None,
            facets=None,
            labels=None,
            langs=['en'],
            reply=ReplyRef(
                parent=StrongRef(
                    cid='bafyreif2ijylrc3cativstjcrbbcvtaa3xtptx23kkiqimq5y6hk2amdiy',
                    uri="at://did:plc:did-1/app.bsky.feed.post/post-uri-1",  # by author="did:plc:study-user-1"
                    py_type='com.atproto.repo.strongRef'
                ),
                root=StrongRef(
                    cid='bafyreif2ijylrc3cativstjcrbbcvtaa3xtptx23kkiqimq5y6hk2amdiy',
                    uri="at://did:plc:random-did/app.bsky.feed.post/some-random-post-uri",
                    py_type='com.atproto.repo.strongRef'
                )
            ),
            tags=None,
            py_type='app.bsky.feed.post'
        ),
        'uri': 'at://did:plc:sjeosezgc7mpqn6sfc7neabg/app.bsky.feed.post/generic-post-uri-1',
        'cid': 'bafyreidmb5wsupl6iz5wo2xjgusjpsrduug6qkpytjjckupdttot6jrbna',
        'author': 'did:plc:generic-user-1'
    },  # a post in the same thread as a study user post (reply_parent) - replying to the user post # noqa
    {
        'record': Record(
            created_at='2024-02-07T05:10:02.159Z',
            text='<Test post>',
            embed=None,
            entities=None,
            facets=None,
            labels=None,
            langs=['en'],
            reply=ReplyRef(
                parent=StrongRef(
                    cid='bafyreif2ijylrc3cativstjcrbbcvtaa3xtptx23kkiqimq5y6hk2amdiy',
                    uri="at://did:plc:random-did/app.bsky.feed.post/some-random-post-uri",
                    py_type='com.atproto.repo.strongRef'
                ),
                root=StrongRef(
                    cid='bafyreif2ijylrc3cativstjcrbbcvtaa3xtptx23kkiqimq5y6hk2amdiy',
                    uri="at://did:plc:did-1/app.bsky.feed.post/post-uri-1",  # by author="did:plc:study-user-1"
                    py_type='com.atproto.repo.strongRef'
                )
            ),
            tags=None,
            py_type='app.bsky.feed.post'
        ),
        'uri': 'at://did:plc:sjeosezgc7mpqn6sfc7neabg/app.bsky.feed.post/generic-post-uri-1',
        'cid': 'bafyreidmb5wsupl6iz5wo2xjgusjpsrduug6qkpytjjckupdttot6jrbna',
        'author': 'did:plc:generic-user-1'
    },  # a post in the same thread as a study user post (reply_root) - replying to the user post # noqa
]
