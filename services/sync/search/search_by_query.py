"""Search by keyword."""

from atproto_client.models.app.bsky.feed.defs import PostView

from lib.helper import get_client
from services.sync.search.helper import (
    DEFAULT_LIMIT_RESULTS_PER_REQUEST,
    send_request_with_pagination,
)
from transform.transform_raw_data import flatten_post

client = get_client()


def search_by_query(
    query: str, limit: int = DEFAULT_LIMIT_RESULTS_PER_REQUEST
) -> list[dict]:
    """Search by query. Uses the same endpoint as the search bar on the app.

    Corresponding lexicon:
    - https://github.com/MarshalX/atproto/blob/main/lexicons/app.bsky.feed.searchPosts.json
    - https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/feed/search_posts.py

    Example post (out of a list of posts) returned from query (before flattening):
    {
        'author': ProfileViewBasic(did='did:plc:7punls2ofrpj3x4tikb5n7ho', handle='lfb.bsky.social', avatar='https://cdn.bsky.app/img/avatar/plain/did:plc:7punls2ofrpj3x4tikb5n7ho/bafkreieyz7vk7o524asr4zgl2fsnpiezyjmntl6fyc3cpnageorsx7rnli@jpeg', display_name='Kuřák na paprice 🌶️', labels=[Label(cts='1970-01-01T00:00:00.000Z', src='did:plc:7punls2ofrpj3x4tikb5n7ho', uri='at://did:plc:7punls2ofrpj3x4tikb5n7ho/app.bsky.actor.profile/self', val='!no-unauthenticated', cid='bafyreichedyddadjykey3ilwmrhtrtsr2roelvbw6mexxjpuqcbjdurnie', neg=False, py_type='com.atproto.label.defs#label')], viewer=ViewerState(blocked_by=False, blocking=None, blocking_by_list=None, followed_by=None, following=None, muted=False, muted_by_list=None, py_type='app.bsky.actor.defs#viewerState'), py_type='app.bsky.actor.defs#profileViewBasic'),
        'cid': 'bafyreiahnq3wcuzbyeorboua3duuxbvd3saxiyuqdwu23cpebb6e3bf3km',
        'indexed_at': '2024-02-08T10:10:09.453Z',
        'record': Main(created_at='2024-02-08T10:10:09.453Z', text="Oh no! How dare you, pronoucing your opinion about politics which might be influencing your very life. In a public space? Willy-nilly? Not on that guy's watch.", embed=Main(images=[Image(alt='', image=BlobRef(mime_type='image/jpeg', size=49277, ref=IpldLink(link='bafkreihxwhsyvjvfi5ttp73ctrzqxkpnhk4kvgiqaxxoqbuerdm45p4dn4'), py_type='blob'), aspect_ratio=AspectRatio(height=197, width=256, py_type='app.bsky.embed.images#aspectRatio'), py_type='app.bsky.embed.images#image')], py_type='app.bsky.embed.images'), entities=None, facets=None, labels=None, langs=['cs'], reply=ReplyRef(parent=Main(cid='bafyreifdjsjf27qcmoa4ajdrzqvk62pmokm5pyyvrnw2cd3lw5hcz5kzue', uri='at://did:plc:atwgx3kry3qwsd67eve5wkwd/app.bsky.feed.post/3kkvjexa6nl2e', py_type='com.atproto.repo.strongRef'), root=Main(cid='bafyreic3xabs2jpcaer7ppk7zzuv55batlenxynjgtekd6mcjwbinmottu', uri='at://did:plc:atwgx3kry3qwsd67eve5wkwd/app.bsky.feed.post/3kkrkrgmizs2y', py_type='com.atproto.repo.strongRef'), py_type='app.bsky.feed.post#replyRef'), tags=None, py_type='app.bsky.feed.post'),
        'uri': 'at://did:plc:7punls2ofrpj3x4tikb5n7ho/app.bsky.feed.post/3kkvk45mviq2b',
        'embed': View(images=[ViewImage(alt='', fullsize='https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:7punls2ofrpj3x4tikb5n7ho/bafkreihxwhsyvjvfi5ttp73ctrzqxkpnhk4kvgiqaxxoqbuerdm45p4dn4@jpeg', thumb='https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:7punls2ofrpj3x4tikb5n7ho/bafkreihxwhsyvjvfi5ttp73ctrzqxkpnhk4kvgiqaxxoqbuerdm45p4dn4@jpeg', aspect_ratio=AspectRatio(height=197, width=256, py_type='app.bsky.embed.images#aspectRatio'), py_type='app.bsky.embed.images#viewImage')], py_type='app.bsky.embed.images#view'),
        'labels': [],
        'like_count': 0,
        'reply_count': 0,
        'repost_count': 0,
        'threadgate': None,
        'viewer': ViewerState(like=None, reply_disabled=None, repost=None, py_type='app.bsky.feed.defs#viewerState'),
        'py_type': 'app.bsky.feed.defs#postView'
    }
    The same post, after flattening
    {
        'author_did': 'did:plc:7punls2ofrpj3x4tikb5n7ho',
        'author_handle': 'lfb.bsky.social',
        'author_display_name': 'Kuřák na paprice 🌶️',
        'created_at': '2024-02-08T10:10:09.453Z',
        'text': "Oh no! How dare you, pronoucing your opinion about politics which might be influencing your very life. In a public space? Willy-nilly? Not on that guy's watch.",
        'langs': ['cs'],
        'cid': 'bafyreiahnq3wcuzbyeorboua3duuxbvd3saxiyuqdwu23cpebb6e3bf3km',
        'indexed_at': '2024-02-08T10:10:09.453Z',
        'like_count': 0,
        'reply_count': 0,
        'repost_count': 0
    }
    """  # noqa
    kwargs = {"q": query}
    posts: list[PostView] = send_request_with_pagination(
        func=client.app.bsky.feed.search_posts,
        kwargs={"params": kwargs},
        response_key="posts",
        limit=limit,
        update_params_directly=True,
    )
    flattened_posts: list[dict] = [flatten_post(post) for post in posts]
    return flattened_posts


def main():
    query = "politics"
    limit = 150
    posts = search_by_query(query=query, limit=limit)
    print(f"Number of posts found: {len(posts)} (expected {limit})")
    print(f"First ten posts: {posts[:10]}")
    print(f"Last ten posts: {posts[-10:]}")


if __name__ == "__main__":
    main()
