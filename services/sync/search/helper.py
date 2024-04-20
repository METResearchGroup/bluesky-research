from typing import Callable, Optional

DEFAULT_MAX_TOTAL_RESULTS_LIMIT = 999999
DEFAULT_LIMIT_RESULTS_PER_REQUEST = 50
MAX_POSTS_PER_REQUEST = 100


def send_request_with_pagination(
    func: Callable,
    kwargs: dict,
    response_key: str,
    args: Optional[tuple] = (),
    limit: Optional[int] = None,
    update_params_directly: bool = False,
    silence_logs: bool = False
) -> list:
    """Implement a request with pagination.

    Useful for endpoints that return a cursor and a list of results.

    Example result (from getFollowers endpoint):
    {
        'followers': [
            ProfileView(did='did:plc:5dhg2sndfjcz7yltxxsb4grs', handle='simoduki301.bsky.social', avatar=None, description=None, display_name='', indexed_at='2024-02-08T09:02:34.745Z', labels=[], viewer=ViewerState(blocked_by=False, blocking=None, blocking_by_list=None, followed_by=None, following=None, muted=False, muted_by_list=None, py_type='app.bsky.actor.defs#viewerState'), py_type='app.bsky.actor.defs#profileView'),
            ProfileView(did='did:plc:kn4e5ek3t2ox6aj65zjuhxwe', handle='kusanmark4.bsky.social', avatar=None, description=None, display_name='', indexed_at='2024-02-08T09:02:28.944Z', labels=[], viewer=ViewerState(blocked_by=False, blocking=None, blocking_by_list=None, followed_by=None, following=None, muted=False, muted_by_list=None, py_type='app.bsky.actor.defs#viewerState'), py_type='app.bsky.actor.defs#profileView')
        ],
        'subject': ProfileView(did='did:plc:eclio37ymobqex2ncko63h4r', handle='nytimes.com', avatar='https://cdn.bsky.app/img/avatar/plain/did:plc:eclio37ymobqex2ncko63h4r/bafkreidvvqj5jymmpaeklwkpq6gi532el447mjy2yultuukypzqm5ohfju@jpeg', description='In-depth, independent reporting to better understand the world, now on Bluesky. News tips? Share them here: http://nyti.ms/2FVHq9v', display_name='The New York Times', indexed_at='2024-01-25T23:46:23.929Z', labels=[], viewer=ViewerState(blocked_by=False, blocking=None, blocking_by_list=None, followed_by=None, following='at://did:plc:w5mjarupsl6ihdrzwgnzdh4y/app.bsky.graph.follow/3kkvauysemf2p', muted=False, muted_by_list=None, py_type='app.bsky.actor.defs#viewerState'), py_type='app.bsky.actor.defs#profileView'),
        'cursor': '3kkvgejfrew2y'
    }

    Example result (from getAuthorFeed endpoint):
    {
        'feed': [
            FeedViewPost(post=PostView(author=ProfileViewBasic(did='did:plc:eclio37ymobqex2ncko63h4r', handle='nytimes.com', avatar='https://cdn.bsky.app/img/avatar/plain/did:plc:eclio37ymobqex2ncko63h4r/bafkreidvvqj5jymmpaeklwkpq6gi532el447mjy2yultuukypzqm5ohfju@jpeg', display_name='The New York Times', labels=[], viewer=ViewerState(blocked_by=False, blocking=None, blocking_by_list=None, followed_by=None, following='at://did:plc:w5mjarupsl6ihdrzwgnzdh4y/app.bsky.graph.follow/3kkvauysemf2p', muted=False, muted_by_list=None, py_type='app.bsky.actor.defs#viewerState'), py_type='app.bsky.actor.defs#profileViewBasic'), cid='bafyreih5v5yirvzs3dv3bfb3vzwja2tquemgw5jtajjy7kekxvoowkiv5y', indexed_at='2024-02-07T22:42:20.003Z', record=Main(created_at='2024-02-07T22:42:20.003Z', text='A listeria outbreak that sickened at least 26 people in 11 states since 2014 has been linked to queso fresco and cotija cheese made by Rizo-López Foods, a California-based food supplier, officials said. The company recalled its dairy products this week.', embed=Main(external=External(description='Rizo-López Foods recalled its dairy products this week, as officials linked some of them to an outbreak that has sickened 26 people since 2014.', title='Dairy Products Are Linked to Listeria Outbreak', uri='https://www.nytimes.com/2024/02/07/business/cheese-listeria-outbreak-recall.html?smtyp=cur&smid=bsky-nytimes', thumb=BlobRef(mime_type='image/jpeg', size=363715, ref=IpldLink(link='bafkreihntuin5roaann33qkn6ulsvlbf6lolw3uk6ylj4njip3i5tuthwu'), py_type='blob'), py_type='app.bsky.embed.external#external'), py_type='app.bsky.embed.external'), entities=None, facets=None, labels=None, langs=['en'], reply=None, tags=None, py_type='app.bsky.feed.post'), uri='at://did:plc:eclio37ymobqex2ncko63h4r/app.bsky.feed.post/3kkudo7lqma2v', embed=View(external=ViewExternal(description='Rizo-López Foods recalled its dairy products this week, as officials linked some of them to an outbreak that has sickened 26 people since 2014.', title='Dairy Products Are Linked to Listeria Outbreak', uri='https://www.nytimes.com/2024/02/07/business/cheese-listeria-outbreak-recall.html?smtyp=cur&smid=bsky-nytimes', thumb='https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:eclio37ymobqex2ncko63h4r/bafkreihntuin5roaann33qkn6ulsvlbf6lolw3uk6ylj4njip3i5tuthwu@jpeg', py_type='app.bsky.embed.external#viewExternal'), py_type='app.bsky.embed.external#view'), labels=[], like_count=46, reply_count=5, repost_count=23, threadgate=None, viewer=ViewerState(like=None, reply_disabled=None, repost=None, py_type='app.bsky.feed.defs#viewerState'), py_type='app.bsky.feed.defs#postView'), reason=None, reply=None, py_type='app.bsky.feed.defs#feedViewPost'),
            FeedViewPost(post=PostView(author=ProfileViewBasic(did='did:plc:eclio37ymobqex2ncko63h4r', handle='nytimes.com', avatar='https://cdn.bsky.app/img/avatar/plain/did:plc:eclio37ymobqex2ncko63h4r/bafkreidvvqj5jymmpaeklwkpq6gi532el447mjy2yultuukypzqm5ohfju@jpeg', display_name='The New York Times', labels=[], viewer=ViewerState(blocked_by=False, blocking=None, blocking_by_list=None, followed_by=None, following='at://did:plc:w5mjarupsl6ihdrzwgnzdh4y/app.bsky.graph.follow/3kkvauysemf2p', muted=False, muted_by_list=None, py_type='app.bsky.actor.defs#viewerState'), py_type='app.bsky.actor.defs#profileViewBasic'), cid='bafyreicb35ugbubqbkwpadfbkyzjisrshvi66tyxs2igfzj4ilj3bz5fi4', indexed_at='2024-02-07T22:26:59.642Z', record=Main(created_at='2024-02-07T22:26:59.642Z', text='Male elephant seals are not known for their paternal instincts. But in 2022, a rising tide pulled a pup out to sea, where it was struggling to stay afloat. A male used his body to nudge it back to the beach — probably saving its life.\n\nResearchers said they had never seen anything like this before.', embed=Main(external=External(description='In an unlikely act of altruism observed two years ago, a male elephant seal prevented a younger animal from drowning.', title='A Two-Ton Lifeguard That Saved a Young Pup', uri='https://www.nytimes.com/2024/02/07/science/elephant-seals-pup-drowning.html?smtyp=cur&smid=bsky-nytimes', thumb=BlobRef(mime_type='image/jpeg', size=276206, ref=IpldLink(link='bafkreiaxxjeln2vjjfccjepy4gsvzpsb6seutgxjc6j5p2fydcvtprm4pq'), py_type='blob'), py_type='app.bsky.embed.external#external'), py_type='app.bsky.embed.external'), entities=None, facets=None, labels=None, langs=['en'], reply=None, tags=None, py_type='app.bsky.feed.post'), uri='at://did:plc:eclio37ymobqex2ncko63h4r/app.bsky.feed.post/3kkucsrva2i2v', embed=View(external=ViewExternal(description='In an unlikely act of altruism observed two years ago, a male elephant seal prevented a younger animal from drowning.', title='A Two-Ton Lifeguard That Saved a Young Pup', uri='https://www.nytimes.com/2024/02/07/science/elephant-seals-pup-drowning.html?smtyp=cur&smid=bsky-nytimes', thumb='https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:eclio37ymobqex2ncko63h4r/bafkreiaxxjeln2vjjfccjepy4gsvzpsb6seutgxjc6j5p2fydcvtprm4pq@jpeg', py_type='app.bsky.embed.external#viewExternal'), py_type='app.bsky.embed.external#view'), labels=[], like_count=100, reply_count=1, repost_count=16, threadgate=None, viewer=ViewerState(like=None, reply_disabled=None, repost=None, py_type='app.bsky.feed.defs#viewerState'), py_type='app.bsky.feed.defs#postView'), reason=None, reply=None, py_type='app.bsky.feed.defs#feedViewPost')
        ],
        'cursor': '2024-02-07T22:26:59.642Z'
    }

    Based on https://github.com/MarshalX/atproto/blob/main/examples/advanced_usage/handle_cursor_pagination.py
    """  # noqa
    cursor = None
    total_fetched: int = 0

    total_results = []

    if limit is None:
        limit = DEFAULT_MAX_TOTAL_RESULTS_LIMIT

    total_to_fetch: int = limit

    if (
        limit > MAX_POSTS_PER_REQUEST
        and limit != DEFAULT_MAX_TOTAL_RESULTS_LIMIT
        and not silence_logs
    ):
        print(
            f"Limit of {limit} exceeds the maximum of {MAX_POSTS_PER_REQUEST}."
        )
        print(f"Will batch requests in chunks of {MAX_POSTS_PER_REQUEST}.")

    request_limit = min(limit, MAX_POSTS_PER_REQUEST)

    while True:
        if not silence_logs:
            print(f"Fetching {request_limit} results, out of total max of {limit}...") # noqa
        if update_params_directly:
            kwargs["params"].update({"cursor": cursor, "limit": request_limit})
        else:
            kwargs.update({"cursor": cursor, "limit": request_limit})
        res = func(*args, **kwargs)
        # get results from pagination
        # get attribute "response_key" from response, not using brackets
        results: list = getattr(res, response_key)
        assert isinstance(results, list)
        num_fetched = len(results)
        total_fetched += num_fetched
        total_results.extend(results[:total_to_fetch])
        total_to_fetch -= num_fetched
        if not res.cursor:
            break
        if total_fetched >= limit:
            print(f"Total fetched results: {total_fetched}")
            break
        cursor = res.cursor

    return total_results
