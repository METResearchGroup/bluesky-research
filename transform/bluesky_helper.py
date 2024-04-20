"""Helper functions for processing Bluesky data."""
from typing import Optional

from atproto_client.models.app.bsky.actor.defs import ProfileView, ProfileViewDetailed # noqa
from atproto_client.models.app.bsky.feed.defs import FeedViewPost, GeneratorView, ThreadViewPost # noqa
from atproto_client.models.app.bsky.feed.post import GetRecordResponse
from atproto_client.models.app.bsky.feed.get_actor_feeds import Response as GetActorFeedsResponse  # noqa
from atproto_client.models.app.bsky.feed.get_feed import Response as FeedResponse # noqa
from atproto_client.models.app.bsky.feed.get_likes import Like
from atproto_client.models.app.bsky.feed.get_post_thread import Response as PostThreadResponse # noqa
from atproto_client.models.app.bsky.graph.defs import ListItemView
from atproto_client.models.app.bsky.graph.get_list import Response as GetListResponse # noqa

from lib.constants import current_datetime_str
from lib.helper import client
from services.sync.search.helper import send_request_with_pagination
from transform.transform_raw_data import flatten_firehose_post


def get_author_handle_and_post_id_from_link(link: str) -> dict[str, str]:
    """Given a link, get the author and post ID.

    Example:
    >get_author_and_post_id_from_link("https://bsky.app/profile/scottsantens.com/post/3knqkh2es7k2i")
    {'author': 'scottsantens.com', 'post_id': '3knqkh2es7k2i}
    """
    # Split the link by the forward slash
    split_link = link.split("/")
    # Get the author and post ID
    author = split_link[4]
    post_id = split_link[6]
    return {"author": author, "post_id": post_id}


def get_author_did_from_handle(author_handle: str) -> str:
    """Given an author handle, get the DID.

    Example:
    >get_author_did_from_handle("scottsantens.com")
    "did:example:123"
    """
    # Get the profile
    profile = client.get_profile(author_handle)
    # Get the DID
    return profile["did"]


def get_post_record_from_post_link(link: str) -> GetRecordResponse:
    """Given a post link, get the post record.

    Example:
    >post = get_post_record_from_post_link("https://bsky.app/profile/gbbranstetter.bsky.social/post/3knssi4ouko24")
    GetRecordResponse(
        uri='at://did:plc:mlmouohgzbjofidukcp4pxf2/app.bsky.feed.post/3knssi4ouko24',
        value=Record(created_at='2024-03-16T12:17:36.784Z', text='A running theme in Woods\' telling is how even those who opposed Hitler blamed his rise on the "depravity" and excess of Weimar Berlin "as if the mere presence of ersatz women in the club was enough to foment and justify a right-wing putsch"--a phrase I find myself repeating often', embed=Main(record=Main(cid='bafyreiesskdi2vfkrvj2kaajglsklqd7b3wywn2goyxhr7ctl3kvaikbsa', uri='at://did:plc:mlmouohgzbjofidukcp4pxf2/app.bsky.feed.post/3knss3jmoeu2f', py_type='com.atproto.repo.strongRef'), py_type='app.bsky.embed.record'), entities=None, facets=None, labels=None, langs=['en'], reply=None, tags=None, py_type='app.bsky.feed.post'),
        cid='bafyreidujr2qzblrtxyh6e5shqtjdool2c7vim5jsqybdhi4pkpuphuj5q'
    )
    """ # noqa
    author_and_post_id: dict = get_author_handle_and_post_id_from_link(link)
    author_did: str = get_author_did_from_handle(author_and_post_id["author"])
    post_rkey = author_and_post_id["post_id"]
    profile_identify = author_did
    print(f"Getting post record for {post_rkey} by {profile_identify}")
    response = client.get_post(
        post_rkey=post_rkey, profile_identify=profile_identify
    )
    return response


def convert_post_link_to_post(post_link: str) -> dict:
    """Gets post from the Record API and converts it to a RawPost.

    The post record, by default, won't hydrate the author information, but we
    can set a blank as we don't need that information if all we care about is
    the post.
    """ # noqa
    record = get_post_record_from_post_link(post_link)
    # set up in the format expected by our RawPost class.
    post_dict = {
        "record": record.value,
        "uri": record.uri,
        "cid": record.cid,
        "author": ""
    }
    flattened_firehose_post: dict = flatten_firehose_post(post_dict)
    return flattened_firehose_post


def get_post_record_given_post_uri(post_uri: str) -> GetRecordResponse:
    """Given a post URI, get the post record.

    Example:
    >post = get_post_record_given_post_uri("at://did:plc:mlmouohgzbjofidukcp4pxf2/app.bsky.feed.post/3knssi4ouko24")
    GetRecordResponse(
        uri='at://did:plc:mlmouohgzbjofidukcp4pxf2/app.bsky.feed.post/3knssi4ouko24',
        value=Record(created_at='2024-03-16T12:17:36.784Z', text='A running theme in Woods\' telling is how even those who
    """ # noqa
    split_uri = post_uri.split("/")
    post_rkey = split_uri[-1]
    profile_identify = split_uri[-3]
    response = client.get_post(
        post_rkey=post_rkey, profile_identify=profile_identify
    )
    return response


def get_repost_profiles(post_uri: str) -> list[ProfileView]:
    """Get the profiles of all the users who reposted a post."""
    reposts: list[ProfileView] = send_request_with_pagination(
        func=client.get_reposted_by,
        kwargs={"uri": post_uri},
        response_key="reposted_by",
        limit=None,
        silence_logs=True
    )
    return reposts


def get_liked_by_profiles(post_uri: str) -> list[Like]:
    """Get the profiles of all the users who liked a post."""
    likes: list[Like] = send_request_with_pagination(
        func=client.get_likes,
        kwargs={"uri": post_uri},
        response_key="likes",
        limit=None,
        silence_logs=True
    )
    return likes


def get_post_thread_replies(post_uri: str) -> list[ThreadViewPost]:
    """Get the thread of replies to a post."""
    response: PostThreadResponse = client.get_post_thread(post_uri)
    thread: ThreadViewPost = response.thread
    replies: list[ThreadViewPost] = thread.replies
    return replies


def calculate_post_engagement(post_response: GetRecordResponse) -> dict:
    """Calculates the number of likes, retweets, and comments for a post."""
    uri = post_response["uri"]

    # grab # of likes
    likes: list[Like] = get_liked_by_profiles(uri)
    num_likes: int = len(likes)

    # grab # of retweets/reposts
    reposts: list[ProfileView] = get_repost_profiles(uri)
    num_reposts = len(reposts)

    # grab # of replies
    replies: list[ThreadViewPost] = get_post_thread_replies(uri)
    num_replies = len(replies)

    return {
        "uri": uri,
        "num_likes": num_likes,
        "num_reposts": num_reposts,
        "num_replies": num_replies
    }


def calculate_post_engagement_from_link(link: str) -> dict:
    """Calculates the number of likes, retweets, and comments for a post given a link.""" # noqa
    post_response: GetRecordResponse = get_post_record_from_post_link(link)
    post_engagement = calculate_post_engagement(post_response)
    return {
        "link": link,
        "created_at": post_response.value.created_at,
        **post_engagement
    }


def get_author_profile_from_link(link: str) -> ProfileViewDetailed:
    """Given a link, get the author profile.

    Example:
    >get_author_profile_from_link("https://bsky.app/profile/gbbranstetter.bsky.social/post/3knssi4ouko24")
    ProfileViewDetailed(
        did='did:plc:mlmouohgzbjofidukcp4pxf2',
        handle='gbbranstetter.bsky.social',
        avatar='https://cdn.bsky.app/img/avatar/plain/did:plc:mlmouohgzbjofidukcp4pxf2/bafkreicdpwjn4jyxlcbkxmm7baha6vfohzujcjfre6vvx35vruhud64a7e@jpeg',
        banner='https://cdn.bsky.app/img/banner/plain/did:plc:mlmouohgzbjofidukcp4pxf2/bafkreidnd5k7yltrbprbygfgpzoxngihqp4aziouuwsvsecindofzipjhe@jpeg',
        description='comms strategy @aclu // autonomy.substack.com // opinions my own ',
        display_name='Gillian Branstetter',
        followers_count=14545,
        follows_count=502,
        indexed_at='2024-03-20T14:58:09.443Z',
        labels=[Label(cts='1970-01-01T00:00:00.000Z', src='did:plc:mlmouohgzbjofidukcp4pxf2', uri='at://did:plc:mlmouohgzbjofidukcp4pxf2/app.bsky.actor.profile/self', val='!no-unauthenticated', cid='bafyreigay5agy6hjvqnbi2xcn4bwinxaw4g5g3vjcxjsom5me7njrigv2a', neg=None, py_type='com.atproto.label.defs#label')],
        posts_count=4429,
        viewer=ViewerState(blocked_by=False, blocking=None, blocking_by_list=None, followed_by=None, following=None, muted=False, muted_by_list=None, py_type='app.bsky.actor.defs#viewerState'),
        py_type='app.bsky.actor.defs#profileViewDetailed',
        associated={'lists': 0, 'feedgens': 0, 'labeler': False}
    )
    """ # noqa
    author_and_post_id: dict = get_author_handle_and_post_id_from_link(link)
    author_did: str = get_author_did_from_handle(author_and_post_id["author"])
    return client.get_profile(author_did)


def get_user_info_from_list_item(list_item: ListItemView):
    """Given a list item view (from the list view), return a dictionary with
    the user's blocked user id (did) and their handle (username)"""
    return {"did": list_item.subject.did, "handle": list_item.subject.handle}


def get_users_added_to_list(list_items: list[ListItemView]) -> list[dict]:
    """Given a list of list items, return a list of dictionaries with the
    user's blocked user id (did) and their handle (username)"""
    return [get_user_info_from_list_item(item) for item in list_items]


def generate_list_uri_given_list_url(list_url: str) -> str:
    """Given a list url, return the corresponding list uri.

    Example:
    >> list_url = "https://bsky.app/profile/nickwrightdata.ntw.app/lists/3kmr32obinz2q"
    >> generate_list_uri_given_list_url(list_url)
    "at://did:plc:7allko6vtrpvyxxcd5beapou/app.bsky.graph.list/3kmr32obinz2q"
    """ # noqa
    split_url = list_url.split("/")
    author_handle: str = split_url[-3]
    list_did: str = split_url[-1]

    author_did: str = get_author_did_from_handle(author_handle)
    list_uri = f"at://{author_did}/app.bsky.graph.list/{list_did}"
    return list_uri


def get_users_on_list_given_list_uri(list_uri: str) -> list[dict]:
    """Given a list uri, return a list of dictionaries with the user's
    blocked user id (did) and their handle (username)"""
    res: GetListResponse = client.app.bsky.graph.get_list(
        params={"list": list_uri})
    return get_users_added_to_list(res.items)


def get_users_in_list_given_list_url(list_url: str) -> list[dict]:
    """Given the URL to a list, return the users added to the list."""
    list_uri: str = generate_list_uri_given_list_url(list_url)
    return get_users_on_list_given_list_uri(list_uri)


def get_list_info_given_list_url(list_url: str) -> dict:
    """Given the URL of a list, get both the metadata for a list as well as
    the users on the list."""
    list_uri: str = generate_list_uri_given_list_url(list_url)
    res: GetListResponse = client.app.bsky.graph.get_list(
        params={"list": list_uri})
    list_metadata: dict = {
        "cid": res.list.cid,
        "name": res.list.name,
        "uri": res.list.uri,
        "description": res.list.description,
        "author_did": res.list.creator.did,
        "author_handle": res.list.creator.handle,
    }
    users: list[dict] = get_users_added_to_list(res.items)
    return {"list_metadata": list_metadata, "users": users}


def get_list_and_user_data_from_list_links(list_urls: list[str]) -> dict:
    """Given a list of list URLs, return a list of dictionaries with the list
    metadata and the users on the list."""
    list_data_list = [get_list_info_given_list_url(url) for url in list_urls]
    lists_list = []
    users_list = []
    for list_data in list_data_list:
        # add the list metadata to the lists_list
        lists_list.append(list_data["list_metadata"])
        # for each user, get their data plus which list it cames from
        for user in list_data["users"]:
            user["source_list_uri"] = list_data["list_metadata"]["uri"]
            user["source_list_name"] = list_data["list_metadata"]["name"]
            user["timestamp_added"] = current_datetime_str
            users_list.append(user)

    # for each user in users_list, dedupe based on the "did" key. We only need
    # one entry per user.
    deduped_users_list = []
    seen_dids = set()
    for user in users_list:
        if user["did"] not in seen_dids:
            deduped_users_list.append(user)
            seen_dids.add(user["did"])

    return {"lists": lists_list, "users": deduped_users_list}


def get_author_feeds(
    author_handle: Optional[str] = None,
    author_did: Optional[str] = None,
    limit: Optional[int] = 50,
    cursor: Optional[str] = None
) -> list[dict]:
    """Given an author handle, get the feeds that they have.

    Example valid request to the endpoint:
    client.app.bsky.feed.get_actor_feeds({"actor": "did:plc:tenurhgjptubkk5zf5qhi3og"})

    We need to get the author DID given their handle.

    Returns a list of GeneratorView objects, where each GeneratorView is a view
    of the feed generator. Here is an example of a GeneratorView:

    GeneratorView(
        cid='bafyreifblss7nfly7dp4ubhq5qt4kxeo43kwhk27dd6bg37cpg4pqaf7z4',
        creator=ProfileView(
            did='did:plc:tenurhgjptubkk5zf5qhi3og',
            handle='skyfeed.xyz',
            avatar='https://cdn.bsky.app/img/avatar/plain/did:plc:tenurhgjptubkk5zf5qhi3og/bafkreif3xgkr6pq5r7k5oiw4dttwvgjeoqhhgzksxkxzojiwtgicf6zfeq@jpeg',
            description='A collection of custom feeds to enhance your Bluesky experience ⛅\n\nSource code with all queries/algorithms: https://skyfeed.xyz/queries',
            display_name='Sky Feeds',
            indexed_at='2024-01-26T00:15:37.143Z',
            labels=[],
            viewer=ViewerState(
                blocked_by=False,
                blocking=None,
                blocking_by_list=None,
                followed_by=None,
                following='at://did:plc:w5mjarupsl6ihdrzwgnzdh4y/app.bsky.graph.follow/3knu4u66ag32s',
                muted=False,
                muted_by_list=None,
                py_type='app.bsky.actor.defs#viewerState'
            ),
            py_type='app.bsky.actor.defs#profileView'
        ),
        did='did:web:skyfeed.me',
        display_name="What's Warm",
        indexed_at='2023-05-22T13:53:19.996Z',
        uri='at://did:plc:tenurhgjptubkk5zf5qhi3og/app.bsky.feed.generator/whats-warm',
        avatar='https://cdn.bsky.app/img/avatar/plain/did:plc:tenurhgjptubkk5zf5qhi3og/bafkreibmc3eb7gkqvjmhmbkwnoisjf4lhs6gxm62yjehhgiyvuch3hs7oe@jpeg',
        description='Trending content from the whole network with more noise',
        description_facets=None,
        like_count=30,
        viewer=GeneratorViewerState(
            like=None,
            py_type='app.bsky.feed.defs#generatorViewerState'
        ),
        py_type='app.bsky.feed.defs#generatorView',
        labels=[]
    )
    """ # noqa
    if not author_did:
        author_did = get_author_did_from_handle(author_handle)
    payload = {"actor": author_did}
    if limit:
        payload["limit"] = limit
    if cursor:
        payload["cursor"] = cursor
    response: GetActorFeedsResponse = (
        client.app.bsky.feed.get_actor_feeds(payload)
    )
    feeds: list[GeneratorView] = response.feeds
    return feeds


def get_posts_from_custom_feed(
    feed_uri: str,
    limit: Optional[int] = 50,
    cursor: Optional[str] = None
) -> list[FeedViewPost]:
    """Given the URI of a post, get the posts from the feed.

    Corresponding lexicon: https://github.com/MarshalX/atproto/blob/main/lexicons/app.bsky.feed.getFeed.json

    Returns a list of posts in the feed, as a FeedViewPost. Here is an example:

    example_feed_uri = "at://did:plc:tenurhgjptubkk5zf5qhi3og/app.bsky.feed.generator/catch-up"
    feed = client.app.bsky.feed.get_feed({"feed": example_feed_uri})
    post = feed.feed[0]
    post:
    FeedViewPost(
        post=PostView(
            author=ProfileViewBasic(
                did='did:plc:ne454cutmmpesxbcvkvz375d',
                handle='harrisj.bsky.social',
                avatar='https://cdn.bsky.app/img/avatar/plain/did:plc:ne454cutmmpesxbcvkvz375d/bafkreibgjdbc6afa5fcql6q5t4ugmma3jyle5nwpfvmu65kkgj5tvpetde@jpeg',
                display_name='Jacob Harris',
                labels=[],
                viewer=ViewerState(
                    blocked_by=False,
                    blocking=None,
                    blocking_by_list=None,
                    followed_by=None,
                    following=None,
                    muted=False,
                    muted_by_list=None,
                    py_type='app.bsky.actor.defs#viewerState'
                ),
                py_type='app.bsky.actor.defs#profileViewBasic'
            ),
            cid='bafyreihpcqtpclkr7klcchb4cdb2nvlvar2bjteyeo3h2vbwsgy6ayid3u',
            indexed_at='2024-04-16T23:57:23.470Z',
            record=Record(
                created_at='2024-04-16T23:57:23.470Z',
                text='“Before you croque” was right there',
                embed=Main(
                    images=[
                        Image(
                            alt='A Food & Wine feature titled “8 French Sandwiches to Eat Before You Die”',
                            image=BlobRef(
                                mime_type='image/jpeg',
                                size=832962,
                                ref=IpldLink(link='bafkreicp2mjfm747gffroto5dmdnp6dayrqwdkunn2mh6mlp5wfvvrt7x4'),
                                py_type='blob'
                            ),
                            aspect_ratio=AspectRatio(height=1689, width=1170, py_type='app.bsky.embed.images#aspectRatio'),
                            py_type='app.bsky.embed.images#image'
                        )
                    ],
                    py_type='app.bsky.embed.images'
                ),
                entities=None,
                facets=None,
                labels=None,
                langs=['en'],
                reply=None,
                tags=None,
                py_type='app.bsky.feed.post'
            ),
            uri='at://did:plc:ne454cutmmpesxbcvkvz375d/app.bsky.feed.post/3kqbxzx74mp2h',
            embed=View(
                images=[
                    ViewImage(
                        alt='A Food & Wine feature titled “8 French Sandwiches to Eat Before You Die”',
                        fullsize='https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:ne454cutmmpesxbcvkvz375d/bafkreicp2mjfm747gffroto5dmdnp6dayrqwdkunn2mh6mlp5wfvvrt7x4@jpeg',
                        thumb='https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:ne454cutmmpesxbcvkvz375d/bafkreicp2mjfm747gffroto5dmdnp6dayrqwdkunn2mh6mlp5wfvvrt7x4@jpeg',
                        aspect_ratio=AspectRatio(height=1689, width=1170, py_type='app.bsky.embed.images#aspectRatio'),
                        py_type='app.bsky.embed.images#viewImage'
                    )
                ],
                py_type='app.bsky.embed.images#view'
            ),
            labels=[],
            like_count=3531,
            reply_count=43,
            repost_count=678,
            threadgate=None,
            viewer=ViewerState(like=None, reply_disabled=None, repost=None, py_type='app.bsky.feed.defs#viewerState'),
            py_type='app.bsky.feed.defs#postView'
        ),
        reason=None,
        reply=None,
        py_type='app.bsky.feed.defs#feedViewPost'
    )
    """ # noqa
    payload = {"feed": feed_uri}
    if limit:
        payload["limit"] = limit
    if cursor:
        payload["cursor"] = cursor
    response: FeedResponse = client.app.bsky.feed.get_feed(payload)
    feed: list[FeedViewPost] = response.feed
    return feed[:limit]


def construct_feed_uri_from_feed_url(feed_url: str) -> str:
    """Constructs the feed URI from the feed URL.

    Example:
    >>> feed_url = "https://bsky.app/profile/did:plc:tenurhgjptubkk5zf5qhi3og/feed/catch-up"
    >>> construct_feed_uri_from_feed_url(feed_url)
    "at://did:plc:tenurhgjptubkk5zf5qhi3og/app.bsky.feed.generator/catch-up"
    """ # noqa
    split_url = feed_url.split("/")
    author_did = split_url[-3]
    feed_name = split_url[-1]
    return f"at://{author_did}/app.bsky.feed.generator/{feed_name}"


def get_posts_from_custom_feed_url(
    feed_url: str,
    limit: Optional[int] = 50,
    cursor: Optional[str] = None
) -> list[FeedViewPost]:
    """Given the link to a custom feed, get the posts from that feed.

    Example feed link: https://bsky.app/profile/did:plc:tenurhgjptubkk5zf5qhi3og/feed/catch-up-weekly
    """  # noqa
    feed_uri = construct_feed_uri_from_feed_url(feed_url)
    return get_posts_from_custom_feed(feed_uri, limit, cursor)
