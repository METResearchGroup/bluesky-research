"""Helper functions for processing Bluesky data."""
from atproto_client.models.app.bsky.actor.defs import ProfileView, ProfileViewDetailed
from atproto_client.models.app.bsky.feed.defs import ThreadViewPost
from atproto_client.models.app.bsky.feed.post import GetRecordResponse
from atproto_client.models.app.bsky.feed.get_likes import Like
from atproto_client.models.app.bsky.feed.get_post_thread import Response as PostThreadResponse

from lib.helper import client
from services.sync.search.helper import send_request_with_pagination

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
    """
    author_and_post_id: dict = get_author_handle_and_post_id_from_link(link)
    author_did: str = get_author_did_from_handle(author_and_post_id["author"])
    post_rkey = author_and_post_id["post_id"]
    profile_identify = author_did
    print(f"Getting post record for {post_rkey} by {profile_identify}")
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
    """Calculates the number of likes, retweets, and comments for a post given a link."""
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
    """
    author_and_post_id: dict = get_author_handle_and_post_id_from_link(link)
    author_did: str = get_author_did_from_handle(author_and_post_id["author"])
    return client.get_profile(author_did)
