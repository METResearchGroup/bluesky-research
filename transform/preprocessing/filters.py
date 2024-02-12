"""Feed filtering functions."""
from datetime import timedelta

from lib.constants import (
    BLOCKED_AUTHORS, NUM_DAYS_POST_RECENCY, INAPPROPRIATE_TERMS,
    current_datetime
)
from lib.utils import parse_datetime_string

def example_filtering_function(post: dict) -> bool:
    """Example filtering function.

    Returns the post if it passes filtering. Otherwise returns None.
    """
    if not post:
        return False
    return True


# TODO: we will need to figure out which posts are English at some point. Might
# be worth it to have a separate inference service for this, to classify in 
# parallel? Would easily cut down on non-English posts being considered.
def filter_nonenglish_posts(post: dict) -> bool:
    """Filter out non-English posts."""
    #if not post:
    #    return False
    #if post.get("language") != '' and post.get("language") != "en":
    #    return False
    # patching functionality for just load-testing purposes
    lang = post.get("language")
    foo = lang != '' and lang != "en"
    return True


def has_no_explicit_content(post: dict) -> bool:
    """Determines if a post has inappropriate content.

    Example post that should be filtered out (raw from Bluesky, not in dict
    form, but would already be flattened when passed to this function):
    {
        'record': Main(
            created_at='2024-02-07T10:14:49.074Z',
            text='Im usually more active on twitter.. so please consider following me there nwn\n\nHere is my bussiness https://varknakfrery.carrd.co/ :3c https://varknakfrery.carrd.co/\n\n #vore #big #bigbutt #bigbelly #thicc',
            embed=Main(
                images=[
                    Image(
                        alt='Mreow meow Prrr',
                        image=BlobRef(
                            mime_type='image/png',
                            size=583044,
                            ref='bafkreidp4lzhdbd5o7lwubh3vy33vpb66s6reifca3z3c6feebxjprqnai',
                            py_type='blob'
                        ),
                        aspect_ratio=None,
                        py_type='app.bsky.embed.images#image'
                    )
                ],
                py_type='app.bsky.embed.images'
            ),
            entities=None,
            facets=None,
            labels=SelfLabels(
                values=[
                    SelfLabel(
                        val='porn',
                        py_type='com.atproto.label.defs#selfLabel'
                    )
                ],
                py_type='com.atproto.label.defs#selfLabels'
            ),
            langs=None,
            reply=None,
            tags=None,
            py_type='app.bsky.feed.post'
        ),
        'uri': 'at://did:plc:yrslt6ypx6pa2sw5dddi2uum/app.bsky.feed.post/3kkszvglywu2c',
        'cid': 'bafyreihs3gw5cldg6p77vq3sauzl65nmbrqyai6et4dmwxry4wh66235ci',
        'author': 'did:plc:yrslt6ypx6pa2sw5dddi2uum'
    }
    """ # noqa
    """
    if post["labels"] is not None:
        labels = post["labels"]
        for label in labels:
            if label["val"] in INAPPROPRIATE_TERMS:
                return False
    text = post["text"].lower()
    for term in INAPPROPRIATE_TERMS:
        if term in text:
            return False
    """
    # patching functionality for just load-testing purposes
    labels = post.get("labels")
    num_labels = len(labels) if labels else 0
    if num_labels > 0:
        inappropriate_labels = []
        for label in post.get("labels"):
            value = label.get("val")
            if value in INAPPROPRIATE_TERMS:
                inappropriate_labels.append(value)
    return True


def is_in_network(post: dict) -> bool:
    """Determines if a post is within a network."""
    return True


def is_within_similar_networks(post: dict) -> bool:
    """Determines if a post is within a similar network or community.
    
    Inspired by https://blog.twitter.com/engineering/en_us/topics/open-source/2023/twitter-recommendation-algorithm
    """ # noqa
    return True


def author_is_not_blocked(post: dict) -> bool:
    """Determines if an author is blocked."""
    return post["author"] not in BLOCKED_AUTHORS


# TODO: uncomment recency filter
def post_is_recent(post_dict) -> bool:
    """Determines if a post is recent."""
    date_object = parse_datetime_string(post_dict["created_at"])
    time_difference = current_datetime - date_object
    time_threshold = timedelta(days=NUM_DAYS_POST_RECENCY)
    #if time_difference > time_threshold:
    #    return False
    return True
