import json
import os
import requests

from lib.helper import client


user_profiles_filename = "user_profiles.jsonl"
search_file_directory = os.path.dirname(os.path.abspath(__file__))
user_profiles_fp = os.path.join(search_file_directory, user_profiles_filename)


def write_user_profiles_to_file(user_profiles: list[dict]) -> None:
    """Write user profiles to a file.
    
    NOTE: f we want more users, we can add pagination.
    Example: https://github.com/kojira/bluesky-chan/blob/main/util.py#L244
    """
    with open(user_profiles_fp, "w") as f:
        for user_profile in user_profiles:
            f.write(json.dumps(user_profile) + "\n")
    print(f"Wrote {len(user_profiles)} user profiles to {user_profiles_filename}") # noqa


def load_user_profiles_from_file() -> list[dict]:
    """Load user profiles from a file."""
    with open(user_profiles_fp, "r") as f:
        user_profiles = [json.loads(line) for line in f]
    return user_profiles


def sync_did_user_profiles() -> list[dict]:
    """Sync data from https://plc.directory/export, a data dump of DID data,
    and get some sample user profiles.
    
    Example:
    {
        "did":"did:plc:gwpm3pe7p2xigazbkjf3eu7g",
        "operation":{
            "sig":"hdv0ZZaBiFSexncjD_xLL7BrjCqcuZ83_QGOr1dpMeZEDhC3IWH7EkKT4UgPtHi595ErG8JgbWlQIclVmZXX4w",
            "prev":null,
            "type":"create",
            "handle":"ancapalex.bsky.social",
            "service":"https://bsky.social",
            "signingKey":"did:key:zQ3shP5TBe1sQfSttXty15FAEHV1DZgcxRZNxvEWnPfLFwLxJ",
            "recoveryKey":"did:key:zQ3shhCGUqDKjStzuDxPkTxN6ujddP4RkEKJJouJGRRkaLGbg"
        },
        "cid":"bafyreibvt3g3zh36v2bqgiksjozfhzs2vzpagpjxjn25qvlbluipiill2q",
        "nullified":false,
        "createdAt":"2023-01-18T08:49:23.662Z"
    }
    """ # noqa
    with requests.get("https://plc.directory/export") as r:
        r.raise_for_status()
        lines = r.text.split('\n')
        user_profiles = [json.loads(line) for line in lines if line.strip()]
        write_user_profiles_to_file(user_profiles=user_profiles)

    return user_profiles


def get_author_feed(user_profile: dict) -> dict:
    """Given an author, get their feed."""
    # https://atproto.blue/en/latest/atproto_client/index.html#atproto_client.Client.get_author_feed
    try:
        feed = client.get_author_feed(actor=user_profile["did"])
    except Exception as e:
        # for cases where profile can't be found.
        print(f"Error getting feed for {user_profile['did']}: {e}")
        return {}
    return feed.dict()


def export_author_feed(user_profile) -> None:
    """Export the author feed to a file."""
    feed: dict = get_author_feed(user_profile=user_profile)
    if len(feed) == 0:
        return
    with open(f"feed_{user_profile['did']}.json", "w") as f:
        json.dump(feed, f)


def export_author_feeds(user_profiles: list[dict], count=1) -> None:
    """Export the author feeds to files."""
    total = 0
    while total < count:
        user_profile = user_profiles[total]
        export_author_feed(user_profile=user_profile)
        total += 1
    print(f"Exported {total} author feeds.")


def load_author_feeds_from_file() -> list[dict]:
    """Load existing author feeds from files.
    
    These will be in the form of "feed_*.json".
    """
    list_filenames = os.listdir(search_file_directory)
    feed_filenames = [
        filename for filename in list_filenames
        if filename.startswith("feed_")
        and filename.endswith(".json")
    ]
    author_feeds = []
    for filename in feed_filenames:
        full_fp = os.path.join(search_file_directory, filename)
        with open(full_fp, "r") as f:
            author_feed = json.load(f)
            author_feeds.append(author_feed)
    print(f"Loaded {len(author_feeds)} author feeds.")
    return author_feeds


def get_posts_by_author():
    """Get posts by author."""
    pass


def get_posts_by_hashtag():
    """Get posts by hashtag."""
    pass
