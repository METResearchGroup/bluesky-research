import json
import os
import requests

from lib.helper import client
#import sync.search.helper

user_profiles_filename = "user_profiles.jsonl"
current_file_directory = os.path.dirname(os.path.abspath(__file__))
user_profiles_fp = os.path.join(current_file_directory, user_profiles_filename)


def write_user_profiles_to_file(user_profiles: list[dict]) -> None:
    """Write user profiles to a file.
    
    TODO: if we want more users, we can add pagination.
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
    feed = client.get_author_feed(actor=user_profile["did"])
    return feed.dict()


def export_author_feed(user_profile) -> None:
    """Export the author feed to a file."""
    feed: dict = get_author_feed(user_profile=user_profile)
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
    list_filenames = os.listdir(current_file_directory)
    feed_filenames = [
        filename for filename in list_filenames
        if filename.startswith("feed_")
        and filename.endswith(".json")
    ]
    author_feeds = []
    for filename in feed_filenames:
        with open(filename, "r") as f:
            author_feed = json.load(f)
            author_feeds.append(author_feed)
    print(f"Loaded {len(author_feeds)} author feeds.")
    return author_feeds


def main(event: dict, context: dict) -> int:
    """Fetches data from the Bluesky API and stores it in the database."""

    if event["sync_sample_user_profiles"]:
        print("Syncing sample user profiles...")
        # fetch some sample user profiles
        user_profiles: list[dict] = sync_did_user_profiles()
    else:
        print("Loading existing user profiles from file...")
        user_profiles: list[dict] = load_user_profiles_from_file()

    if event["export_author_feeds"]:
        print("Exporting author feeds...")
        # export the author feed for each user profile
        export_author_feeds(
            user_profiles=user_profiles,
            count=event["total_author_feeds_to_export"]
        )

    if event["load_author_feeds"]:
        # load author feed from file
        print("Loading author feeds from file...")
        author_feeds: list[dict] = load_author_feeds_from_file()

    breakpoint()

    return 0


if __name__ == "__main__":
    event = {
        "sync_sample_user_profiles": False,
        "total_author_feeds_to_export": 1,
        "export_author_feeds": False,
        "load_author_feeds": True
    }
    context = {}
    main(event=event, context=context)
