import json
import os
import requests

from lib.helper import client
#import sync.search.helper

user_profiles_filename = "user_profiles.jsonl"
current_file_directory = os.path.dirname(os.path.abspath(__file__))
user_profiles_fp = os.path.join(current_file_directory, user_profiles_filename)


def write_user_profiles_to_file(user_profiles: list[dict]) -> None:
    """Write user profiles to a file."""
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


def main(event: dict, context: dict) -> int:
    """Fetches data from the Bluesky API and stores it in the database."""

    if event["sync_sample_user_profiles"]:
        print("Syncing sample user profiles...")
        # fetch some sample user profiles
        user_profiles: list[dict] = sync_did_user_profiles()
    else:
        print("Loading existing user profiles from file...")
        user_profiles: list[dict] = load_user_profiles_from_file()

    return 0


if __name__ == "__main__":
    event = {
        "sync_sample_user_profiles": True
    }
    context = {}
    main(event=event, context=context)
