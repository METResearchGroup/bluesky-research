import json
import requests


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
    """  # noqa
    with requests.get("https://plc.directory/export") as r:
        r.raise_for_status()
        lines = r.text.split('\n')
        user_profiles = [json.loads(line) for line in lines if line.strip()]

    return user_profiles


def main():
    profiles = sync_did_user_profiles()
    print(f"Profiles: {profiles}")


if __name__ == "__main__":
    main()
