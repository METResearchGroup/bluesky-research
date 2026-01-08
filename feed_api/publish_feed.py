"""Tooling for publishing a feed.

Copied from https://github.com/MarshalX/bluesky-feed-generator/blob/main/publish_feed.py
"""  # noqa

from lib.load_env_vars import EnvVarsContainer

from atproto import Client, models

# YOUR bluesky handle
# Ex: user.bsky.social
HANDLE: str = EnvVarsContainer.get_env_var("BLUESKY_HANDLE") or ""

# YOUR bluesky password, or preferably an App Password (found in your client settings) # noqa
# Ex: abcd-1234-efgh-5678
PASSWORD: str = EnvVarsContainer.get_env_var("BLUESKY_PASSWORD") or ""

# The hostname of the server where feed server will be hosted
# Ex: feed.bsky.dev
HOSTNAME: str = "mindtechnologylab.com"

# A short name for the record that will show in urls
# Lowercase with no spaces.
# Ex: whats-hot
RECORD_NAME: str = "nw-study-feed"

# A display name for your feed
# Ex: What's Hot
DISPLAY_NAME: str = "Bluesky Study Feed"

# (Optional) A description of your feed
# Ex: Top trending content from the whole network
DESCRIPTION: str = "Feed you were assigned for the study."

# (Optional) The path to an image to be used as your feed's avatar
# Ex: ./path/to/avatar.jpeg
AVATAR_PATH: str = "./nw_logo_round.png"
# (Optional). Only use this if you want a service did different from did:web
SERVICE_DID: str = ""


# -------------------------------------
# NO NEED TO TOUCH ANYTHING BELOW HERE
# -------------------------------------


def main():
    client = Client()
    client.login(HANDLE, PASSWORD)

    feed_did = SERVICE_DID
    if not feed_did:
        feed_did = f"did:web:{HOSTNAME}"

    avatar_blob = None
    if AVATAR_PATH:
        with open(AVATAR_PATH, "rb") as f:
            avatar_data = f.read()
            avatar_blob = client.upload_blob(avatar_data).blob

    response = client.com.atproto.repo.put_record(
        models.ComAtprotoRepoPutRecord.Data(  # noqa
            repo=client.me.did,
            collection=models.ids.AppBskyFeedGenerator,
            rkey=RECORD_NAME,
            record=models.AppBskyFeedGenerator.Record(
                did=feed_did,
                display_name=DISPLAY_NAME,
                description=DESCRIPTION,
                avatar=avatar_blob,
                created_at=client.get_current_time_iso(),
            ),
        )
    )

    print("Successfully published!")
    print('Feed URI (put in "WHATS_ALF_URI" env var):', response.uri)


if __name__ == "__main__":
    main()
