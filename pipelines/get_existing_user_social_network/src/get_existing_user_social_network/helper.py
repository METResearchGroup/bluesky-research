"""Helper tooling for fetching a user's existing social network."""

import os
import uuid

from boto3.dynamodb.types import TypeSerializer

from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.constants import current_datetime_str
from lib.log.logger import get_logger
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel

logger = get_logger(__name__)

s3 = S3()
dynamodb = DynamoDB()
key_root = "scraped-user-social-network"  # should match TF configuration.
dynamodb_table_name = "users_whose_social_network_has_been_fetched"

serializer = TypeSerializer()


def fetch_profiles(url) -> list[dict]:
    """Given a url (follow/follower list), fetches the profiles.

    Returns a list of dicts, in the form:
    {
        "handle": <the handle of the user>,
        "profile_url": <the URL for the profile>
    }
    """
    return [
        {"handle": f"example_handle_{i}", "profile_url": f"example_profile_url_{i}"}
        for i in range(10)
    ]


def fetch_follows_for_user(user_handle: str):
    url = f"https://bsky.app/profile/{user_handle}/follows"
    profiles: list[dict] = fetch_profiles(url)
    follows = [
        {
            "follow_handle": profile["handle"],
            "follow_url": profile["profile_url"],
            "follower_handle": user_handle,
            "follower_url": f"https://bsky.app/profile/{user_handle}",
        }
        for profile in profiles
    ]
    return follows


def fetch_followers_for_user(user_handle: str):
    url = f"https://bsky.app/profile/{user_handle}/followers"
    profiles: list[dict] = fetch_profiles(url)
    followers = [
        {
            "follower_handle": profile["handle"],
            "follower_url": profile["profile_url"],
            "follow_handle": user_handle,
            "follow_url": f"https://bsky.app/profile/{user_handle}",
        }
        for profile in profiles
    ]
    return followers


def fetch_follows_and_followers_for_user(user_handle: str):
    follows = fetch_follows_for_user(user_handle)
    followers = fetch_followers_for_user(user_handle)
    return follows, followers


def update_completed_fetch_user_network(user_handle: str):
    """Once a user's social network has been fetched, update the completed
    user's status in DynamoDB."""
    payload = {"user_handle": user_handle, "insert_timestamp": current_datetime_str}
    serialized_payload = {k: serializer.serialize(v) for k, v in payload.items()}
    dynamodb.insert_item_into_table(
        item=serialized_payload, table_name=dynamodb_table_name
    )
    print(
        f"Updated DynamoDB table `{dynamodb_table_name}` by adding user {user_handle}."
    )  # noqa


def get_users_whose_social_network_has_been_fetched() -> list[str]:
    """Gets the list of users whose social network has been fetched.

    Retrieves from DynamoDB.
    """
    res = dynamodb.get_all_items_from_table(dynamodb_table_name)
    return [item["user_handle"] for item in res]


def export_follows_and_followers_for_user(
    user_handle: str, follows: list[dict], followers: list[dict]
):
    """Exports the follows and followers for a user to S3."""
    hashed_value = str(uuid.uuid4())
    follows_filename = f"follows_{user_handle}-{hashed_value}.json"
    followers_filename = f"followers_{user_handle}-{hashed_value}.json"
    follows_key = os.path.join(key_root, follows_filename)
    followers_key = os.path.join(key_root, followers_filename)
    s3.write_dicts_jsonl_to_s3(data=follows, key=follows_key)
    s3.write_dicts_jsonl_to_s3(data=followers, key=followers_key)
    update_completed_fetch_user_network(user_handle)


def get_social_network_for_user(user_handle: str):
    """Given a user handle, fetches the user's social network."""
    follows, followers = fetch_follows_and_followers_for_user(user_handle)
    export_follows_and_followers_for_user(user_handle, follows, followers)
    logger.info(
        f"Exported {len(follows)} follows and {len(followers)} followers for user {user_handle}."
    )  # noqa


def get_social_networks_for_users(user_handles: list[str]):
    """Given a list of user handles, fetches the social networks for each user."""
    users_whose_social_network_has_been_fetched = (
        get_users_whose_social_network_has_been_fetched()
    )
    logger.info(
        f"Number of users whose social network has been fetched: {users_whose_social_network_has_been_fetched}"
    )
    user_handles = [
        user_handle
        for user_handle in user_handles
        if user_handle not in users_whose_social_network_has_been_fetched
    ]
    logger.info(f"Fetching social networks for {len(user_handles)} users.")
    for i, user_handle in enumerate(user_handles):
        if i % 10 == 0:
            logger.info(
                f"Fetching social network for user {i+1}/{len(user_handles)}: {user_handle}"
            )  # noqa
        get_social_network_for_user(user_handle)
    logger.info(f"Finished fetching social networks for {len(user_handles)} users.")


def main():
    study_users: list[UserToBlueskyProfileModel] = get_all_users()
    user_handles = [user.bluesky_handle for user in study_users]
    get_social_networks_for_users(user_handles)


if __name__ == "__main__":
    main()
