"""Helper tooling for fetching a user's existing social network."""

import os
from typing import Literal
import uuid

from boto3.dynamodb.types import TypeSerializer

from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.constants import current_datetime_str
from lib.helper import client
from lib.log.logger import get_logger
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel
from services.sync.search.helper import send_request_with_pagination

logger = get_logger(__name__)

s3 = S3()
dynamodb = DynamoDB()
key_root = "scraped-user-social-network"  # should match TF configuration.
dynamodb_table_name = "users_whose_social_network_has_been_fetched"

serializer = TypeSerializer()

max_requests_per_url = 5  # max 500 results, 100 per request.


def fetch_profiles(
    user_handle: str, network_type: Literal["follows", "followers"]
) -> list[dict]:
    """Given a url (follow/follower list), fetches the profiles.

    Returns a list of dicts, in the form:
    {
        "handle": <the handle of the user>,
        "profile_url": <the URL for the profile>
    }
    """
    if network_type == "follows":
        res = send_request_with_pagination(
            func=client.get_follows,
            kwargs={"actor": user_handle},
            response_key="follows",
            max_requests=max_requests_per_url,
        )
        print(f"Returning {len(res)} follows for {user_handle}.")
    elif network_type == "followers":
        res = send_request_with_pagination(
            func=client.get_followers,
            kwargs={"actor": user_handle},
            response_key="followers",
            max_requests=max_requests_per_url,
        )
        print(f"Returning {len(res)} followers for {user_handle}.")
    else:
        raise ValueError(f"Invalid network type: {network_type}")

    output: list[dict] = [
        {
            "handle": profile.handle,
            "did": profile.did,
            "profile_url": f"https://bsky.app/profile/{profile.handle}",
        }
        for profile in res
    ]
    return output


def fetch_follows_for_user(user_handle: str, user_did: str):
    profiles: list[dict] = fetch_profiles(
        user_handle=user_handle, network_type="follows"
    )
    follows = [
        {
            "follow_handle": profile["handle"],
            "follow_url": profile["profile_url"],
            "follow_did": profile["did"],
            "follower_handle": user_handle,
            "follower_url": f"https://bsky.app/profile/{user_handle}",
            "follower_did": user_did,
            "insert_timestamp": current_datetime_str,
            "relationship_to_study_user": "follow",  # equivalent to 'followee'
        }
        for profile in profiles
    ]
    return follows


def fetch_followers_for_user(user_handle: str, user_did: str):
    profiles: list[dict] = fetch_profiles(
        user_handle=user_handle, network_type="followers"
    )
    followers = [
        {
            "follower_handle": profile["handle"],
            "follower_url": profile["profile_url"],
            "follower_did": profile["did"],
            "follow_handle": user_handle,
            "follow_url": f"https://bsky.app/profile/{user_handle}",
            "follow_did": user_did,
            "insert_timestamp": current_datetime_str,
            "relationship_to_study_user": "follower",
        }
        for profile in profiles
    ]
    return followers


def fetch_follows_and_followers_for_user(user_handle: str, user_did: str):
    follows = fetch_follows_for_user(user_handle, user_did)
    followers = fetch_followers_for_user(user_handle, user_did)
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


def get_users_whose_social_network_has_been_fetched() -> set[str]:
    """Gets the list of users whose social network has been fetched.

    Retrieves from DynamoDB.
    """
    res = dynamodb.get_all_items_from_table(dynamodb_table_name)
    user_handles_res = [item["user_handle"] for item in res]
    user_handles = set([item["S"] for item in user_handles_res])
    return user_handles


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


def get_social_network_for_user(user_handle: str, user_did: str):
    """Given a user handle, fetches the user's social network."""
    follows, followers = fetch_follows_and_followers_for_user(
        user_handle=user_handle, user_did=user_did
    )
    export_follows_and_followers_for_user(
        user_handle=user_handle, follows=follows, followers=followers
    )
    logger.info(
        f"Exported {len(follows)} follows and {len(followers)} followers for user {user_handle}."
    )  # noqa


def get_social_networks_for_users(study_users: list[UserToBlueskyProfileModel]):  # noqa
    """Given a list of user handles, fetches the social networks for each user."""
    users_whose_social_network_has_been_fetched: set[str] = (
        get_users_whose_social_network_has_been_fetched()
    )
    logger.info(
        f"Number of users whose social network has been fetched: {users_whose_social_network_has_been_fetched}"
    )
    users = [
        user
        for user in study_users
        if user.bluesky_handle not in users_whose_social_network_has_been_fetched
    ]
    if len(users) == 0:
        logger.info("No users to fetch social networks for.")
        return
    total_users = len(users)
    logger.info(f"Fetching social networks for {total_users} users.")
    for i, user in enumerate(users):
        if i % 10 == 0:
            logger.info(
                f"Fetching social network for user {i+1}/{total_users}: {user.bluesky_handle}"
            )  # noqa
        get_social_network_for_user(
            user_handle=user.bluesky_handle, user_did=user.bluesky_user_did
        )
    logger.info(f"Finished fetching social networks for {total_users} users.")


def main():
    study_users: list[UserToBlueskyProfileModel] = get_all_users()
    get_social_networks_for_users(study_users)


if __name__ == "__main__":
    main()
