import os

from atproto_client.models.app.bsky.actor.defs import ProfileViewDetailed
import polars as pl

from lib.aws.s3 import S3
from services.sync.search.search_by_user import get_profile_of_user
from services.transform.transform_raw_data import flatten_user_profile

USERS_KEY_ROOT = "users"

s3_client = S3()


def get_user_bluesky_profile(actor: str) -> dict:
    """Get the preliminary study data (e.g., demographics, screening questions
    etc) and the user's profile from Bluesky."""
    if not actor:
        print("No actor provided. Exiting...")
        raise ValueError("Must provide an actor handle or DID.")
    author_did = actor if actor.startswith("did:") else None
    author_handle = actor if not author_did else None
    if not author_did or not author_handle:
        raise ValueError("Invalid actor handle or DID")
    bluesky_user_profile: ProfileViewDetailed = get_profile_of_user(
        author_handle=author_handle, author_did=author_did
    )
    bluesky_user_profile: dict = flatten_user_profile(
        bluesky_user_profile
    )
    return bluesky_user_profile


def insert_new_user_to_s3(
    user_did: str, study_data: dict, bluesky_user_profile: dict
) -> None:
    """Inserts new users into S3.

    We will identify users by their DID.

    Takes as input a dictionary of the following format:
    {
        "user_did": DID of the user (from Bluesky)
        "study_data": (dict) study data (e.g., preliminary screening questions,
        demographics, etc.) of the user
        "bluesky_user_profile": (dict) user profile from Bluesky
    }
    """
    base_key = os.path.join(USERS_KEY_ROOT, user_did)

    # check that base key doesn't already exist
    if s3_client.check_if_prefix_exists(prefix=base_key):
        print(f"User {user_did} already exists in S3. Exiting...")
        return

    study_data_key = os.path.join(base_key, "study_data.json")
    s3_client.write_dict_json_to_s3(study_data, study_data_key)

    bluesky_data_key = os.path.join(base_key, "bluesky_user_profile.json")
    s3_client.write_dict_json_to_s3(bluesky_user_profile, bluesky_data_key)


def create_and_insert_new_user(user_info: dict) -> None:
    """Creates a new user, fetches their Bluesky profile and inserts the
    user's data and Bluesky profile into S3."""
    bluesky_profile = get_user_bluesky_profile(user_info["actor"])
    user_did = bluesky_profile["did"]
    insert_new_user_to_s3(
        user_did=user_did,
        study_data=user_info,
        bluesky_user_profile=bluesky_profile
    )
    print(f"User {user_did} created and inserted into S3.")


def create_and_insert_new_users(filepath: str) -> None:
    """Creates and inserts multiple new users into S3.

    This assumes that whatever onboarding procedure we use (e.g., prescreening
    questions), that those will be saved in an Excel sheet that can be then
    converted to .csv. This .csv file can then be passed in here to create
    and insert new users into S3.

    Requires the following fields in the .csv file specified in `csv_data`:
    - Demographic data
    - actor: Bluesky handle or DID of participant

    Assumes that each row is a new user. The only mandatory specified field
    is "actor", since this is used to fetch the user's profile from Bluesky.
    """
    df: pl.DataFrame = pl.read_csv(filepath)
    users = df.to_dicts()
    for user in users:
        create_and_insert_new_user(user)
    print(f"Successfully added {len(users)} new users to S3")
