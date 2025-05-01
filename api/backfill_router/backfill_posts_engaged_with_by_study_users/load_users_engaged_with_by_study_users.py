"""Load users whose posts were either liked or reposted or replied to by
someone in the study.
"""

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel

service = "raw_sync"


def get_details_of_posts_liked_by_study_users(
    user_dids: set[str],
) -> tuple[set[str], set[str]]:
    """Get the details of posts that were liked by study users.

    Loads likes and then saves the URI of the liked posts as well as the
    DID of the users who wrote those posts that were liked.
    """
    custom_args = {"record_type": "like"}
    print("Loading likes data...")
    active_df = load_data_from_local_storage(
        service=service,
        directory="active",
        custom_args=custom_args,
    )
    cache_df = load_data_from_local_storage(
        service=service,
        directory="cache",
        custom_args=custom_args,
    )
    df = pd.concat([active_df, cache_df])
    df = df[df["author"].isin(user_dids)]
    dids: set[str] = set(df["author"].unique())
    uris: set[str] = set(df["uri"].unique())

    print(f"(Data type: likes) Found {len(dids)} DIDs and {len(uris)} URIs")

    return dids, uris


def get_details_of_posts_reposted_by_study_users(
    user_dids: set[str],
) -> tuple[set[str], set[str]]:
    """Get the details of posts that were reposted by study users.

    Loads reposts and then saves the URI of the reposted posts as well as the
    DID of the users who wrote those posts that were reposted.
    """
    custom_args = {"record_type": "repost"}
    print("Loading reposts data...")
    active_df = load_data_from_local_storage(
        service=service,
        directory="active",
        custom_args=custom_args,
    )
    cache_df = load_data_from_local_storage(
        service=service,
        directory="cache",
        custom_args=custom_args,
    )
    df = pd.concat([active_df, cache_df])
    df = df[df["author"].isin(user_dids)]
    dids: set[str] = set(df["author"].unique())
    uris: set[str] = set(df["uri"].unique())

    print(f"(Data type: reposts) Found {len(dids)} DIDs and {len(uris)} URIs")

    return dids, uris


def get_details_of_posts_replied_to_by_study_users(
    user_dids: set[str],
) -> tuple[set[str], set[str]]:
    """Get the details of posts that were replied to by study users.

    Loads replies and then saves the (1) URIs of the posts in the parent
    and root posts in the thread, and (2) the DIDs of the users who wrote
    those posts.
    """
    custom_args = {"record_type": "reply"}
    print("Loading replies data...")
    active_df = load_data_from_local_storage(
        service=service,
        directory="active",
        custom_args=custom_args,
    )
    cache_df = load_data_from_local_storage(
        service=service,
        directory="cache",
        custom_args=custom_args,
    )
    df = pd.concat([active_df, cache_df])
    df = df[df["author"].isin(user_dids)]
    dids: set[str] = set(df["author"].unique())
    uris: set[str] = set(df["uri"].unique())

    print(f"(Data type: replies) Found {len(dids)} DIDs and {len(uris)} URIs")

    return dids, uris


def main():
    users: list[UserToBlueskyProfileModel] = get_all_users()
    users: list[dict] = [user.model_dump() for user in users]
    user_dids: set[str] = set([user["bluesky_user_did"] for user in users])

    dids_of_liked_posts, uris_of_liked_posts = (
        get_details_of_posts_liked_by_study_users(
            user_dids=user_dids,
        )
    )
    dids_of_reposted_posts, uris_of_reposted_posts = (
        get_details_of_posts_reposted_by_study_users(
            user_dids=user_dids,
        )
    )
    dids_of_replied_posts, uris_of_replied_posts = (
        get_details_of_posts_replied_to_by_study_users(
            user_dids=user_dids,
        )
    )

    dids_of_users_engaged_with = (
        dids_of_liked_posts | dids_of_reposted_posts | dids_of_replied_posts
    )
    uris_of_posts_engaged_with = (
        uris_of_liked_posts | uris_of_reposted_posts | uris_of_replied_posts
    )

    print(f"Total number of users engaged with: {len(dids_of_users_engaged_with)}")
    print(f"Total number of posts engaged with: {len(uris_of_posts_engaged_with)}")

    # TODO: export these to SQLite.
    pass

    breakpoint()


if __name__ == "__main__":
    main()
