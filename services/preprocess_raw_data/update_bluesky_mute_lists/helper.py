"""Helper functions for updating the mute lists."""
from services.preprocess_raw_data.update_bluesky_mute_lists.constants import MUTE_LIST_LINKS  # noqa
from services.preprocess_raw_data.update_bluesky_mute_lists.database import (
    batch_create_mute_lists, batch_create_muted_users
)
from transform.bluesky_helper import get_list_and_user_data_from_list_links


def save_list_and_user_data_to_db(
    lists: list[dict], users: list[dict]
) -> None:
    """Given a list of lists and a list of users, save them to the mute list
    database."""
    batch_create_mute_lists(lists)
    batch_create_muted_users(users)
    print("Saved list and user data to the database.")


def sync_users_from_mute_lists() -> None:
    """Syncs our store of users to mute by fetching muted users from
    the lists in Bluesky.

    To be updated on a schedule.
    """
    list_and_user_data: dict = get_list_and_user_data_from_list_links(
        list_urls=MUTE_LIST_LINKS
    )
    save_list_and_user_data_to_db(
        lists=list_and_user_data["lists"],
        users=list_and_user_data["users"]
    )
    print("Synced users from mute lists.")


def check_if_user_in_mute_list(user_did: str) -> bool:
    """Checks to see if a user is in a mute list.

    Uses a bloom filter to scale up the number of users that can be checked.
    """
    pass
