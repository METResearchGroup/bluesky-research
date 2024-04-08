"""Helper functions for updating the mute lists."""
from lib.helper import client

SOURCE_MUTE_LISTS = []


def sync_users_from_mute_list() -> list[str]:
    """Syncs list of users from a mute list."""
    # load from client
    pass


def update_mute_list(user_did: str) -> None:
    """Updates the mute list with a user's DID."""
    pass


def save_mute_list() -> None:
    """Saves the mute list."""
    pass


def check_if_user_in_mute_list(user_did: str) -> bool:
    """Checks to see if a user is in a mute list.

    Uses a bloom filter to scale up the number of users that can be checked.
    """
    pass


def sync_mute_list() -> None:
    """Syncs our store of users to mute by fetching muted users from
    the lists in Bluesky.

    To be updated on a schedule.
    """
    pass