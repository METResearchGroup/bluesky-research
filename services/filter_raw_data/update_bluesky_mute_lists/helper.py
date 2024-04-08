"""Helper functions for updating the mute lists."""
from atproto_client.models.app.bsky.graph.defs import ListItemView, ListView
from atproto_client.models.app.bsky.graph.get_list import Response as GetListResponse

from lib.constants import current_datetime_str
from lib.helper import client
from services.filter_raw_data.update_bluesky_mute_lists.constants import MUTE_LIST_LINKS # noqa
from services.filter_raw_data.update_bluesky_mute_lists.database import (
    batch_create_mute_lists, batch_create_muted_users
)
from transform.bluesky_helper import get_author_did_from_handle


def get_user_info_from_list_item(list_item: ListItemView):
    """Given a list item view (from the list view), return a dictionary with
    the user's blocked user id (did) and their handle (username)"""
    return {"did": list_item.subject.did, "handle": list_item.subject.handle}


def get_users_added_to_list(list_items: list[ListItemView]) -> list[dict]:
    """Given a list of list items, return a list of dictionaries with the user's
    blocked user id (did) and their handle (username)"""
    return [get_user_info_from_list_item(item) for item in list_items]


def generate_list_uri_given_list_url(list_url: str) -> str:
    """Given a list url, return the corresponding list uri.
    
    Example:
    >> list_url = "https://bsky.app/profile/nickwrightdata.ntw.app/lists/3kmr32obinz2q"
    >> generate_list_uri_given_list_url(list_url)
    "at://did:plc:7allko6vtrpvyxxcd5beapou/app.bsky.graph.list/3kmr32obinz2q"
    """
    split_url = list_url.split("/")
    author_handle: str = split_url[-3]
    list_did: str = split_url[-1]

    author_did: str = get_author_did_from_handle(author_handle)
    list_uri = f"at://{author_did}/app.bsky.graph.list/{list_did}"
    return list_uri


def get_users_on_list_given_list_uri(list_uri: str) -> list[dict]:
    """Given a list uri, return a list of dictionaries with the user's
    blocked user id (did) and their handle (username)"""
    res: GetListResponse = client.app.bsky.graph.get_list(params={"list": list_uri})
    return get_users_added_to_list(res.items)


def get_users_in_list_given_list_url(list_url: str) -> list[dict]:
    """Given the URL to a list, return the users added to the list."""
    list_uri: str = generate_list_uri_given_list_url(list_url)
    return get_users_on_list_given_list_uri(list_uri)


def get_list_info_given_list_url(list_url: str) -> dict:
    """Given the URL of a list, get both the metadata for a list as well as
    the users on the list."""
    list_uri: str = generate_list_uri_given_list_url(list_url)
    res: GetListResponse = client.app.bsky.graph.get_list(params={"list": list_uri})
    list_metadata: dict = {
        "cid": res.list.cid,
        "name": res.list.name,
        "uri": res.list.uri,
        "description": res.list.description,
        "author_did": res.list.creator.did,
        "author_handle": res.list.creator.handle,
    }
    users: list[dict] = get_users_added_to_list(res.items)
    return {"list_metadata": list_metadata, "users": users}


def get_list_and_user_data_from_mute_list_links(list_urls: list[str]) -> dict:
    """Given a list of list URLs, return a list of dictionaries with the list
    metadata and the users on the list."""
    list_data_list = [get_list_info_given_list_url(url) for url in list_urls]
    lists_list = []
    users_list = []
    for list_data in list_data_list:
        # add the list metadata to the lists_list
        lists_list.append(list_data["list_metadata"])
        # for each user, get their data plus which list it cames from
        for user in list_data["users"]:
            user["source_list_uri"] = list_data["list_metadata"]["uri"]
            user["source_list_name"] = list_data["list_metadata"]["name"]
            user["timestamp_added"] = current_datetime_str
            users_list.append(user)

    # for each user in users_list, dedupe based on the "did" key. We only need
    # one entry per user.
    deduped_users_list = []
    seen_dids = set()
    for user in users_list:
        if user["did"] not in seen_dids:
            deduped_users_list.append(user)
            seen_dids.add(user["did"])

    return {"lists": lists_list, "users": deduped_users_list}


def save_list_and_user_data_to_db(lists: list[dict], users: list[dict]) -> None:
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
    list_and_user_data: dict = get_list_and_user_data_from_mute_list_links(
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
