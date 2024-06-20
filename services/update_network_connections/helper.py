from typing import Optional

from atproto_client.models.app.bsky.actor.defs import ProfileView, ProfileViewDetailed  # noqa

from lib.constants import current_datetime_str
from lib.db.sql.network_connections_database import (
    batch_insert_network_connections, get_connection_dids_for_user,
    get_user_network_counts, insert_user_network_counts
)
from lib.db.sql.participant_data_database import get_user_to_bluesky_profiles
from lib.helper import client
from services.participant_data.models import UserToBlueskyProfileModel
from services.sync.search.helper import send_request_with_pagination
from services.update_network_connections.models import (
    ConnectionModel, UserSocialNetworkCountsModel, UserToConnectionModel
)


def get_follows_for_user(
    user: UserToBlueskyProfileModel, limit: Optional[int] = None
) -> dict[str, ConnectionModel]:
    """Get the follows for a user.

    Since follows are sorted by most recent, we can use the limit to get
    the most recent follows.
    """
    params = {"actor": user.bluesky_user_did}
    follows: list[ProfileView] = send_request_with_pagination(
        func=client.app.bsky.graph.get_follows,
        kwargs={"params": params},
        update_params_directly=True,
        response_key="records",
        limit=limit,
    )
    follow_to_model_dict = {
        follow.did: ConnectionModel(
            connection_did=follow.did,
            connection_handle=follow.handle,
            connection_display_name=follow.display_name,
            connection_type="follow",
            synctimestamp=current_datetime_str,
        )
        for follow in follows
    }
    return follow_to_model_dict


def get_followers_for_user(
    user: UserToBlueskyProfileModel, limit: Optional[int] = None
) -> dict[str, ConnectionModel]:
    """Get the followers for a user.

    Since followers are sorted by most recent, we can use the limit to get
    the most recent followers.
    """
    params = {"actor": user.bluesky_user_did}
    followers: list[ProfileView] = send_request_with_pagination(
        func=client.app.bsky.graph.get_followers,
        kwargs={"params": params},
        update_params_directly=True,
        response_key="records",
        limit=limit,
    )
    follower_to_model_dict: list[ConnectionModel] = {
        follower.did: ConnectionModel(
            connection_did=follower.did,
            connection_handle=follower.handle,
            connection_display_name=follower.display_name,
            connection_type="follower",
            synctimestamp=current_datetime_str,
        )
        for follower in followers
    }
    return follower_to_model_dict


def consolidate_follows_followers_for_user(
    user: UserToBlueskyProfileModel,
    follow_to_model_dict: dict[str, ConnectionModel],
    follower_to_model_dict: dict[str, ConnectionModel],
    existing_connections_dids: set[str]
) -> list[UserToConnectionModel]:
    """Consolidate the lists of follows and followers into a single model
    to encompass them both.

    Accounts for cases where there's only either new follows, only new
    followers, or both. Also accounts for cases where the user is already
    following someone who follows them back, making the connection bidirectional.
    """  # noqa
    if follow_to_model_dict and follower_to_model_dict:
        mutual_dids: set[str] = follow_to_model_dict.keys() & follower_to_model_dict.keys()  # noqa
    else:
        mutual_dids = set()

    for did in existing_connections_dids:
        # for connections that already exist in the database, we need to
        # make those connections bidirectional.
        mutual_dids.add(did)

    user_to_connection_map = dict[str, UserToConnectionModel] = {}

    # process only if there are new follows
    if follow_to_model_dict:
        for did, follow in follow_to_model_dict.items():
            user_follows_connection = True
            if did in mutual_dids:
                connection_follows_user = True
            else:
                connection_follows_user = False
            user_connection_model = UserToConnectionModel(
                study_user_id=user.study_user_id,
                user_did=user.bluesky_user_did,
                user_handle=user.bluesky_handle,
                connection_did=follow.connection_did,
                connection_handle=follow.connection_handle,
                connection_display_name=follow.connection_display_name,
                user_follows_connection=user_follows_connection,
                connection_follows_user=connection_follows_user,
                synctimestamp=follow.synctimestamp,
            )
            user_to_connection_map[did] = user_connection_model

    # process only if there are new followers
    if follower_to_model_dict:
        for did, follower in follower_to_model_dict.items():
            if did in user_to_connection_map:
                # skip if already processed above.
                continue
            connection_follows_user = True
            if did in mutual_dids:
                user_follows_connection = True
            else:
                user_follows_connection = False
            user_connection_model = UserToConnectionModel(
                study_user_id=user.study_user_id,
                user_did=user.bluesky_user_did,
                user_handle=user.bluesky_handle,
                connection_did=follower.connection_did,
                connection_handle=follower.connection_handle,
                connection_display_name=follower.connection_display_name,
                user_follows_connection=user_follows_connection,
                connection_follows_user=connection_follows_user,
                synctimestamp=follower.synctimestamp,
            )
            user_to_connection_map[did] = user_connection_model
    user_connections = list(user_to_connection_map.values())
    return user_connections


# TODO: check, because we're assuming that the existing connections are
# unidirectional, but we should also account for bidirectional connections.
# TODO: we need to make sure that we're not overriding past connections.
# TODO: we should probably probably have a dictionary that maps user_did to
# the set of connection_dids for each direction (follows/followers/both).
def check_for_existing_network_connections(
    user: UserToBlueskyProfileModel, possible_new_connection_dids: set[str]
) -> set[str]:
    """Check if any of the new follows/followers already exist in the user's
    network connections. For example, if a user is already following someone
    and that account follows the user back, we want to update the connection.

    Should only be for cases where the existing link is unidirectional and we
    need to make it bidirectional.
    """
    # load all connections for the user
    user_did = user.bluesky_user_did
    connection_dids: set[str] = get_connection_dids_for_user(user_did=user_did)
    return connection_dids & possible_new_connection_dids


def get_current_network_counts(user: UserToBlueskyProfileModel) -> dict[str, int]:
    """Get the current network counts for a user."""
    user_profile: ProfileViewDetailed = client.get_profile(actor=user.bluesky_user_did)  # noqa
    return {
        "follows": user_profile.follows_count,
        "followers": user_profile.followers_count,
    }


def get_existing_network_counts() -> dict:
    """Get the existing network counts for a list of users, keyed on user DID."""  # noqa
    user_network_counts: list[UserSocialNetworkCountsModel] = get_user_network_counts()  # noqa
    res = {}

    for user_network_count in user_network_counts:
        res[user_network_count.user_did] = {
            "follows": user_network_count.user_following_count,
            "followers": user_network_count.user_followers_count,
        }
    return res


def update_latest_network_counts(
    user: UserToBlueskyProfileModel, existing_network_counts_map: dict
) -> dict:
    """Updates the latest follows/follower counts for a user.

    Returns a dictionary with the difference in counts for follows/followers.
    """
    previous_user_network_counts_map = existing_network_counts_map.get(user.bluesky_user_did, {})
    latest_user_network_counts_map = get_current_network_counts(user=user)

    latest_follows_count = latest_user_network_counts_map["follows"]
    latest_followers_count = latest_user_network_counts_map["followers"]

    if user.bluesky_user_did not in existing_network_counts_map:
        print(f"User {user.bluesky_user_did} not found in existing network counts. Adding...")  # noqa
        follows_difference = latest_follows_count
        followers_difference = latest_followers_count
    else:
        previous_follows_count = previous_user_network_counts_map["follows"]
        previous_followers_count = previous_user_network_counts_map["followers"]

        if (
            previous_follows_count == latest_follows_count
            and previous_followers_count == latest_followers_count
        ):
            print(f"User {user.bluesky_user_did} has the same follow/follower counts. No need to reprocess...")  # noqa
            return {
                "follows_difference": 0,
                "followers_difference": 0,
            }
        else:
            # doesn't account for decreasing counts, where a user might have
            # fewer followers/follows. We can add that in later, since it takes
            # too much computation to reprocess the list of follows/followers
            # to find whoever unfollowed. We treat negative counts like we do
            # zeros and we skip computation.
            follows_difference = latest_follows_count - previous_follows_count
            followers_difference = latest_followers_count - previous_followers_count  # noqa

    user_social_network_counts_model = UserSocialNetworkCountsModel(
        study_user_id=user.study_user_id,
        user_did=user.bluesky_user_did,
        user_handle=user.bluesky_handle,
        user_followers_count=latest_followers_count,
        user_following_count=latest_follows_count,
        synctimestamp=current_datetime_str,
    )
    insert_user_network_counts(user_social_network_counts_model)

    return {
        "follows_difference": follows_difference,
        "followers_difference": followers_difference,
    }


def update_network_connections_for_user(
    user: UserToBlueskyProfileModel,
    total_new_follows: int,
    total_new_followers: int
) -> list[UserToConnectionModel]:
    """Updates the network connections for a given user."""
    if total_new_follows > 0:
        follow_to_model_dict = get_follows_for_user(user=user, limit=total_new_follows)  # noqa
    else:
        follow_to_model_dict = {}

    if total_new_followers > 0:
        follower_to_model_dict = get_followers_for_user(user=user, limit=total_new_followers)  # noqa
    else:
        follower_to_model_dict = {}

    if not follow_to_model_dict and not follower_to_model_dict:
        print(f"No new follows/followers for user {user.bluesky_user_did}. Skipping...")  # noqa
        return

    existing_connection_dids = check_for_existing_network_connections(
        user=user,
        possible_new_connection_dids=set(
            follow_to_model_dict.keys() | follower_to_model_dict.keys()
        )
    )

    total_follows_followers_for_user: list[UserToConnectionModel] = consolidate_follows_followers_for_user(  # noqa
        user=user,
        follow_to_model_dict=follow_to_model_dict,
        follower_to_model_dict=follower_to_model_dict,
        existing_connections_dids=existing_connection_dids
    )

    return total_follows_followers_for_user


def update_network_connections():
    """This updates our database with the follows/followers for a given set of
    users in the study."""
    users: list[UserToBlueskyProfileModel] = get_user_to_bluesky_profiles()
    existing_network_counts_map: dict[str, dict[str, int]] = get_existing_network_counts()  # noqa
    for user in users:
        print(f"--> Updating network connections for user: {user.user_id} ...")
        network_count_diffs = update_latest_network_counts(
            user=user, existing_network_counts_map=existing_network_counts_map
        )
        total_new_follows = network_count_diffs["follows_difference"]
        total_new_followers = network_count_diffs["followers_difference"]
        if total_new_follows <= 0 and total_new_followers <= 0:
            # no need to update network connections if the follow/follower
            # counts haven't changed.
            print(f"Skipping user {user.bluesky_user_did} since follow and follower counts are the same.")  # noqa
            continue
        updated_network_connections: list[UserToConnectionModel] = (
            update_network_connections_for_user(
                user=user,
                total_new_follows=total_new_follows,
                total_new_followers=total_new_followers
            )
        )
        batch_insert_network_connections(updated_network_connections)
        print(f"--> Done updating network connections for user: {user.user_id}")  # noqa
