"""Onboards the list of users to the network.

Assumes that everyone who is being added has some follows/followers.
"""

from lib.aws.athena import Athena
from lib.log.logger import get_logger
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel
from pipelines.get_existing_user_social_network.src.get_existing_user_social_network.helper import (  # noqa
    export_follows_and_followers_for_user,
    fetch_follows_for_user,
    get_users_whose_social_network_has_been_fetched,
)

athena = Athena()
logger = get_logger(__name__)

# we set an artificial cap on the # of follows that we want to initialize per
# person, to make it easier to onboard all the new users.
# NOTE: most people have well below this number of follows.
# NOTE: if we want to add more, we can do this on a case-by-case basis for
# individual accounts. It'll just mean resyncing the follows for the account,
# which is OK.
max_follows_to_track = 200
max_requests = max_follows_to_track // 100  # 100 per request.
# NOTE: we don't care about their existing followers (we will just track any
# new followers they get).
max_followers_to_track = 0


def main(users_to_sync: list[str] = None):
    """Syncs social networks for users.

    Optionally can pass in the list of Bluesky handles to sync.
    """
    users: list[UserToBlueskyProfileModel] = get_all_users()
    if users_to_sync:
        users_not_synced: list[UserToBlueskyProfileModel] = [
            user for user in users if user.bluesky_handle in users_to_sync
        ]
    else:
        users_whose_social_network_has_been_fetched: set[str] = (
            get_users_whose_social_network_has_been_fetched()
        )  # noqa
        # we assume that any users who have connections in the study have
        # already been synced.
        users_not_synced: list[UserToBlueskyProfileModel] = [
            user
            for user in users
            if user.bluesky_handle not in users_whose_social_network_has_been_fetched
        ]
    total_users_to_sync = len(users_not_synced)
    if total_users_to_sync == 0:
        logger.info("No users to sync")
        return
    logger.info(f"Syncing follows for {total_users_to_sync} users.")
    user_bsky_handles_to_sync = "\n".join(
        [user.bluesky_handle for user in users_not_synced]
    )
    logger.info(f"Users to sync: {user_bsky_handles_to_sync}")
    for i, user in enumerate(users_not_synced):
        if i % 10 == 0:
            logger.info(f"Syncing user {i} of {total_users_to_sync}")
        logger.info(f"{user.bluesky_handle} - {user.bluesky_user_did}")
        try:
            follows: list[dict] = fetch_follows_for_user(
                user_handle=user.bluesky_handle,
                user_did=user.bluesky_user_did,
                max_requests=max_requests,
            )
        except Exception as e:
            logger.error(f"Failed to fetch follows for {user.bluesky_handle}: {e}")
            follows = []
            continue
        # NOTE: we don't care about existing followers. We just care about
        # new ones. User feeds are not affected by the presence or lack of
        # follower accounts; they're only affected by who the user follows.
        followers: list[dict] = []
        export_follows_and_followers_for_user(
            user_handle=user.bluesky_handle,
            follows=follows,
            followers=followers,
        )
        logger.info(f"Exported follows for {user.bluesky_handle}")


if __name__ == "__main__":
    # users_to_sync = ["percy.bsky.social"]
    # main(users_to_sync=users_to_sync)
    users_to_sync = []
    main(users_to_sync)
