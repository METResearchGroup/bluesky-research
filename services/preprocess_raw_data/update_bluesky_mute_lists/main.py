"""Updates our database with the list of users who are on the mute lists
that we are tracking, so that we can filter out users who are on these lists.
"""
from services.preprocess_raw_data.update_bluesky_mute_lists.helper import (
    sync_users_from_mute_lists
)

def main() -> None:
    sync_users_from_mute_lists()


if __name__ == "__main__":
    main()
