from lib.helper import track_function_runtime
from services.sync.search.helper import (
    sync_did_user_profiles,
    load_user_profiles_from_file,
    export_author_feeds,
    load_author_feeds_from_file
)


@track_function_runtime
def main(event: dict, context: dict) -> int:
    """Fetches data from the Bluesky API and stores it in the database."""

    if event["sync_sample_user_profiles"]:
        print("Syncing sample user profiles...")
        # fetch some sample user profiles
        user_profiles: list[dict] = sync_did_user_profiles()
    else:
        print("Loading existing user profiles from file...")
        user_profiles: list[dict] = load_user_profiles_from_file()

    if event["export_author_feeds"]:
        print("Exporting author feeds...")
        # export the author feed for each user profile
        export_author_feeds(
            user_profiles=user_profiles,
            count=event["total_author_feeds_to_export"]
        )

    if event["load_author_feeds"]:
        # load author feed from file
        print("Loading author feeds from file...")
        author_feeds: list[dict] = load_author_feeds_from_file()

    return 0


# TODO: implement deduplication of feeds? Should include a functionality for
# if we want to sync new feeds or if we want to resync old ones (or both).
if __name__ == "__main__":
    event = {
        "sync_sample_user_profiles": True,
        "total_author_feeds_to_export": 20,
        "export_author_feeds": True,
        "load_author_feeds": False
    }
    context = {}
    main(event=event, context=context)
