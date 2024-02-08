from lib.helper import track_function_runtime


@track_function_runtime
def main(event: dict, context: dict) -> int:
    """Fetches data from the Bluesky API and stores it in the database."""
    return 0


# NOTE: implement deduplication of feeds? Should include a functionality for
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
