from services.sync.most_liked_posts.helper import main as get_most_liked_posts


def get_posts() -> None:
    print("Getting posts from the most liked feed.")
    args = {
        "use_latest_local": False,
        "store_local": True,
        "store_remote": True,
        "bulk_write_remote": True,
        "feeds": ["today"]
    }
    get_most_liked_posts(**args)
