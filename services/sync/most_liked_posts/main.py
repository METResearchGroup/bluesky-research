"""Gets the most liked posts today and during the week and saves them to
the database."""
from services.sync.most_liked_posts.helper import get_latest_most_liked_posts


def main() -> None:
    posts: list[dict] = get_latest_most_liked_posts()


if __name__ == "__main__":
    main()
