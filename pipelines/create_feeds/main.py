"""Pipeline logic for creating the latest feeds."""
from services.create_feeds.helper import create_latest_feeds


def main() -> None:
    create_latest_feeds()


if __name__ == "__main__":
    print("Creating latest feeds...")
    main()
    print("Feeds created successfully.")
