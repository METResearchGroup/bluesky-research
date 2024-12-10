"""Pipeline logic for creating the latest feeds."""

from services.create_feeds.main import main as create_latest_feeds


def main(payload: dict) -> None:
    create_latest_feeds(payload)


if __name__ == "__main__":
    print("Creating latest feeds...")
    payload: dict = {"reverse_chronological_only": True}
    main(payload=payload)
    print("Feeds created successfully.")
