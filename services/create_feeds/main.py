"""Service for creating feeds."""
from services.create_feeds.helper import create_latest_feeds

def main(payload: dict) -> None:
    kwargs = payload
    create_latest_feeds(**kwargs)


if __name__ == "__main__":
    main()
