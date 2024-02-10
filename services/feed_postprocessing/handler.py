"""Service for managing feed postprocessing."""
from services.feed_postprocessing.helper import do_feed_postprocessing


def main(event: dict, context: dict) -> int:
    """Main function for managing feed postprocessing."""
    do_feed_postprocessing()
    return 0


if __name__ == "__main__":
    event = {}
    context = {}
    main(event=event, context=context)
