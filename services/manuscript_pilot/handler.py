"""Manage ranking of posts. """
from services.manuscript_pilot.helper import main as rank_posts


def main(event: dict, context: dict) -> int:
    """Manage ranking and recommendation service. """
    rank_posts(algorithm=event["algorithm"])
    return 0


if __name__ == "__main__":
    event = {"algorithm": "reverse_chronological"}
    context = {}
    main(event=event, context=context)
