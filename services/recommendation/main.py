"""Service for ranking."""
from services.ranking.reverse_chronological.main import main as reverse_chronological_feed # noqa


def main(event: dict, context: dict) -> int:
    algorithm = event["algorithm"]
    if algorithm == "reverse_chronological":
        feed = reverse_chronological_feed()
        print(feed)
    return 0


if __name__ == "__main__":
    event = {"algorithm": "reverse_chronological"}
    context = {}
    main(event=event, context=context)
