"""Manage ranking and recommendation service. """
from services.recommendation.helper import main as generate_recommendations


def main(event: dict, context: dict) -> int:
    """Manage ranking and recommendation service. """
    generate_recommendations(algorithm=event["algorithm"])
    return 0

if __name__ == "__main__":
    event = {"algorithm": "reverse_chronological"}
    context = {}
    main(event=event, context=context)
