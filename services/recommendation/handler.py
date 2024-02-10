"""Service for ranking."""
from services.recommendation.helper import main as generate_recommendations


if __name__ == "__main__":
    event = {"algorithm": "reverse_chronological"}
    context = {}
    generate_recommendations(algorithm=event["algorithm"])
