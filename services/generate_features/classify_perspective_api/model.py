"""Model for the classify_perspective_api service."""


def classify(post: dict) -> dict:
    """Classifies a post using the Perspective API."""
    return {
        "toxicity": 0.0,
        "severe_toxicity": 0.0,
        "identity_attack": 0.0,
        "insult": 0.0,
        "profanity": 0.0,
        "threat": 0.0,
        "sexually_explicit": 0.0,
        "flirtation": 0.0
    }
