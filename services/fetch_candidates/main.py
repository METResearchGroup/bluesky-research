"""Generates candidates."""
from services.fetch_candidates.helper import fetch_candidates

def main() -> dict[str, list[dict]]:
    """Fetches candidates per user.
    
    Key = user, value = list of posts for the user.
    """
    return fetch_candidates()
