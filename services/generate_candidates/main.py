"""Generates candidates."""
from services.generate_candidates.helper import generate_candidates

def main() -> dict[str, list[dict]]:
    """Generates candidates per user.
    
    Key = user, value = list of posts for the user.
    """
    return generate_candidates()
