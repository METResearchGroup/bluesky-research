"""Helper functions for generating candidates."""
def get_users() -> list[str]:
    return []

def generate_candidates_for_user(
    user_id: str, num_candidates: int
) -> list[dict]:
    """Generate candidates for a given user.
    
    Pulls from vector database.
    """
    return []


def generate_candidates(num_candidates_per_user: int) -> dict[str, list[dict]]:
    """Generates candidates per user.
    """
    users: list[str] = get_users()
    return {
        user_id: generate_candidates_for_user(
            user_id=user_id, num_candidates=num_candidates_per_user
        )
        for user_id in users
    }
