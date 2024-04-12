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


def write_latest_candidates_per_user(user_to_candidate_dict: dict) -> None:
    """Write the latest candidates per user to database.
    
    We want to store all the candidate posts that we generated per user.

    We only need to store user and list of post URIs. Downstream, we can
    do joins in order to get the post details and features.
    """
    pass


def generate_candidates(num_candidates_per_user: int) -> dict[str, list[dict]]:
    """Generates candidates per user.
    """
    users: list[str] = get_users()
    user_to_candidate_dict = {
        user_id: generate_candidates_for_user(
            user_id=user_id, num_candidates=num_candidates_per_user
        )
        for user_id in users
    }
    write_latest_candidates_per_user(user_to_candidate_dict)
    return user_to_candidate_dict