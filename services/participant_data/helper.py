"""Helper tooling from the participant data service."""
import hashlib

from services.participant_data.models import UserToBlueskyProfileModel


def insert_bsky_user_to_db(user_model: UserToBlueskyProfileModel) -> None:
    """Insert a user into the study."""
    pass


def delete_bsky_user_from_db(bluesky_handle: str) -> None:
    """Deletes a user from the study."""
    pass


def fetch_all_users_in_condition(condition: str) -> list[UserToBlueskyProfileModel]:  # noqa
    """Return all users in a given condition."""
    pass


def insert_bsky_user_to_study(
    bluesky_handle: str, condition: str, bluesky_user_did: str,
    is_study_user: bool = True
) -> None:
    """Insert a user into the study."""
    study_user_id = hashlib.sha256(bluesky_handle.encode()).hexdigest()
    user_model = UserToBlueskyProfileModel(
        study_user_id=study_user_id,
        bluesky_handle=bluesky_handle,
        bluesky_user_did=bluesky_user_did,
        condition=condition,
        is_study_user=is_study_user
    )
    insert_bsky_user_to_db(user_model)


def manage_insert_bsky_users_to_study(payload: dict):
    pass


def delete_bsky_user_from_study(bluesky_handle: str) -> None:
    """Delete a user from the study."""
    pass


def manage_delete_bsky_users_from_study(payload: dict):
    pass


def manage_bsky_study_user(payload: dict) -> dict:
    """Manage Bluesky study users."""
    operation = payload.get("operation")
    try:
        if operation == "create":
            # TODO: need fields that I'll need to filter by.
            insert_bsky_user_to_study()
        elif operation == "delete":
            delete_bsky_user_from_study()
        else:
            return {
                "status": 400, "message": f"Invalid operation: {operation}."
            }
    except Exception as e:
        return {
            "status": 400,
            "message": f"Operation not implemented due to error: {e}."
        }
    return {
        "status": 200, "message": "Operation successful."
    }
