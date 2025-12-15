"""Helper tooling from the participant data service."""

import hashlib
from typing import Optional

from botocore.exceptions import ClientError

from lib.aws.dynamodb import DynamoDB
from lib.constants import current_datetime_str
from lib.log.logger import get_logger
from lib.constants import TEST_USER_HANDLES
from services.participant_data.mock_users import mock_users
from services.participant_data.models import UserToBlueskyProfileModel

logger = get_logger(__name__)
TABLE_NAME = "study_participants"
dynamodb = DynamoDB()
table = dynamodb.resource.Table(TABLE_NAME)


def insert_bsky_user_to_db(
    user_model: UserToBlueskyProfileModel, overwrite_existing: bool = True
) -> None:
    """Insert a user into the study.

    Overwrites it if it already exists, unless the flag is set to False.
    """
    try:
        if overwrite_existing:
            table.put_item(Item=user_model.dict())
        else:
            table.put_item(
                Item=user_model.dict(),
                ConditionExpression="attribute_not_exists(bluesky_user_did)",
            )
    except ClientError as e:
        print(f"Failed to insert user to DynamoDB: {e}")
        raise e


def delete_bsky_user_from_db(bluesky_user_did: str) -> None:
    """Deletes a user from the study."""
    try:
        table.delete_item(Key={"bluesky_user_did": bluesky_user_did})
    except ClientError as e:
        print(f"Failed to delete user from DynamoDB: {e}")
        raise e


def fetch_all_users_in_condition(condition: str) -> list[UserToBlueskyProfileModel]:  # noqa
    """Return all users in a given condition."""
    try:
        response = table.scan(
            FilterExpression="condition = :val",
            ExpressionAttributeValues={":val": condition},
        )
        return [UserToBlueskyProfileModel(**item) for item in response["Items"]]  # noqa
    except ClientError as e:
        print(f"Failed to fetch users from DynamoDB: {e}")
        return []


def insert_bsky_user_to_study(
    bluesky_handle: str,
    condition: str,
    bluesky_user_did: str,
    is_study_user: bool = True,
) -> UserToBlueskyProfileModel:
    """Insert a user into the study.

    Assumes that the primary key is the user's bluesky_user_did, which cannot
    be changed (bluesky_handle can technically be changed, e.g., if the user
    has a custom domain).
    """
    study_user_id = hashlib.sha256(bluesky_handle.encode()).hexdigest()
    user_model = UserToBlueskyProfileModel(
        study_user_id=study_user_id,
        bluesky_handle=bluesky_handle,
        bluesky_user_did=bluesky_user_did,
        condition=condition,
        is_study_user=is_study_user,
        created_timestamp=current_datetime_str,
    )
    insert_bsky_user_to_db(user_model)
    return user_model


def delete_bsky_user_from_study(bluesky_user_did: str) -> None:
    """Delete a user from the study."""
    delete_bsky_user_from_db(bluesky_user_did)
    return


def get_bsky_study_user(bluesky_user_did: str) -> UserToBlueskyProfileModel:
    """Get a user from the study."""
    try:
        response = table.get_item(Key={"bluesky_user_did": bluesky_user_did})
        return UserToBlueskyProfileModel(**response["Item"])
    except ClientError as e:
        print(f"Failed to get user from DynamoDB: {e}")
        raise e


def manage_bsky_study_user(payload: dict) -> dict:
    """Manage Bluesky study users."""
    operation = payload.get("operation")
    message: str = None
    result: Optional[dict] = None
    try:
        if operation == "POST":
            user_model: UserToBlueskyProfileModel = insert_bsky_user_to_study(
                bluesky_handle=payload["bluesky_handle"],
                condition=payload["condition"],
                bluesky_user_did=payload["bluesky_user_did"],
                is_study_user=payload.get("is_study_user", True),
            )
            result = user_model
            message = "User added to study."
        elif operation == "GET":
            user_model: UserToBlueskyProfileModel = get_bsky_study_user(
                bluesky_user_did=payload["bluesky_user_did"]
            )
            if user_model:
                result = user_model.dict()
            message = "User fetched successfully."
        elif operation == "DELETE":
            delete_bsky_user_from_study(bluesky_user_did=payload["bluesky_user_did"])
            message = "User deleted successfully."
        elif operation == "INSERT_MOCK_USERS":
            _insert_mock_users_into_study()
        else:
            return {
                "status": 400,
                "message": f"Invalid operation: {operation}.",
                "operation": operation,
                "payload": payload,
            }
    except Exception as e:
        return {
            "status": 400,
            "message": f"Operation not implemented due to error: {e}.",
            "operation": operation,
            "payload": payload,
        }
    return {
        "status": 200,
        "message": f"Operation successful. {message}",
        "result": result,
    }


def manage_bsky_study_users(payloads: list[dict]) -> list[dict]:
    return [manage_bsky_study_user(payload) for payload in payloads]


def _insert_mock_users_into_study():
    """Inserts mock users into the study."""
    payloads = [
        {
            "operation": "POST",
            "bluesky_user_did": user["bluesky_user_did"],
            "bluesky_handle": user["bluesky_handle"],
            "is_study_user": False,
            "condition": user["condition"],
        }
        for user in mock_users
    ]
    manage_bsky_study_users(payloads)
    print(f"Inserted {len(mock_users)} mock users into the study.")


def get_all_users(test_mode: bool = False) -> list[UserToBlueskyProfileModel]:
    """Get all users in the study."""
    try:
        response = table.scan()
        users = [UserToBlueskyProfileModel(**item) for item in response["Items"]]
        if test_mode:
            logger.info(f"Filtering to test users only: {TEST_USER_HANDLES}.")
            users = [user for user in users if user.bluesky_handle in TEST_USER_HANDLES]
        return users
    except ClientError as e:
        print(f"Failed to fetch users from DynamoDB: {e}")
        return []
