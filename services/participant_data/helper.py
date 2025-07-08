"""Helper tooling from the participant data service."""
import hashlib
from typing import Optional

from botocore.exceptions import ClientError

from lib.aws.dynamodb import DynamoDB
from lib.constants import current_datetime_str
from services.participant_data.mock_users import mock_users
from services.participant_data.models import UserToBlueskyProfileModel, UserProfileResponse


TABLE_NAME = "study_participants"
dynamodb = DynamoDB()
table = dynamodb.resource.Table(TABLE_NAME)


def calculate_user_score(bluesky_user_did: str) -> int:
    """Calculate user score based on number of posts.

    Args:
        bluesky_user_did: The user's DID to calculate score for

    Returns:
        int: The user's score (number of posts they've made)
    """
    try:
        from lib.db.sql.user_engagement_database import PostsWrittenByStudyUsers
        
        # Count posts written by this user
        post_count = PostsWrittenByStudyUsers.select().where(
            PostsWrittenByStudyUsers.author_did == bluesky_user_did
        ).count()
        
        return post_count
    except Exception as e:
        print(f"Error calculating score for user {bluesky_user_did}: {e}")
        return 0


def get_user_by_handle(bluesky_handle: str) -> Optional[UserToBlueskyProfileModel]:
    """Get a user by their Bluesky handle.

    Args:
        bluesky_handle: The user's Bluesky handle

    Returns:
        UserToBlueskyProfileModel or None if not found
    """
    try:
        response = table.scan(
            FilterExpression='bluesky_handle = :handle',
            ExpressionAttributeValues={':handle': bluesky_handle}
        )
        
        if response['Items']:
            user_data = response['Items'][0]
            user = UserToBlueskyProfileModel(**user_data)
            
            # Calculate current score
            current_score = calculate_user_score(user.bluesky_user_did)
            
            # Update user model with current score
            user.score = current_score
            
            # Update score in database if it's different
            if 'score' not in user_data or user_data.get('score', 0) != current_score:
                update_user_score(user.bluesky_user_did, current_score)
            
            return user
        
        return None
    except ClientError as e:
        print(f"Failed to get user by handle from DynamoDB: {e}")
        return None


def update_user_score(bluesky_user_did: str, score: int) -> None:
    """Update a user's score in the database.

    Args:
        bluesky_user_did: The user's DID
        score: The new score value
    """
    try:
        table.update_item(
            Key={'bluesky_user_did': bluesky_user_did},
            UpdateExpression='SET score = :score',
            ExpressionAttributeValues={':score': score}
        )
    except ClientError as e:
        print(f"Failed to update user score in DynamoDB: {e}")
        raise e


def get_user_profile_response(bluesky_handle: str) -> Optional[UserProfileResponse]:
    """Get user profile response for API endpoint.

    Args:
        bluesky_handle: The user's Bluesky handle

    Returns:
        UserProfileResponse or None if user not found
    """
    user = get_user_by_handle(bluesky_handle)
    if not user:
        return None
    
    return UserProfileResponse(
        study_user_id=user.study_user_id,
        bluesky_handle=user.bluesky_handle,
        bluesky_user_did=user.bluesky_user_did,
        condition=user.condition,
        score=user.score,
        is_study_user=user.is_study_user,
        created_timestamp=user.created_timestamp
    )


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
                ConditionExpression='attribute_not_exists(bluesky_user_did)'
            )
    except ClientError as e:
        print(f"Failed to insert user to DynamoDB: {e}")
        raise e


def delete_bsky_user_from_db(bluesky_user_did: str) -> None:
    """Deletes a user from the study."""
    try:
        table.delete_item(Key={'bluesky_user_did': bluesky_user_did})
    except ClientError as e:
        print(f"Failed to delete user from DynamoDB: {e}")
        raise e


def fetch_all_users_in_condition(condition: str) -> list[UserToBlueskyProfileModel]:  # noqa
    """Return all users in a given condition."""
    try:
        response = table.scan(
            FilterExpression='condition = :val',
            ExpressionAttributeValues={':val': condition}
        )
        return [UserToBlueskyProfileModel(**item) for item in response['Items']]  # noqa
    except ClientError as e:
        print(f"Failed to fetch users from DynamoDB: {e}")
        return []


def insert_bsky_user_to_study(
    bluesky_handle: str, condition: str, bluesky_user_did: str,
    is_study_user: bool = True
) -> UserToBlueskyProfileModel:
    """Insert a user into the study.

    Assumes that the primary key is the user's bluesky_user_did, which cannot
    be changed (bluesky_handle can technically be changed, e.g., if the user
    has a custom domain).
    """
    study_user_id = hashlib.sha256(bluesky_handle.encode()).hexdigest()
    
    # Calculate initial score
    initial_score = calculate_user_score(bluesky_user_did)
    
    user_model = UserToBlueskyProfileModel(
        study_user_id=study_user_id,
        bluesky_handle=bluesky_handle,
        bluesky_user_did=bluesky_user_did,
        condition=condition,
        is_study_user=is_study_user,
        created_timestamp=current_datetime_str,
        score=initial_score
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
        response = table.get_item(Key={'bluesky_user_did': bluesky_user_did})
        return UserToBlueskyProfileModel(**response['Item'])
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
                is_study_user=payload.get("is_study_user", True)
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
            delete_bsky_user_from_study(
                bluesky_user_did=payload["bluesky_user_did"]
            )
            message = "User deleted successfully."
        elif operation == "INSERT_MOCK_USERS":
            _insert_mock_users_into_study()
        else:
            return {
                "status": 400,
                "message": f"Invalid operation: {operation}.",
                "operation": operation,
                "payload": payload
            }
    except Exception as e:
        return {
            "status": 400,
            "message": f"Operation not implemented due to error: {e}.",
            "operation": operation,
            "payload": payload
        }
    return {
        "status": 200,
        "message": f"Operation successful. {message}",
        "result": result
    }


def manage_bsky_study_users(payloads: list[dict]) -> list[dict]:
    return [
        manage_bsky_study_user(payload) for payload in payloads
    ]


def _insert_mock_users_into_study():
    """Inserts mock users into the study."""
    payloads = [
        {
            "operation": "POST",
            "bluesky_user_did": user["bluesky_user_did"],
            "bluesky_handle": user["bluesky_handle"],
            "is_study_user": False,
            "condition": user["condition"]
        }
        for user in mock_users
    ]
    manage_bsky_study_users(payloads)
    print(f"Inserted {len(mock_users)} mock users into the study.")


def get_all_users() -> list[UserToBlueskyProfileModel]:
    """Get all users in the study."""
    try:
        response = table.scan()
        users = []
        for item in response['Items']:
            user = UserToBlueskyProfileModel(**item)
            # Calculate current score for each user
            current_score = calculate_user_score(user.bluesky_user_did)
            user.score = current_score
            # Update score in database if needed
            if 'score' not in item or item.get('score', 0) != current_score:
                update_user_score(user.bluesky_user_did, current_score)
            users.append(user)
        return users
    except ClientError as e:
        print(f"Failed to fetch users from DynamoDB: {e}")
        return []
