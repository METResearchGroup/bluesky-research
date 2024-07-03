"""API interface for adding users to a study."""
import os
import json

from services.participant_data.helper import manage_bsky_study_user


def lambda_handler(event, context):
    payload = event["payload"]
    res: dict = manage_bsky_study_user(payload)
    status = res.pop("status")
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(res)
    }
