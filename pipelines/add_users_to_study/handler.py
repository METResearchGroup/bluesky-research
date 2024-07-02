"""API interface for adding users to a study."""
import os
import json


def lambda_handler(event, context):
    res: dict = {}
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(res)
    }
