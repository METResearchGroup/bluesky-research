"""API interface for adding users to a study."""
import json

from services.participant_data.helper import manage_bsky_study_user


def lambda_handler(event, context):
    payload = event["payload"]
    res: dict = manage_bsky_study_user(payload)
    status = res.pop("status")
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(res)
    }


def _test_handler():
    event = {"payload": {"operation": "INSERT_MOCK_USERS"}}
    # event = {"payload": {"operation": "GET", "bluesky_user_did": "did:plc:mvvopd2jj3432twfga7nvpcm"}}
    context = {}
    res = lambda_handler(event, context)
    print(res)


if __name__ == "__main__":
    # _test_handler()
    pass
