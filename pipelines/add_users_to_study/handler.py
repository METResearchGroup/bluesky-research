"""API interface for adding users to a study."""
import json

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from mangum import Mangum
import uvicorn

from services.participant_data.helper import manage_bsky_study_user
from services.participant_data.models import (
    CreateUserRequestModel, UserToBlueskyProfileModel
)

app = FastAPI()
handler = Mangum(app)


def lambda_handler(event, context):
    payload = event["payload"]
    res: dict = manage_bsky_study_user(payload)
    status = res.pop("status")
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(res)
    }


@app.post("/users", status_code=201, response_model=UserToBlueskyProfileModel)
def add_user(request: CreateUserRequestModel):
    payload = request.dict()
    payload['operation'] = "POST"
    response = manage_bsky_study_user(payload)
    if response.get("status") != 200:
        raise HTTPException(
            status_code=response.get("status", 400),
            detail=response.get("message")
        )
    result: dict = response.get("result")
    return result


@app.get("/users/{bluesky_user_did}", status_code=200, response_model=UserToBlueskyProfileModel)  # noqa
def get_user(bluesky_user_did: str):
    payload = {
        "operation": "GET",
        "bluesky_user_did": bluesky_user_did
    }
    response = manage_bsky_study_user(payload)
    if response.get("status") != 200:
        raise HTTPException(status_code=response.get("status", 400), detail=response.get("message"))  # noqa
    result: dict = response.get("result")
    return result


@app.delete("/users/{bluesky_user_did}", status_code=200)
def delete_user(bluesky_user_did: str):
    payload = {
        "operation": "DELETE",
        "bluesky_user_did": bluesky_user_did
    }
    response = manage_bsky_study_user(payload)
    if response.get("status") != 200:
        raise HTTPException(status_code=response.get("status", 400), detail=response.get("message"))  # noqa
    return response


def _test_handler():
    event = {"payload": {"operation": "INSERT_MOCK_USERS"}}
    # event = {"payload": {"operation": "GET", "bluesky_user_did": "did:plc:mvvopd2jj3432twfga7nvpcm"}}
    context = {}
    res = lambda_handler(event, context)
    print(res)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
