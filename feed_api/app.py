"""Base app for the feed API.

Based on specs in the following docs:
- https://github.com/bluesky-social/feed-generator/blob/main/src/lexicon/types/app/bsky/feed/getFeedSkeleton.ts#L4
- https://github.com/bluesky-social/feed-generator
"""  # noqa

import json
import os
from typing import Optional, Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.security.api_key import APIKeyHeader
from mangum import Mangum

from feed_api.auth import AuthorizationError, validate_auth
from feed_api.helper import (
    cache_request,
    export_log_data,
    get_cached_request,
    is_valid_user_did,
    load_latest_user_feed_from_s3,
)
from lib.aws.s3 import S3
from lib.aws.secretsmanager import get_secret
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from services.participant_data.helper import get_all_users, manage_bsky_study_user
from services.participant_data.models import UserOperation
from transform.bluesky_helper import get_author_did_from_handle


app = FastAPI()

s3 = S3()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)
security = HTTPBearer()

logger = get_logger(__file__)

API_KEY_NAME = "bsky-internal-api-key"
REQUIRED_API_KEY = json.loads(get_secret("bsky-internal-api-key"))[
    "BSKY_INTERNAL_API_KEY"
]

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)


async def get_api_key(api_key_header: Optional[str] = Security(api_key_header)):
    if api_key_header == REQUIRED_API_KEY:
        return api_key_header
    else:
        raise HTTPException(status_code=403, detail="Could not validate credentials")


@app.middleware("http")
async def log_request(request: Request, call_next):
    logger.info(f"Request method: {request.method}")
    logger.info(f"Request url: {request.url}")
    headers = {key: value for key, value in request.headers.items()}
    logger.info(f"Request headers: {headers}")
    body = await request.body()
    logger.info(f"Request body: {body.decode('utf-8')}")
    response = await call_next(request)
    return response


# redirect to Billy's site: https://sites.google.com/u.northwestern.edu/mind-technology-lab
@app.get("/")
async def root():
    return RedirectResponse(
        url="https://sites.google.com/u.northwestern.edu/mind-technology-lab"
    )


# https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/app.py#L36
@app.get("/.well-known/did.json")
def get_did_document():  # should this be async def?
    return {
        "@context": ["https://www.w3.org/ns/did/v1"],
        "id": "did:web:mindtechnologylab.com",
        "service": [
            {
                "id": "#bsky_fg",
                "type": "BskyFeedGenerator",
                "serviceEndpoint": "https://mindtechnologylab.com",
            }
        ],
    }


@app.get("/test-get-s3")
async def fetch_test_file_from_s3():
    """Testing fetching a file from S3."""
    bucket = "bluesky-research"
    key = os.path.join(
        "ml_inference_perspective_api", "previously_classified_post_uris.json"
    )
    res: dict = s3.read_json_from_s3(bucket=bucket, key=key)
    return {
        "message": "Successfully fetched test file.",
        "data": json.dumps(res),
    }


@app.post("/manage_user", dependencies=[Security(get_api_key)])
async def manage_user(user_operation: UserOperation):
    """Manage user operations.

    Add, modify, or delete a user in the study.

    Auth required with API key. Managed by API gateway.
    """
    logger.info(f"User operation: {user_operation}")
    operation = user_operation.operation.lower()
    valid_operations = ["add", "modify", "delete"]
    if operation not in valid_operations:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid operation: {operation} (only {valid_operations} allowed)",  # noqa
        )

    profile_link = user_operation.bluesky_user_profile_link

    if not profile_link.startswith("https://bsky.app/profile/"):
        raise HTTPException(status_code=400, detail="Invalid profile link")

    if operation != "delete" and user_operation.condition is None:
        raise HTTPException(
            status_code=400,
            detail="Study condition required for add or modify operation",
        )

    # get profile info from Bsky.
    bluesky_handle = profile_link.split("/")[-1]

    # first, check if handle is in the study already.
    # should be OK to reload this each time. Plus this is a small table
    # and we're not doing this operation frequently.
    study_users = get_all_users()
    existing_study_user_bsky_handles = [user.bluesky_handle for user in study_users]
    if operation == "add" and bluesky_handle in existing_study_user_bsky_handles:
        raise HTTPException(status_code=400, detail="User already exists in study")
    elif (
        operation == "delete" or operation == "modify"
    ) and bluesky_handle not in existing_study_user_bsky_handles:
        raise HTTPException(status_code=400, detail="User does not exist in study")

    # then, get info from Bluesky.
    try:
        bsky_author_did = get_author_did_from_handle(bluesky_handle)
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Error getting user information from Bluesky: {e}"
        )

    if operation in ["add", "modify"]:
        res = manage_bsky_study_user(
            payload={
                "operation": "POST",
                "bluesky_handle": bluesky_handle,
                "condition": user_operation.condition,
                "bluesky_user_did": bsky_author_did,
                "is_study_user": user_operation.is_study_user,
            }
        )
    elif operation in ["delete"]:
        res = manage_bsky_study_user(
            payload={"operation": "DELETE", "bluesky_user_did": bsky_author_did}
        )
    return res


@app.get("/xrpc/app.bsky.feed.describeFeedGenerator")
async def describe_feed_generator():  # def or async def?
    feeds = [
        {
            "uri": "at://did:plc:dupygefpurstnheocpdfi2qd/app.bsky.feed.generator/bsky-feed-4"
        }
    ]
    response = {
        "encoding": "application/json",
        "body": {"did": "did:web:mindtechnologylab.com", "feeds": feeds},
    }
    return response


@app.get("/xrpc/app.bsky.feed.getFeedSkeleton")
async def get_feed_skeleton(
    request: Request,
    cursor: Annotated[Optional[str], Query()] = None,
    limit: Annotated[
        int, Query(ge=1, le=100)
    ] = 30,  # Bluesky fetches 30 results by default.
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Fetches the feed skeleton for a user.

    Returns a cached request if it exists.
    Otherwise, fetches the feed from S3 and caches it.
    """
    try:
        requester_did = await validate_auth(credentials)
    except AuthorizationError:
        raise HTTPException(status_code=401, detail="Unauthorized")
    except HTTPException as e:
        if e.status_code == 403:
            logger.error("Invalid or missing Authorization header")
        raise
    if not is_valid_user_did(requester_did):
        logger.info(f"Invalid user DID: {requester_did}. Using default feed.")
        requester_did = "default"
    logger.info(f"Validated request for DID={requester_did}...")
    cached_request = get_cached_request(user_did=requester_did, cursor=cursor)
    if cached_request:
        logger.info(f"Found cached request for user={requester_did}...")
        return cached_request
    request_cursor = cursor
    feed, next_cursor = load_latest_user_feed_from_s3(
        user_did=requester_did, cursor=request_cursor, limit=limit
    )
    output = {"cursor": next_cursor, "feed": feed}
    cache_request(user_did=requester_did, cursor=request_cursor, data=output)
    user_session_log = {
        "user_did": requester_did,
        "cursor": next_cursor,
        "limit": limit,
        "feed_length": len(feed),
        "feed": feed,
        "timestamp": generate_current_datetime_str(),
    }
    export_log_data(user_session_log)
    logger.info(f"Fetched {len(feed)} posts for user={requester_did}...")
    return output


# https://stackoverflow.com/questions/76844538/the-adapter-was-unable-to-infer-a-handler-to-use-for-the-event-this-is-likely-r
def handler(event, context):
    logger.info(f"Event payload: {event}")
    logger.info(f"Context payload: {context}")
    asgi_handler = Mangum(app)
    return asgi_handler(event, context)
