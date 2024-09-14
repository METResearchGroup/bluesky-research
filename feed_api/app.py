"""Base app for the feed API.

Based on specs in the following docs:
- https://github.com/bluesky-social/feed-generator/blob/main/src/lexicon/types/app/bsky/feed/getFeedSkeleton.ts#L4
- https://github.com/bluesky-social/feed-generator
"""  # noqa

import json
import os
from typing import Optional, Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.security.api_key import APIKeyHeader
from mangum import Mangum
import uvicorn

from feed_api.auth import AuthorizationError, validate_auth
from feed_api.helper import (
    cache_request,
    get_cached_request,
    export_log_data,
    is_valid_user_did,
    load_latest_user_feed,
)
from lib.aws.s3 import S3
from lib.aws.secretsmanager import get_secret
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from services.participant_data.helper import get_all_users, manage_bsky_study_user
from services.participant_data.models import UserOperation


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
DEFAULT_FEED_TOKEN = json.loads(get_secret("feed-api-default-test-token"))[
    "feed-api-default-test-token"
]
if not DEFAULT_FEED_TOKEN:
    raise ValueError("Need to provide a default token for test purposes")


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
    handle_to_did_map = {
        user.bluesky_handle: user.bluesky_user_did for user in study_users
    }
    if operation == "add" and bluesky_handle in existing_study_user_bsky_handles:
        raise HTTPException(status_code=400, detail="User already exists in study")
    elif (
        operation == "delete" or operation == "modify"
    ) and bluesky_handle not in existing_study_user_bsky_handles:
        raise HTTPException(status_code=400, detail="User does not exist in study")

    # then, get info from Bluesky.
    try:
        bsky_author_did = handle_to_did_map[bluesky_handle]
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"User doesn't exist in the study: {e}"
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
            "uri": "at://did:plc:dupygefpurstnheocpdfi2qd/app.bsky.feed.generator/nw-feed-algos"
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

    First checks to see if Bluesky has sent a duplicate request in the
    past 1 minute or so, and then returns the cached result without logging
    the request. This is because Bluesky sends duplicate requests when
    fetching a feed, and we don't want to count each request when they're
    clearly duplicated.

    Then, if we've determined that it's not a duplicated request, we fetch
    the latest feed. First, we see if there's a feed in the cache (which is
    updated whenever we generate new feeds). If for some reason there isn't,
    we fetch from S3 and then write that feed to the cache.

    We then return that feed and cursor and log the request.
    """
    try:
        requester_did = await validate_auth(credentials)
    except AuthorizationError:
        raise HTTPException(status_code=401, detail="Unauthorized")
    # NOTE: feeds can be accessed by Bluesky users who aren't in the study,
    # but I couldn't figure out how to get it to be accessed when you're
    # not logged in. I think the error is related to
    # `credentials: HTTPAuthorizationCredentials = Depends(security)`
    # but it's not worth figuring out for now.
    except HTTPException as e:
        logger.error(f"Error validating auth: {e}")
        if e.status_code == 403:
            logger.error("Invalid or missing Authorization header. Using default feed.")
            raise
    if not is_valid_user_did(requester_did):
        logger.info(f"User DID not in the study: {requester_did}. Using default feed.")
        requester_did = "default"
    logger.info(f"Validated request for DID={requester_did}...")
    cached_request = get_cached_request(user_did=requester_did, cursor=cursor)
    if cached_request:
        # we cache requests with a short-lived TTL because Bluesky sends
        # duplicate requests (e.g., if they need 2 requests to get the actual
        # feed, they send 4-6 for some reason). In this implementation, we
        # see if they've requested a feed recently and then just return.
        # We do it like this so that we can avoid duplicate requests,
        # saving on S3 reads, cache hits, and, possibly most importantly,
        # we don't log those requests multiple times.
        logger.info(f"Found cached request for user={requester_did}...")
        return cached_request
    request_cursor = cursor
    feed, next_cursor = load_latest_user_feed(
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
    # NOTE: might be slow to export to S3. Maybe faster to write to cache
    # and have the cache write to S3 offline?
    export_log_data(user_session_log)
    logger.info(f"Fetched {len(feed)} posts for user={requester_did}...")
    return output


@app.get("/health")
async def health_check():
    """Health check endpoint to verify the application is running."""
    return {"status": "healthy"}


@app.get("/get-default-feed")
async def get_default_feed_skeleton(
    request: Request,
    cursor: Annotated[Optional[str], Query()] = None,
    limit: Annotated[
        int, Query(ge=1, le=100)
    ] = 30,  # Bluesky fetches 30 results by default.
    authorization: str = Header(None),
):
    """Test endpoint for getting the default feeds."""
    if authorization != f"Bearer {DEFAULT_FEED_TOKEN}":
        raise HTTPException(status_code=401, detail="Invalid token")
    requester_did = "default"
    request_cursor = cursor
    feed, next_cursor = load_latest_user_feed(
        user_did=requester_did, cursor=request_cursor, limit=limit
    )
    output = {"cursor": next_cursor, "feed": feed}
    logger.info(f"Fetched {len(feed)} posts for user={requester_did}...")
    return output


@app.get("/test/")
async def test_endpoint():
    """Health check endpoint to verify the application is running."""
    return {"status": "healthy"}


# https://stackoverflow.com/questions/76844538/the-adapter-was-unable-to-infer-a-handler-to-use-for-the-event-this-is-likely-r
def handler(event, context):
    logger.info(f"Event payload: {event}")
    logger.info(f"Context payload: {context}")
    asgi_handler = Mangum(app)
    return asgi_handler(event, context)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
