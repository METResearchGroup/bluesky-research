"""Base app for the feed API.

Based on specs in the following docs:
- https://github.com/bluesky-social/feed-generator/blob/main/src/lexicon/types/app/bsky/feed/getFeedSkeleton.ts#L4
- https://github.com/bluesky-social/feed-generator
"""  # noqa

import asyncio
import json
import threading
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
    is_valid_user_did,
    create_feed_and_cursor,
    load_all_latest_user_feeds_from_s3,
    load_latest_user_feed,
    valid_dids,
    study_user_did_to_handle_map,
    test_user_dids,
)
from feed_api.user_session_queue import background_s3_writer, log_queue
from lib.aws.s3 import S3
from lib.aws.secretsmanager import get_secret
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger


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

user_did_to_cached_feed: dict[str, list[dict]] = {}

threading.Thread(target=background_s3_writer, daemon=True).start()

feed_refresh_hours = 3
feed_refresh_minutes = feed_refresh_hours * 60
feed_refresh_seconds = feed_refresh_minutes * 60


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


# add "default" key to the valid DIDs set.
valid_dids.add("default")


def refresh_user_did_to_cached_feed():
    """Refreshes the local cached feeds from S3."""
    global user_did_to_cached_feed
    user_did_to_cached_feed = load_all_latest_user_feeds_from_s3()
    logger.info("Initialized user DID to cache feed mapping.")


async def refresh_feeds_periodically():
    """Refreshes the local cached feeds every hour."""
    while True:
        logger.info("Refreshing local cached feeds...")
        refresh_user_did_to_cached_feed()
        logger.info("Refreshed local cached feeds. Waiting 1 hour for next refresh.")  # noqa
        await asyncio.sleep(feed_refresh_seconds)


# Start the periodic refresh in a background task
@app.on_event("startup")
async def start_refresh_task():
    """Starts the background task to refresh the local cached feeds."""
    asyncio.create_task(refresh_feeds_periodically())


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


@app.get("/xrpc/app.bsky.feed.describeFeedGenerator")
async def describe_feed_generator():  # def or async def?
    feeds = [
        {
            "uri": "at://did:plc:dupygefpurstnheocpdfi2qd/app.bsky.feed.generator/nw-study-feed"
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
    request_cursor = cursor
    if requester_did in test_user_dids:
        handle = study_user_did_to_handle_map[requester_did]
        logger.info(
            f"Test user handle={handle} accessed the feed. Fetch latest feed from external cache + S3."
        )
        feed_dicts: list[dict] = load_latest_user_feed(user_did=requester_did)
    else:
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
        if requester_did in user_did_to_cached_feed:
            # if requester_did's feed is in the in-memory cache, use that one.
            feed_dicts: list[dict] = user_did_to_cached_feed[requester_did]
        else:
            logger.warning(
                f"Feed for {requester_did} not in local cache (should be). Loading from external cache + S3..."
            )  # noqa
            feed_dicts: list[dict] = load_latest_user_feed(user_did=requester_did)
            logger.info(
                f"Loaded feed for {requester_did} from S3 + cache. Added to local store"
            )  # noqa
            user_did_to_cached_feed[requester_did] = feed_dicts
    feed, next_cursor = create_feed_and_cursor(
        feed_dicts=feed_dicts,
        user_did=requester_did,
        cursor=request_cursor,
        limit=limit,
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
    # Write user session logs to queue in background thread, which will be
    # periodically flushed and inserted into S3.
    log_queue.put(user_session_log)
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
    if requester_did in user_did_to_cached_feed:
        feed_dicts: list[dict] = user_did_to_cached_feed[requester_did]
    else:
        logger.warning(
            f"Feed for {requester_did} not in local cache (should be). Loading from external cache + S3..."
        )  # noqa
        feed_dicts: list[dict] = load_latest_user_feed(
            user_did=requester_did, cursor=cursor, limit=limit
        )
        logger.info(
            f"Loaded feed for {requester_did} from S3 + cache. Added to local store"
        )  # noqa
        user_did_to_cached_feed[requester_did] = feed_dicts
    feed, next_cursor = create_feed_and_cursor(
        feed_dicts=feed_dicts,
        user_did=requester_did,
        cursor=request_cursor,
        limit=limit,
    )
    output = {"cursor": next_cursor, "feed": feed}
    logger.info(f"Fetched {len(feed)} posts for user={requester_did}...")
    user_session_log = {
        "user_did": requester_did,
        "cursor": next_cursor,
        "limit": limit,
        "feed_length": len(feed),
        "feed": feed,
        "timestamp": generate_current_datetime_str(),
    }
    # Write user session logs to queue in background thread, which will be
    # periodically flushed and inserted into S3.
    log_queue.put(user_session_log)
    return output


# https://stackoverflow.com/questions/76844538/the-adapter-was-unable-to-infer-a-handler-to-use-for-the-event-this-is-likely-r
def handler(event, context):
    logger.info(f"Event payload: {event}")
    logger.info(f"Context payload: {context}")
    asgi_handler = Mangum(app)
    return asgi_handler(event, context)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
