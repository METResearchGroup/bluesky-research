"""Base app for the feed API.

Based on specs in the following docs:
- https://github.com/bluesky-social/feed-generator/blob/main/src/lexicon/types/app/bsky/feed/getFeedSkeleton.ts#L4
- https://github.com/bluesky-social/feed-generator
"""  # noqa
import json
import logging
import os
from typing import Optional, Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from mangum import Mangum

from lib.aws.s3 import S3
from services.participant_data.helper import (
    get_all_users, manage_bsky_study_user
)
from services.participant_data.models import UserOperation
from transform.bluesky_helper import get_author_did_from_handle


app = FastAPI()

s3 = S3()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=["GET"],
    allow_headers=["*"],
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
@app.get('/')
async def root():
    return RedirectResponse(
        url="https://sites.google.com/u.northwestern.edu/mind-technology-lab"
    )


# https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/app.py#L36
@app.get("/.well-known/did.json")
def get_did_document():  # should this be async def?
    return {
        "@context": [
            "https://www.w3.org/ns/did/v1"
        ],
        "id": "did:web:mindtechnologylab.com",
        "service": [
            {
                "id": "#bsky_fg",
                "type": "BskyFeedGenerator",
                "serviceEndpoint": "https://mindtechnologylab.com"
            }
        ]
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


@app.post("/manage_user")
async def manage_user(user_operation: UserOperation):
    """Manage user operations.

    Add, modify, or delete a user in the study.

    Auth required with API key. Managed by API gateway.
    """
    operation = user_operation.operation.lower()
    if operation not in ["add", "modify", "delete"]:
        raise HTTPException(status_code=400, detail="Invalid operation")

    profile_link = user_operation.bluesky_user_profile_link

    if not profile_link.startswith("https://bsky.app/profile/"):
        raise HTTPException(status_code=400, detail="Invalid profile link")

    if operation != "delete" and user_operation.condition is None:
        raise HTTPException(
            status_code=400,
            detail="Study condition required for add or modify operation"
        )

    # get profile info from Bsky.
    bluesky_handle = profile_link.split("/")[-1]

    # first, check if handle is in the study already.
    # should be OK to reload this each time. Plus this is a small table
    # and we're not doing this operation frequently.
    study_users = get_all_users()
    existing_study_user_bsky_handles = [
        user.bluesky_handle for user in study_users
    ]
    if (
        operation == "add"
        and bluesky_handle in existing_study_user_bsky_handles
    ):
        raise HTTPException(
            status_code=400, detail="User already exists in study"
        )
    elif (
        operation == "delete" or operation == "modify"
        and bluesky_handle not in existing_study_user_bsky_handles
    ):
        raise HTTPException(
            status_code=400, detail="User does not exist in study"
        )

    # then, get info from Bluesky.
    bsky_author_did = get_author_did_from_handle(bluesky_handle)

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
            payload={
                "operation": "DELETE", "bluesky_user_did": bsky_author_did
            }
        )
    return res


@app.get('/xrpc/app.bsky.feed.describeFeedGenerator')
async def describe_feed_generator():  # def or async def?
    feeds = [
        {'uri': "at://did:plc:dupygefpurstnheocpdfi2qd/app.bsky.feed.generator/bsky-feed-4"}
    ]
    response = {
        'encoding': 'application/json',
        'body': {
            'did': "did:web:mindtechnologylab.com",
            'feeds': feeds
        }
    }
    return response


@app.get("/xrpc/app.bsky.feed.getFeedSkeleton")
async def get_feed_skeleton(
    cursor: Annotated[Optional[str], Query()] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
):
    # try:
    #     return JSONResponse(content={"message": "Not implemented yet."})
    # except ValueError:
    #     raise HTTPException(status_code=400, detail="Malformed cursor")

    # sourced from "most liked" feed on June 10th and 11th, 2024.
    # s3://bluesky-research/sync/most_liked_posts/year=2024/month=07/day=06/hour=22/minute=03/posts.jsonl
    # NOTE: should get from s3 later.
    test_uris = [
        # "at://did:plc:nvfposmpmhegtyvhbs75s3pw/app.bsky.feed.post/3kumciaqa6c2c",
        # "at://did:plc:s6j27rxb3ic2rxw73ixgqv2p/app.bsky.feed.post/3kum6oz3cov2m",
        # "at://did:plc:qzgy45vpvfpvbjsqhqjapghf/app.bsky.feed.post/3kum5fughrb2r",
        # "at://did:plc:hun6pmxagw3xmzzlmfjpj4fw/app.bsky.feed.post/3kunsu423vo2",
        # "at://did:plc:efx3llkdwipqoz4ie37tego6/app.bsky.feed.post/3kumf6mwle42z",
        # "at://did:plc:kfdiz4ohpkjfceecgiiideek/app.bsky.feed.post/3kumsroimvr2o",
        # "at://did:plc:4lydetq2xtcnkp7vf7r5ljwm/app.bsky.feed.post/3kulxwqjxsz2g"
        "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/3kyj33eengz2f"
    ]
    example_posts = [
        {"post": uri} for uri in test_uris
    ]
    return {"cursor": "eof", "feed": example_posts}


# handler = Mangum(app)

# https://stackoverflow.com/questions/76844538/the-adapter-was-unable-to-infer-a-handler-to-use-for-the-event-this-is-likely-r
def handler(event, context):
    logger.info(f"Event payload: {event}")
    logger.info(f"Context payload: {context}")
    asgi_handler = Mangum(app)
    return asgi_handler(event, context)
