"""Base app for the feed API.

Based on specs in the following docs:
- https://github.com/bluesky-social/feed-generator/blob/main/src/lexicon/types/app/bsky/feed/getFeedSkeleton.ts#L4
- https://github.com/bluesky-social/feed-generator
"""  # noqa
import json
import logging
import os
from typing import Optional, Annotated

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum

from lib.aws.s3 import S3
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


@app.get('/')
async def root():
    return {"message": "Hello, World!"}


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
        "at://did:plc:nvfposmpmhegtyvhbs75s3pw/app.bsky.feed.post/3kumciaqa6c2c",
        "at://did:plc:s6j27rxb3ic2rxw73ixgqv2p/app.bsky.feed.post/3kum6oz3cov2m",
        "at://did:plc:qzgy45vpvfpvbjsqhqjapghf/app.bsky.feed.post/3kum5fughrb2r",
        "at://did:plc:hun6pmxagw3xmzzlmfjpj4fw/app.bsky.feed.post/3kunsu423vo2",
        "at://did:plc:efx3llkdwipqoz4ie37tego6/app.bsky.feed.post/3kumf6mwle42z",
        "at://did:plc:kfdiz4ohpkjfceecgiiideek/app.bsky.feed.post/3kumsroimvr2o",
        "at://did:plc:4lydetq2xtcnkp7vf7r5ljwm/app.bsky.feed.post/3kulxwqjxsz2g"
    ]
    example_posts = [
        {"post": uri} for uri in test_uris
    ]
    return {"cursor": None, "feed": example_posts}


# handler = Mangum(app)

# https://stackoverflow.com/questions/76844538/the-adapter-was-unable-to-infer-a-handler-to-use-for-the-event-this-is-likely-r
def handler(event, context):
    logger.info(f"Event payload: {event}")
    logger.info(f"Context payload: {context}")
    asgi_handler = Mangum(app)
    return asgi_handler(event, context)
