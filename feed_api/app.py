"""Base app for the feed API.

Based on specs in the following docs:
- https://github.com/bluesky-social/feed-generator/blob/main/src/lexicon/types/app/bsky/feed/getFeedSkeleton.ts#L4
- https://github.com/bluesky-social/feed-generator
"""  # noqa
import json
import os
from typing import Optional, Annotated

from fastapi import FastAPI, HTTPException, Query
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


@app.get('/feed_api/')
async def root():
    return {"message": "Hello, World!"}


@app.get("/feed_api/test-get-s3")
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


@app.get("/feed_api/xrpc/app.bsky.feed.getFeedSkeleton")
async def get_feed_skeleton(
    cursor: Annotated[Optional[str], Query()] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
):
    try:
        return JSONResponse(content={"message": "Not implemented yet."})
    except ValueError:
        raise HTTPException(status_code=400, detail="Malformed cursor")


handler = Mangum(app)
