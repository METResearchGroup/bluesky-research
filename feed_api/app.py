"""Base app for the feed API.

Based on specs in the following docs:
- https://github.com/bluesky-social/feed-generator/blob/main/src/lexicon/types/app/bsky/feed/getFeedSkeleton.ts#L4
- https://github.com/bluesky-social/feed-generator
"""
from typing import Optional, Annotated

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

app = FastAPI()


@app.get('/')
async def root():
    return {"message": "Hello, World!"}


@app.get("/test-get-s3")
async def fetch_test_file_from_s3():
    # TODO: fetch a file from S3.
    return {"message": "Not implemented yet."}


@app.get("/xrpc/app.bsky.feed.getFeedSkeleton")
async def get_feed_skeleton(
    cursor: Annotated[Optional[str], Query()] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
):
    try:
        return JSONResponse(content={"message": "Not implemented yet."})
    except ValueError:
        raise HTTPException(status_code=400, detail="Malformed cursor")
