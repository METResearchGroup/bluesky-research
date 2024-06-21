"""Base app for the feed API.

Based on specs in the following docs:
- https://github.com/bluesky-social/feed-generator/blob/main/src/lexicon/types/app/bsky/feed/getFeedSkeleton.ts#L4
- https://github.com/bluesky-social/feed-generator
"""
from typing import Optional, Annotated

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.responses import JSONResponse

from feed_api.auth import get_requester_did, validate_did
from feed_api.helper import get_latest_feed

app = FastAPI()


@app.get('/')
async def root():
    return {"message": "Hello, World!"}


@app.get("/xrpc/app.bsky.feed.getFeedSkeleton")
async def get_feed_skeleton(
    cursor: Annotated[Optional[str], Query()] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    requester_did: str = Depends(get_requester_did),
    valid_did: str = Depends(validate_did)
):
    try:
        body = get_latest_feed(requester_did, limit, cursor)
    except ValueError:
        raise HTTPException(status_code=400, detail="Malformed cursor")

    return JSONResponse(content=body)
