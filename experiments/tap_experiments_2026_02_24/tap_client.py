"""
Client for Bluesky Tap (repo sync). Add DIDs, connect to the channel WebSocket,
and consume events (e.g. app.bsky.feed.post) until requested URIs are received.
"""

import asyncio
import json
from typing import Any, AsyncIterator, Callable

import requests
import websockets

from lib.log.logger import get_logger

logger = get_logger(__name__)

DEFAULT_BASE_URL = "http://localhost:2480"
DEFAULT_WS_URL = "ws://localhost:2480"
POST_COLLECTION = "app.bsky.feed.post"


def _ws_url_from_base(base_url: str) -> str:
    """Derive WebSocket URL from HTTP base (e.g. http://localhost:2480 -> ws://localhost:2480)."""
    if base_url.startswith("http://"):
        return "ws" + base_url[4:]
    if base_url.startswith("https://"):
        return "wss" + base_url[5:]
    return base_url


def _parse_tap_record_event(event: dict, collection: str) -> tuple[str | None, dict | None]:
    """Parse indigo Tap event: type=record, record.did/collection/rkey -> uri, record.record -> post."""
    if event.get("type") != "record":
        return None, None
    rec = event.get("record")
    if not isinstance(rec, dict):
        return None, None
    did_ = rec.get("did")
    coll = rec.get("collection")
    rkey = rec.get("rkey")
    if not all([did_, coll, rkey]) or coll != collection:
        return None, None
    uri = f"at://{did_}/{coll}/{rkey}"
    inner = rec.get("record")
    if isinstance(inner, dict):
        return uri, inner
    return uri, rec


def _process_one_tap_message(
    raw: str | bytes,
    wanted: set[str],
    result: dict[str, dict],
    collection: str,
) -> None:
    """Parse one WebSocket message and if it is a wanted post record, add to result and remove from wanted."""
    text = raw.decode() if isinstance(raw, bytes) else raw
    try:
        event = json.loads(text)
    except json.JSONDecodeError:
        return
    uri, post_record = _parse_tap_record_event(event, collection)
    if uri and uri in wanted and post_record:
        result[uri] = post_record
        wanted.discard(uri)
        logger.debug(f"Tap received post: {uri}")


async def _consume_tap_channel(
    channel_url: str,
    dids_to_add: list[str] | None,
    wanted: set[str],
    result: dict[str, dict],
    collection: str,
    add_repos_fn: Callable[[list[str]], None],
) -> None:
    """Connect to Tap channel, optionally add DIDs, and consume messages until wanted is empty."""
    async with websockets.connect(channel_url) as ws:
        if dids_to_add:
            add_repos_fn(dids_to_add)
        async for raw in ws:
            if not wanted:
                return
            _process_one_tap_message(raw, wanted, result, collection)


class TapRepoSync:
    """Client for Tap: add repos by DID, stream events over WebSocket, collect posts by URI."""

    def __init__(self, base_url: str = DEFAULT_BASE_URL) -> None:
        self.base_url = base_url.rstrip("/")
        self.ws_url = _ws_url_from_base(base_url).rstrip("/")

    def add_repos(self, dids: list[str]) -> None:
        """Register DIDs with Tap so it backfills and streams their repo events."""
        url = f"{self.base_url}/repos/add"
        resp = requests.post(url, json={"dids": dids}, timeout=30)
        resp.raise_for_status()
        logger.info(f"Tap add_repos: {resp.status_code}")

    async def connect(self) -> Any:
        """Open WebSocket connection to Tap channel."""
        channel_url = f"{self.ws_url}/channel"
        return await websockets.connect(channel_url)

    async def stream_events(self) -> AsyncIterator[dict[str, Any]]:
        """Yield parsed JSON events from the Tap channel."""
        async with (await self.connect()) as ws:
            async for raw in ws:
                try:
                    event = json.loads(raw)
                    yield event
                except json.JSONDecodeError as e:
                    logger.warning(f"Tap JSON decode error: {e}")

    async def wait_for_posts(
        self,
        post_uris: set[str],
        timeout_sec: float = 120.0,
        collection: str = POST_COLLECTION,
        dids_to_add: list[str] | None = None,
    ) -> dict[str, dict]:
        """
        Consume Tap events until all requested post URIs have been received or timeout.

        If dids_to_add is provided, connect to the WebSocket first, then call add_repos,
        so we do not miss backfill events that Tap sends immediately after add.
        """
        wanted = set(post_uris)
        result: dict[str, dict] = {}
        channel_url = f"{self.ws_url}/channel"
        try:
            await asyncio.wait_for(
                _consume_tap_channel(
                    channel_url, dids_to_add, wanted, result, collection, self.add_repos
                ),
                timeout=timeout_sec,
            )
        except asyncio.TimeoutError:
            logger.warning(f"Tap wait_for_posts timed out; got {len(result)} of {len(post_uris)}")
        return result


def extract_did_from_post_uri(post_uri: str) -> str:
    """From at://DID/app.bsky.feed.post/RKEY return DID."""
    parts = post_uri.split("/")
    if len(parts) < 3:
        raise ValueError(f"Cannot extract DID from post URI: {post_uri}")
    return parts[2]
