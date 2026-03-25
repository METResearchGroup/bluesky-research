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
LIKE_COLLECTION = "app.bsky.feed.like"


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


def _parse_tap_like_post_event(event: dict) -> tuple[str | None, str | None, str | None]:
    """
    Parse a Tap record event for app.bsky.feed.like where the subject is a feed post.

    Returns (liker_did, like_uri, liked_post_uri) or (None, None, None) if not applicable.
    """
    if event.get("type") != "record":
        return None, None, None
    rec = event.get("record")
    if not isinstance(rec, dict):
        return None, None, None
    did_ = rec.get("did")
    coll = rec.get("collection")
    rkey = rec.get("rkey")
    if not all([did_, coll, rkey]) or coll != LIKE_COLLECTION:
        return None, None, None
    like_uri = f"at://{did_}/{coll}/{rkey}"
    inner = rec.get("record")
    if not isinstance(inner, dict):
        return None, None, None
    subj = inner.get("subject")
    if not isinstance(subj, dict):
        return None, None, None
    liked_uri = subj.get("uri")
    if not liked_uri or "/app.bsky.feed.post/" not in str(liked_uri):
        return None, None, None
    return str(did_), like_uri, str(liked_uri)


def _decode_ws_text(raw: str | bytes) -> str:
    return raw.decode() if isinstance(raw, bytes) else raw


def _ingest_like_event_text(text: str, likes: dict[str, tuple[str, str]]) -> None:
    try:
        event = json.loads(text)
    except json.JSONDecodeError:
        return
    user_did, like_uri, liked_post_uri = _parse_tap_like_post_event(event)
    if user_did and like_uri and liked_post_uri:
        likes[like_uri] = (user_did, liked_post_uri)
        logger.debug(f"Tap like: {like_uri} -> {liked_post_uri}")


async def _recv_like_events_until_stop(
    ws: Any,
    stop: asyncio.Event,
    likes: dict[str, tuple[str, str]],
) -> None:
    while not stop.is_set():
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
        except asyncio.TimeoutError:
            if stop.is_set():
                break
            continue
        _ingest_like_event_text(_decode_ws_text(raw), likes)


async def _drain_like_events_short(
    ws: Any,
    likes: dict[str, tuple[str, str]],
    drain_after_stop_sec: float,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + drain_after_stop_sec
    while loop.time() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=0.15)
        except asyncio.TimeoutError:
            break
        _ingest_like_event_text(_decode_ws_text(raw), likes)


async def _poll_tap_quiescent(
    tap: "TapRepoSync",
    dids: list[str],
    stop: asyncio.Event,
    *,
    poll_interval_sec: float,
    quiesce_confirmations: int,
) -> None:
    stable = 0
    while not stop.is_set():
        await asyncio.sleep(poll_interval_sec)
        try:
            ok = all(tap.is_repo_active(d) for d in dids) and tap.get_outbox_buffer_count() == 0
            stable = stable + 1 if ok else 0
            if stable >= quiesce_confirmations:
                stop.set()
                return
        except requests.RequestException as e:
            logger.warning(f"Tap poller request error: {e}")


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

    def get_repo_info(self, did: str) -> dict[str, Any] | None:
        """GET /info/:did — repo state, record count, etc. Returns None if repo is not yet tracked."""
        url = f"{self.base_url}/info/{did}"
        resp = requests.get(url, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def get_outbox_buffer_count(self) -> int:
        """GET /stats/outbox-buffer — global pending outbox events (indigo Tap)."""
        url = f"{self.base_url}/stats/outbox-buffer"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "outbox_buffer" in data:
            return int(data["outbox_buffer"])
        return 0

    def is_repo_active(self, did: str) -> bool:
        info = self.get_repo_info(did)
        if not info:
            return False
        return str(info.get("state", "")).lower() == "active"

    async def collect_likes_for_dids(
        self,
        dids: list[str],
        *,
        poll_interval_sec: float = 1.0,
        max_wait_sec: float = 3600.0,
        quiesce_confirmations: int = 2,
        drain_after_stop_sec: float = 3.0,
    ) -> dict[str, tuple[str, str]]:
        """
        Connect to the Tap WebSocket, add liker DIDs, stream app.bsky.feed.like events.

        Stops when every DID is active in /info and the outbox buffer reads 0 for
        quiesce_confirmations consecutive polls (Tap may deliver at-least-once; dedupe by like_uri).

        Returns map like_uri -> (user_did, liked_post_uri).
        """
        if not dids:
            return {}
        likes: dict[str, tuple[str, str]] = {}
        channel_url = f"{self.ws_url}/channel"
        stop = asyncio.Event()

        async def _run_once() -> None:
            poller_task = asyncio.create_task(
                _poll_tap_quiescent(
                    self,
                    dids,
                    stop,
                    poll_interval_sec=poll_interval_sec,
                    quiesce_confirmations=quiesce_confirmations,
                )
            )
            try:
                async with websockets.connect(channel_url) as ws:
                    await asyncio.to_thread(self.add_repos, dids)
                    await _recv_like_events_until_stop(ws, stop, likes)
                    await _drain_like_events_short(ws, likes, drain_after_stop_sec)
            finally:
                poller_task.cancel()
                try:
                    await poller_task
                except asyncio.CancelledError:
                    pass

        try:
            await asyncio.wait_for(_run_once(), timeout=max_wait_sec)
        except asyncio.TimeoutError:
            logger.warning(
                f"Tap collect_likes_for_dids timed out after {max_wait_sec}s; "
                f"collected {len(likes)} distinct likes"
            )
        return likes


def extract_did_from_post_uri(post_uri: str) -> str:
    """From at://DID/app.bsky.feed.post/RKEY return DID."""
    parts = post_uri.split("/")
    if len(parts) < 3:
        raise ValueError(f"Cannot extract DID from post URI: {post_uri}")
    return parts[2]
