"""Firehose connection handler for the Bluesky firehose ingestion demo."""

import asyncio
import logging
from typing import AsyncGenerator, Dict, Optional

from atproto import (
    AtUri,
    CAR,
    firehose_models,
    FirehoseSubscribeReposClient,
    models,
    parse_subscribe_repos_message,
)
from atproto.exceptions import FirehoseError

from demos.firehose_ingestion.config import settings

logger = logging.getLogger(__name__)

class FirehoseSubscriber:
    """A subscriber for the Bluesky firehose that filters for specific record types."""

    def __init__(self) -> None:
        """Initialize the subscriber."""
        self._record_types = set(settings.RECORD_TYPES)
        self._client = FirehoseSubscribeReposClient()
        self._queue = asyncio.Queue()
        self._running = False

    def _get_ops_by_type(self, commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> dict:
        """Process commit operations and organize by record type."""
        operation_by_type = {
            settings.RECORD_TYPE_DIRS[record_type]: {"created": [], "deleted": []}
            for record_type in settings.RECORD_TYPES
        }

        car = CAR.from_bytes(commit.blocks)
        for op in commit.ops:
            uri = AtUri.from_str(f"at://{commit.repo}/{op.path}")

            if op.action == "create":
                if not op.cid:
                    continue

                create_info = {"uri": str(uri), "cid": str(op.cid), "author": commit.repo}

                record_raw_data = car.blocks.get(op.cid)
                if not record_raw_data:
                    continue

                record = models.get_or_create(record_raw_data, strict=False)

                if uri.collection in self._record_types:
                    record_type_str = settings.RECORD_TYPE_DIRS[uri.collection]
                    operation_by_type[record_type_str]["created"].append(
                        {"record": record, **create_info}
                    )

            if op.action == "delete":
                if uri.collection in self._record_types:
                    record_type_str = settings.RECORD_TYPE_DIRS[uri.collection]
                    operation_by_type[record_type_str]["deleted"].append({"uri": str(uri)})

        return operation_by_type

    def _on_message_handler(self, message: firehose_models.MessageFrame) -> None:
        """Handle incoming firehose messages."""
        try:
            if message.type == "#identity":
                return None
                
            commit = parse_subscribe_repos_message(message)
            if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
                return None

            if not commit.blocks:
                return None

            processed = self._get_ops_by_type(commit)
            if processed:
                self._queue.put_nowait(processed)
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    async def subscribe(self) -> AsyncGenerator[Dict, None]:
        """Subscribe to the firehose and yield processed records."""
        if self._running:
            raise RuntimeError("Subscriber is already running")

        try:
            self._running = True
            self._client.start(self._on_message_handler)

            while self._running:
                try:
                    processed = await self._queue.get()
                    yield processed
                except asyncio.CancelledError:
                    logger.info("Subscription cancelled")
                    break
                except FirehoseError as e:
                    logger.error(f"Firehose error: {e}")
                    # Attempt to restart the client
                    self._client.stop()
                    await asyncio.sleep(1)  # Brief delay before restart
                    self._client.start(self._on_message_handler)
                except Exception as e:
                    logger.error(f"Error in subscription: {e}")
                    break
        except Exception as e:
            logger.error(f"Error in firehose subscription: {e}")
            # Re-raise to allow proper error handling upstream
            raise
        finally:
            self._running = False
            try:
                self._client.stop()
            except Exception as e:
                logger.error(f"Error stopping client: {e}")

    def stop(self) -> None:
        """Stop the subscriber."""
        self._running = False
        try:
            self._client.stop()
        except Exception as e:
            logger.error(f"Error stopping client: {e}") 