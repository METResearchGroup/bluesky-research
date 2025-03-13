"""Connector for the Jetstream pipeline.

This module provides a connector to the Bluesky Jetstream service, which streams
events from the AT Protocol firehose in a simplified JSON format.
"""

import asyncio
import json
import time
from datetime import datetime
from pprint import pprint
from typing import Any, Dict, List, Optional, Union, Tuple

import websockets

from lib.db.queue import Queue
from lib.log.logger import get_logger
from services.sync.jetstream.models import JetstreamRecord

logger = get_logger(__file__)

PUBLIC_INSTANCES = [
    "jetstream1.us-east.bsky.network",
    "jetstream2.us-east.bsky.network",
    "jetstream1.us-west.bsky.network",
    "jetstream2.us-west.bsky.network",
]


class JetstreamConnector:
    """Connector for the Jetstream pipeline.

    This class provides functionality to connect to Bluesky's firehose,
    process messages, and add them to a queue for further processing.
    """

    def __init__(self, queue_name: str = "jetstream_sync", batch_size: int = 100):
        """Initialize a JetstreamConnector.

        Args:
            queue_name: Name of the queue to use for storing records
            batch_size: Number of records to batch together for queue insertion
        """
        self.queue = Queue(queue_name=queue_name, create_new_queue=True)
        self.batch_size = batch_size

        # Track statistics
        self.messages_received = 0
        self.records_stored = 0
        self.start_time = None
        self.collections_seen: set[str] = set()
        self.pending_records: List[Dict[str, Any]] = []

        logger.info(f"Initialized JetstreamConnector with queue: {queue_name}")

    @staticmethod
    def get_public_instances() -> List[str]:
        """Get the public instances.

        Returns:
            List of public Jetstream instance hostnames.
        """
        return PUBLIC_INSTANCES

    def generate_uri(self, instance: str, params: Dict[str, Any]) -> str:
        """Generate a URI for a given instance and parameters.

        Args:
            instance: Jetstream instance hostname.
            params: Query parameters for the subscription, such as:
                - wantedCollections: Collection types to include
                - wantedDids: Specific DIDs to include
                - cursor: Starting position

        Returns:
            WebSocket URI for connecting to the specified instance.

        Raises:
            ValueError: If the instance is not a public instance.
        """
        if instance not in PUBLIC_INSTANCES:
            raise ValueError(f"Instance {instance} is not a public instance.")

        # Handle multi-valued parameters like wantedCollections
        query_params = []
        for key, value in params.items():
            if isinstance(value, (list, tuple)):
                # Add each value as a separate query parameter with the same name
                for val in value:
                    query_params.append(f"{key}={val}")
            else:
                query_params.append(f"{key}={value}")

        query_string = "&".join(query_params)
        return f"wss://{instance}/subscribe?{query_string}"

    async def connect_to_jetstream(
        self, instance: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Connect to the Jetstream pipeline and consume a single message.

        Args:
            instance: Jetstream instance hostname.
            params: Query parameters for the subscription.

        Returns:
            Processed message from the firehose.
        """
        uri = self.generate_uri(instance, params)
        logger.info(f"Connecting to {uri}")
        async with websockets.connect(uri) as websocket:
            message: str = await websocket.recv()
            message_json: Dict[str, Any] = json.loads(message)
            return message_json

    def extract_record_from_message(
        self, message: Dict[str, Any]
    ) -> Optional[JetstreamRecord]:
        """Extract a JetstreamRecord from a firehose message.

        Args:
            message: Raw message from the firehose.

        Returns:
            JetstreamRecord object or None if the message isn't valid.
        """
        if not message or not isinstance(message, dict):
            return None

        # Check for required fields
        required_fields = ["did", "time_us", "kind"]
        if not all(field in message for field in required_fields):
            logger.warning(f"Message missing required fields: {message}")
            return None

        # Get basic fields
        did = message.get("did", "")
        time_us = str(message.get("time_us", ""))
        kind = message.get("kind", "")

        if kind not in ["commit", "identity", "account"]:
            logger.warning(f"Unknown message kind: {kind}")
            return None

        # Create record based on kind
        record_data = {
            "did": did,
            "time_us": time_us,
            "kind": kind,
        }

        # For commit messages, extract collection from the commit data
        if kind == "commit" and "commit" in message:
            commit = message.get("commit", {})
            collection = commit.get("collection", "")
            record_data["commit_data"] = commit
            record_data["collection"] = collection

            # Track collections we've seen
            if collection:
                self.collections_seen.add(collection)

        # For identity messages
        elif kind == "identity" and "identity" in message:
            record_data["identity_data"] = message.get("identity", {})

        # For account messages
        elif kind == "account" and "account" in message:
            record_data["account_data"] = message.get("account", {})

        try:
            return JetstreamRecord(**record_data)
        except Exception as e:
            logger.error(f"Error creating JetstreamRecord: {e}")
            return None

    def store_message(self, message: Dict[str, Any]) -> bool:
        """Process a firehose message and add it to the pending records.

        Args:
            message: Raw message from the firehose.

        Returns:
            True if the message was processed successfully, False otherwise.
        """
        record = self.extract_record_from_message(message)

        if not record:
            return False

        # Add record to pending batch
        self.pending_records.append(record.to_queue_item())
        self.records_stored += 1

        # If we've reached the batch size, flush the records to the queue
        if len(self.pending_records) >= self.batch_size:
            self._flush_pending_records()

        return True

    def _flush_pending_records(self) -> None:
        """Flush pending records to the queue."""
        if not self.pending_records:
            return

        # Add metadata with batch information
        metadata = {
            "batch_timestamp": datetime.now().isoformat(),
            "batch_size": len(self.pending_records),
            "collections": list(self.collections_seen),
        }

        try:
            # Add items to queue
            self.queue.batch_add_items_to_queue(
                items=self.pending_records,
                metadata=metadata,
                batch_size=self.batch_size,
            )

            logger.info(f"Added batch of {len(self.pending_records)} records to queue")
            self.pending_records = []
        except Exception as e:
            logger.error(f"Error adding records to queue: {e}")

    async def listen_until_count(
        self,
        instance: str,
        wanted_collections: Union[str, List[str], Tuple[str, ...]],
        target_count: int = 1000,
        max_time: int = 300,
        cursor: Optional[str] = None,
        wanted_dids: Optional[Union[str, List[str], Tuple[str, ...]]] = None,
    ) -> Dict[str, Any]:
        """Listen to the firehose and store records until reaching a target count.

        Args:
            instance: Jetstream instance hostname.
            wanted_collections: Collection type(s) to subscribe to.
            target_count: Target number of records to store.
            max_time: Maximum time to listen in seconds.
            cursor: Optional cursor for starting at a specific point.
            wanted_dids: Optional specific DIDs to filter for.

        Returns:
            Dictionary with statistics about the ingestion process.
        """
        # Prepare parameters
        params = {}

        # Handle collections parameter
        if isinstance(wanted_collections, str):
            if "," in wanted_collections:
                wanted_collections = wanted_collections.split(",")
            else:
                wanted_collections = [wanted_collections]
        elif isinstance(wanted_collections, tuple):
            # Convert tuple to list
            wanted_collections = list(wanted_collections)

        params["wantedCollections"] = wanted_collections

        # Add optional cursor
        if cursor:
            params["cursor"] = cursor

        # Add optional DIDs
        if wanted_dids:
            if isinstance(wanted_dids, str):
                if "," in wanted_dids:
                    wanted_dids = wanted_dids.split(",")
                else:
                    wanted_dids = [wanted_dids]
            elif isinstance(wanted_dids, tuple):
                # Convert tuple to list
                wanted_dids = list(wanted_dids)

            params["wantedDids"] = wanted_dids

        # Generate URI
        uri = self.generate_uri(instance, params)
        logger.info(f"Connecting to {uri}")
        logger.info(
            f"Target count: {target_count} records, max time: {max_time} seconds"
        )

        # Initialize tracking variables
        self.start_time = time.time()
        self.messages_received = 0
        self.records_stored = 0
        self.collections_seen = set()
        self.pending_records = []
        latest_cursor = None

        async with websockets.connect(uri) as websocket:
            try:
                while (
                    self.records_stored < target_count
                    and time.time() - self.start_time < max_time
                ):
                    # Print progress every 100 records
                    if self.records_stored % 100 == 0 and self.records_stored > 0:
                        elapsed = time.time() - self.start_time
                        logger.info(
                            f"Progress: {self.records_stored}/{target_count} records "
                            f"({self.records_stored/target_count*100:.1f}%) in {elapsed:.1f}s"
                        )

                    # Receive message
                    try:
                        message_str = await websocket.recv()
                        self.messages_received += 1
                    except websockets.exceptions.ConnectionClosed:
                        logger.error("WebSocket connection closed")
                        break

                    # Parse and store
                    try:
                        message = json.loads(message_str)

                        # Update cursor for resuming
                        if "time_us" in message:
                            latest_cursor = str(message["time_us"])

                        # Store the message
                        success = self.store_message(message)

                        if success:
                            logger.debug(
                                f"Stored record #{self.records_stored} with cursor {latest_cursor}"
                            )

                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse message: {message_str[:200]}...")
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")

            except Exception as e:
                logger.error(f"Error during firehose ingestion: {e}")
            finally:
                # Make sure to flush any remaining records
                self._flush_pending_records()

                # Calculate statistics
                end_time = time.time()
                total_time = end_time - self.start_time

                # Get queue length
                queue_length = self.queue.get_queue_length()

                stats = {
                    "records_stored": self.records_stored,
                    "messages_received": self.messages_received,
                    "total_time": total_time,
                    "records_per_second": self.records_stored / total_time
                    if total_time > 0
                    else 0,
                    "collections": list(self.collections_seen),
                    "target_reached": self.records_stored >= target_count,
                    "latest_cursor": latest_cursor,
                    "queue_length": queue_length,
                }

                logger.info(
                    f"Firehose ingestion completed. Stats: {json.dumps(stats, indent=2)}"
                )
                return stats


if __name__ == "__main__":
    connector = JetstreamConnector(queue_name="jetstream_sync")
    instance = "jetstream2.us-east.bsky.network"

    # Option 1: Just get a single message
    # loop = asyncio.get_event_loop()
    # message = loop.run_until_complete(
    #     connector.connect_to_jetstream(
    #         instance=instance,
    #         params={"wantedCollections": ["app.bsky.feed.post"]}
    #     )
    # )
    # print("Sample message:")
    # pprint(message)

    # Option 2: Listen until we have 1000 records
    loop = asyncio.get_event_loop()
    wanted_collections = [
        "app.bsky.feed.post",
        "app.bsky.feed.like",
        "app.bsky.graph.follow",
    ]
    stats = loop.run_until_complete(
        connector.listen_until_count(
            instance=instance,
            wanted_collections=wanted_collections,
            target_count=1000,
            max_time=300,
        )
    )
    print("Final statistics:")
    pprint(stats)
