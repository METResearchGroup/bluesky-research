"""Connector for the Jetstream pipeline."""
import asyncio
import json
import time
from datetime import datetime
from pprint import pprint
from typing import Optional, Union
from urllib.parse import urlencode

import websockets

from demos.firehose_ingestion.db import FirehoseDB, FirehoseRecord
from lib.log.logger import get_logger

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
    process messages, and store them in a SQLite database.
    """

    def __init__(self, db_name: Optional[str] = None, create_new_db: bool = True):
        """Initialize a JetstreamConnector.
        
        Args:
            db_name: Name of the database to use for storing records.
                If None, a timestamped name will be generated.
            create_new_db: Whether to create a new database if it doesn't exist.
        """
        if db_name is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            db_name = f"firehose_{timestamp}"
            
        self.db = FirehoseDB(db_name, create_new_db=create_new_db)
        logger.info(f"Initialized JetstreamConnector with database: {db_name}")
        
        # Track statistics
        self.messages_received = 0
        self.records_stored = 0
        self.start_time = None
        self.collections_seen: set[str] = set()

    @staticmethod
    def get_public_instances() -> list[str]:
        """Get the public instances.
        
        Returns:
            List of public Jetstream instance hostnames.
        """
        return PUBLIC_INSTANCES

    def generate_uri(self, instance: str, params: dict) -> str:
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
            if isinstance(value, list):
                # Add each value as a separate query parameter with the same name
                for val in value:
                    query_params.append(f"{key}={val}")
            else:
                query_params.append(f"{key}={value}")
        
        query_string = "&".join(query_params)
        return f"wss://{instance}/subscribe?{query_string}"

    async def connect_to_jetstream(self, instance: str, params: dict) -> dict:
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
            message_json: dict = json.loads(message)
            return message_json

    def extract_record_from_message(self, message: dict) -> Optional[dict]:
        """Extract a FirehoseRecord from a firehose message.
        
        Args:
            message: Raw message from the firehose.
            
        Returns:
            FirehoseRecord dictionary or None if the message isn't valid.
        """
        if not message or not isinstance(message, dict):
            return None
            
        # Check for required fields
        required_fields = ["did", "time_us", "kind"]
        if not all(field in message for field in required_fields):
            logger.warning(f"Message missing required fields: {message}")
            return None
            
        # For commit messages, extract collection from the commit data
        collection = None
        if message.get("kind") == "commit" and "commit" in message:
            commit = message["commit"]
            collection = commit.get("collection")
            
            # Track collections we've seen
            if collection:
                self.collections_seen.add(collection)
            
        # Create record dictionary
        record = {
            "did": message.get("did", ""),
            "time_us": str(message.get("time_us", "")),
            "kind": message.get("kind", ""),
            "commit": json.dumps(message.get("commit", {})),
            "collection": collection or "",
        }
            
        return record
        
    def store_message(self, message: dict) -> int:
        """Process and store a firehose message.
        
        Args:
            message: Raw message from the firehose.
            
        Returns:
            Number of records stored (0 or 1).
        """
        record = self.extract_record_from_message(message)
        
        if not record:
            return 0
            
        # Store record in the database
        self.db.add_record(record)
        self.records_stored += 1
        
        return 1
        
    async def listen_until_count(
        self, 
        instance: str,
        wanted_collections: Union[str, list[str]],
        target_count: int = 1000, 
        max_time: int = 300,
        cursor: Optional[str] = None,
        wanted_dids: Optional[Union[str, list[str]]] = None
    ) -> dict:
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
            params["wantedDids"] = wanted_dids
            
        # Generate URI
        uri = self.generate_uri(instance, params)
        logger.info(f"Connecting to {uri}")
        logger.info(f"Target count: {target_count} records, max time: {max_time} seconds")
        
        # Initialize tracking variables
        self.start_time = time.time()
        self.messages_received = 0
        self.records_stored = 0
        self.collections_seen = set()
        latest_cursor = None
        
        async with websockets.connect(uri) as websocket:
            try:
                while self.records_stored < target_count and time.time() - self.start_time < max_time:
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
                        records_stored = self.store_message(message)
                        
                        if records_stored > 0:
                            logger.info(f"Stored record #{self.records_stored} with cursor {latest_cursor}")
                        
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse message: {message_str[:200]}...")
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                    
            except Exception as e:
                logger.error(f"Error during firehose ingestion: {e}")
            finally:
                # Calculate statistics
                end_time = time.time()
                total_time = end_time - self.start_time
                
                stats = {
                    "records_stored": self.records_stored,
                    "messages_received": self.messages_received,
                    "total_time": total_time,
                    "records_per_second": self.records_stored / total_time if total_time > 0 else 0,
                    "collections": list(self.collections_seen),
                    "target_reached": self.records_stored >= target_count,
                    "latest_cursor": latest_cursor,
                    "db_stats": self.db.get_stats().__dict__,
                }
                
                logger.info(f"Firehose ingestion completed. Stats: {json.dumps(stats, indent=2)}")
                return stats


if __name__ == "__main__":
    connector = JetstreamConnector(db_name="firehose_demo", create_new_db=True)
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
    wanted_collections = ["app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.graph.follow"]
    stats = loop.run_until_complete(
        connector.listen_until_count(
            instance=instance,
            wanted_collections=wanted_collections,
            target_count=1000,
            max_time=300
        )
    )
    print("Final statistics:")
    pprint(stats)
