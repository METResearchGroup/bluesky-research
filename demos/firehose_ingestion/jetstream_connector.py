"""Connector for the Jetstream pipeline."""
import asyncio
import json
import time
from datetime import datetime
from pprint import pprint
from typing import Optional, Set, Union

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
        self.record_types_seen: Set[str] = set()

    @staticmethod
    def get_public_instances() -> list[str]:
        """Get the public instances.
        
        Returns:
            List of public Jetstream instance hostnames.
        """
        return PUBLIC_INSTANCES

    def generate_uri(self, instance: str, payload: dict) -> str:
        """Generate a URI for a given instance and payload.
        
        Args:
            instance: Jetstream instance hostname.
            payload: Query parameters for the subscription.
        
        Returns:
            WebSocket URI for connecting to the specified instance.
            
        Raises:
            ValueError: If the instance is not a public instance.
        """
        if instance not in PUBLIC_INSTANCES:
            raise ValueError(f"Instance {instance} is not a public instance.")

        query_params = []
        for key, value in payload.items():
            query_params.append(f"{key}={value}")
        query_string = "&".join(query_params)

        return f"wss://{instance}/subscribe?{query_string}"

    async def connect_to_jetstream(self, instance: str, payload: dict) -> dict:
        """Connect to the Jetstream pipeline and consume a single message.
        
        Args:
            instance: Jetstream instance hostname.
            payload: Query parameters for the subscription.
            
        Returns:
            Processed message from the firehose.
        """
        uri = self.generate_uri(instance, payload)
        logger.info(f"Connecting to {uri}")
        async with websockets.connect(uri) as websocket:
            message: str = await websocket.recv()
            message_json: dict = self.process_message(message)
            return message_json

    def process_message(self, message: str) -> dict:
        """Process a raw message from the firehose.
        
        Args:
            message: Raw JSON string from the firehose.
            
        Returns:
            Parsed JSON object.
        """
        message_json: dict = json.loads(message)
        message_json["commit"] = self.process_commit(message_json["commit"])
        return message_json

    def process_commit(self, commit: dict) -> str:
        """Process a commit message.
        
        Args:
            commit: Commit dictionary from the firehose.
            
        Returns:
            JSON string representation of the commit.
        """
        return json.dumps(commit)
        
    def extract_records_from_message(self, message: dict) -> list[dict]:
        """Extract FirehoseRecord objects from a firehose message.
        
        Args:
            message: Processed message from the firehose.
            
        Returns:
            List of FirehoseRecord dictionaries.
        """
        records = []
        
        # Extract operations from the message
        ops = message.get("ops", [])
        for op in ops:
            # Only process create, update, delete operations
            if op.get("action") not in ["create", "update", "delete"]:
                continue
                
            # Get record details
            cid = op.get("cid", "")
            record_raw = op.get("record", {})
            if not cid or not record_raw:
                continue
                
            # Track record type if available
            record_type = record_raw.get("$type", "unknown")
            self.record_types_seen.add(record_type)
            
            # Create FirehoseRecord
            record = {
                "cid": cid,
                "operation": op.get("action", ""),
                "record": json.dumps(record_raw),
                "rev": op.get("rev", ""),
                "rkey": op.get("path", {}).get("rkey", ""),
            }
            records.append(record)
            
        return records
        
    def store_message(self, message: dict) -> int:
        """Process and store records from a firehose message.
        
        Args:
            message: Processed message from the firehose.
            
        Returns:
            Number of records stored.
        """
        records = self.extract_records_from_message(message)
        
        if not records:
            return 0
            
        # Store records in the database
        self.db.batch_add_records(records)
        self.records_stored += len(records)
        
        return len(records)
        
    async def listen_until_count(
        self, 
        instance: str,
        wanted_collections: Union[str, list[str]],
        target_count: int = 1000, 
        max_time: int = 300,
        batch_size: int = 100
    ) -> dict:
        """Listen to the firehose and store records until reaching a target count.
        
        Args:
            instance: Jetstream instance hostname.
            wanted_collections: Collection types to subscribe to.
            target_count: Target number of records to store.
            max_time: Maximum time to listen in seconds.
            batch_size: Batch size for database insertions.
            
        Returns:
            Dictionary with statistics about the ingestion process.
        """
        # Prepare payload
        if isinstance(wanted_collections, list):
            wanted_collections = ",".join(wanted_collections)
            
        payload = {"wantedCollections": wanted_collections}
        
        # Generate URI
        uri = self.generate_uri(instance, payload)
        logger.info(f"Connecting to {uri}")
        logger.info(f"Target count: {target_count} records, max time: {max_time} seconds")
        
        # Initialize tracking variables
        self.start_time = time.time()
        self.messages_received = 0
        self.records_stored = 0
        self.record_types_seen = set()
        
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
                    message: str = await websocket.recv()
                    self.messages_received += 1
                    
                    # Process and store
                    message_json = self.process_message(message)
                    records_stored = self.store_message(message_json)
                    
                    if records_stored > 0:
                        logger.info(f"Stored {records_stored} records (total: {self.records_stored})")
                    
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
                    "record_types": list(self.record_types_seen),
                    "target_reached": self.records_stored >= target_count,
                    "db_stats": self.db.get_stats().__dict__,
                }
                
                logger.info(f"Firehose ingestion completed. Stats: {json.dumps(stats, indent=2)}")
                return stats


if __name__ == "__main__":
    connector = JetstreamConnector(db_name="firehose_demo", create_new_db=True)
    instance = "jetstream2.us-east.bsky.network"
    
    # Option 1: Just get a single message
    # payload = {"wantedCollections": "app.bsky.feed.post"}
    # loop = asyncio.get_event_loop()
    # message = loop.run_until_complete(connector.connect_to_jetstream(instance, payload))
    # records = connector.extract_records_from_message(message)
    # print(f"Extracted {len(records)} records from message")
    
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
