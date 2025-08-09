#!/usr/bin/env python3
"""
Jetstream Load Testing System

This script simulates the Bluesky Jetstream firehose and demonstrates a complete
data pipeline using Redis Streams for buffering and parallel data writers.

Components:
1. Jetstream Firehose Simulator - publishes events in bursts
2. Data Ingestion - receives events and writes to Redis Streams
3. Redis Streams - buffers the data
4. Data Writers - parallel jobs that consume from streams and write to Parquet
5. Telemetry - real-time monitoring of the pipeline
"""

import json
import time
import uuid
import logging
import threading
from datetime import datetime
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
from pathlib import Path
import redis
import polars as pl
from collections import defaultdict, deque
import queue
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants from the sync service
VALID_COLLECTIONS = [
    "app.bsky.feed.post",
    "app.bsky.feed.like",
    "app.bsky.feed.repost",
    "app.bsky.graph.follow",
    "app.bsky.graph.block",
]


@dataclass
class FirehoseEvent:
    """Represents a firehose event with metadata."""

    collection: str
    data: Dict[str, Any]
    timestamp: float
    event_id: str
    source: str = "jetstream_simulator"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "collection": self.collection,
            "data": self.data,
            "timestamp": self.timestamp,
            "event_id": self.event_id,
            "source": self.source,
        }


class TelemetryCollector:
    """Collects and reports telemetry data for the pipeline."""

    def __init__(self):
        self.stats = {
            "events_published": 0,
            "events_ingested": 0,
            "events_written": 0,
            "errors": 0,
            "start_time": time.time(),
            "collection_stats": defaultdict(int),
            "per_second_stats": deque(maxlen=60),  # Keep last 60 seconds
        }
        self.lock = threading.Lock()
        self.running = True
        self.telemetry_thread = None

    def record_event_published(self, collection: str):
        """Record an event being published."""
        with self.lock:
            self.stats["events_published"] += 1
            self.stats["collection_stats"][collection] += 1

    def record_event_ingested(self, collection: str):
        """Record an event being ingested."""
        with self.lock:
            self.stats["events_ingested"] += 1

    def record_event_written(self, collection: str):
        """Record an event being written to storage."""
        with self.lock:
            self.stats["events_written"] += 1

    def record_error(self):
        """Record an error."""
        with self.lock:
            self.stats["errors"] += 1

    def update_per_second_stats(self):
        """Update per-second statistics."""
        with self.lock:
            current_stats = {
                "timestamp": time.time(),
                "events_published": self.stats["events_published"],
                "events_ingested": self.stats["events_ingested"],
                "events_written": self.stats["events_written"],
                "errors": self.stats["errors"],
            }
            self.stats["per_second_stats"].append(current_stats)

    def get_current_stats(self) -> Dict[str, Any]:
        """Get current statistics."""
        with self.lock:
            elapsed = time.time() - self.stats["start_time"]
            return {
                "elapsed_seconds": elapsed,
                "events_published": self.stats["events_published"],
                "events_ingested": self.stats["events_ingested"],
                "events_written": self.stats["events_written"],
                "errors": self.stats["errors"],
                "publish_rate": self.stats["events_published"] / elapsed
                if elapsed > 0
                else 0,
                "ingest_rate": self.stats["events_ingested"] / elapsed
                if elapsed > 0
                else 0,
                "write_rate": self.stats["events_written"] / elapsed
                if elapsed > 0
                else 0,
                "collection_stats": dict(self.stats["collection_stats"]),
            }

    def start_telemetry_reporting(self):
        """Start the telemetry reporting thread."""
        self.telemetry_thread = threading.Thread(
            target=self._telemetry_loop, daemon=True
        )
        self.telemetry_thread.start()

    def _telemetry_loop(self):
        """Telemetry reporting loop."""
        while self.running:
            try:
                self.update_per_second_stats()
                stats = self.get_current_stats()

                # Clear screen and print telemetry
                print("\033[2J\033[H")  # Clear screen
                print("🚀 Jetstream Load Test - Live Telemetry")
                print("=" * 60)
                print(f"⏱️  Elapsed: {stats['elapsed_seconds']:.1f}s")
                print(f"📊 Events Published: {stats['events_published']:,}")
                print(f"📥 Events Ingested: {stats['events_ingested']:,}")
                print(f"💾 Events Written: {stats['events_written']:,}")
                print(f"❌ Errors: {stats['errors']}")
                print()
                print("📈 Rates (events/sec):")
                print(f"   Publish: {stats['publish_rate']:.1f}")
                print(f"   Ingest: {stats['ingest_rate']:.1f}")
                print(f"   Write: {stats['write_rate']:.1f}")
                print()
                print("📋 Collection Breakdown:")
                for collection, count in stats["collection_stats"].items():
                    print(f"   {collection}: {count:,}")
                print()
                print("🔄 Press Ctrl+C to stop...")

                time.sleep(1)
            except Exception as e:
                logger.error(f"Telemetry error: {e}")

    def stop(self):
        """Stop telemetry reporting."""
        self.running = False
        if self.telemetry_thread:
            self.telemetry_thread.join(timeout=1)


class JetstreamSimulator:
    """Simulates the Bluesky Jetstream firehose."""

    def __init__(self, telemetry: TelemetryCollector):
        self.telemetry = telemetry
        self.running = True

    def generate_event(self, collection: str) -> FirehoseEvent:
        """Generate a realistic firehose event."""
        event_id = str(uuid.uuid4())

        # Generate realistic data based on collection type
        if collection == "app.bsky.feed.post":
            data = {
                "uri": f"at://did:plc:{uuid.uuid4().hex[:16]}/app.bsky.feed.post/{uuid.uuid4().hex[:16]}",
                "cid": f"bafyrei{uuid.uuid4().hex[:40]}",
                "author": f"did:plc:{uuid.uuid4().hex[:16]}",
                "text": f"Test post content {uuid.uuid4().hex[:8]}",
                "createdAt": datetime.now().isoformat(),
                "indexedAt": datetime.now().isoformat(),
            }
        elif collection == "app.bsky.feed.like":
            data = {
                "uri": f"at://did:plc:{uuid.uuid4().hex[:16]}/app.bsky.feed.post/{uuid.uuid4().hex[:16]}",
                "cid": f"bafyrei{uuid.uuid4().hex[:40]}",
                "author": f"did:plc:{uuid.uuid4().hex[:16]}",
                "createdAt": datetime.now().isoformat(),
                "indexedAt": datetime.now().isoformat(),
            }
        elif collection == "app.bsky.feed.repost":
            data = {
                "uri": f"at://did:plc:{uuid.uuid4().hex[:16]}/app.bsky.feed.post/{uuid.uuid4().hex[:16]}",
                "cid": f"bafyrei{uuid.uuid4().hex[:40]}",
                "author": f"did:plc:{uuid.uuid4().hex[:16]}",
                "subject": {
                    "uri": f"at://did:plc:{uuid.uuid4().hex[:16]}/app.bsky.feed.post/{uuid.uuid4().hex[:16]}",
                    "cid": f"bafyrei{uuid.uuid4().hex[:40]}",
                },
                "createdAt": datetime.now().isoformat(),
                "indexedAt": datetime.now().isoformat(),
            }
        elif collection == "app.bsky.graph.follow":
            data = {
                "uri": f"at://did:plc:{uuid.uuid4().hex[:16]}/app.bsky.graph.follow/{uuid.uuid4().hex[:16]}",
                "cid": f"bafyrei{uuid.uuid4().hex[:40]}",
                "author": f"did:plc:{uuid.uuid4().hex[:16]}",
                "subject": f"did:plc:{uuid.uuid4().hex[:16]}",
                "createdAt": datetime.now().isoformat(),
                "indexedAt": datetime.now().isoformat(),
            }
        elif collection == "app.bsky.graph.block":
            data = {
                "uri": f"at://did:plc:{uuid.uuid4().hex[:16]}/app.bsky.graph.block/{uuid.uuid4().hex[:16]}",
                "cid": f"bafyrei{uuid.uuid4().hex[:40]}",
                "author": f"did:plc:{uuid.uuid4().hex[:16]}",
                "subject": f"did:plc:{uuid.uuid4().hex[:16]}",
                "createdAt": datetime.now().isoformat(),
                "indexedAt": datetime.now().isoformat(),
            }
        else:
            data = {"unknown": True}

        return FirehoseEvent(
            collection=collection, data=data, timestamp=time.time(), event_id=event_id
        )

    def publish_burst(self, collection: str, count: int, events_queue: queue.Queue):
        """Publish a burst of events for a collection."""
        for _ in range(count):
            if not self.running:
                break

            event = self.generate_event(collection)
            events_queue.put(event)
            self.telemetry.record_event_published(collection)

    def run_simulation(
        self,
        events_queue: queue.Queue,
        events_per_collection: int = 1000,
        burst_size: int = 50,
    ):
        """Run the complete simulation."""
        logger.info(
            f"Starting Jetstream simulation: {events_per_collection} events per collection, {burst_size} per burst"
        )

        for collection in VALID_COLLECTIONS:
            logger.info(f"Publishing {events_per_collection} events for {collection}")

            # Publish in bursts
            remaining = events_per_collection
            while remaining > 0 and self.running:
                burst_count = min(burst_size, remaining)
                self.publish_burst(collection, burst_count, events_queue)
                remaining -= burst_count

                # Rate limiting: 50 events per second
                time.sleep(burst_count / 50.0)

        logger.info("Jetstream simulation completed")

    def stop(self):
        """Stop the simulator."""
        self.running = False


class DataIngestion:
    """Handles data ingestion into Redis Streams."""

    def __init__(self, redis_client: redis.Redis, telemetry: TelemetryCollector):
        self.redis = redis_client
        self.telemetry = telemetry
        self.running = True
        self.stream_prefix = "firehose"

    def ingest_event(self, event: FirehoseEvent):
        """Ingest a single event into Redis Streams."""
        try:
            # Create stream name for the collection
            stream_name = f"{self.stream_prefix}:{event.collection}"

            # Add event to stream
            message_data = {
                "event_id": event.event_id,
                "collection": event.collection,
                "data": json.dumps(event.data),
                "timestamp": str(event.timestamp),
                "source": event.source,
            }

            self.redis.xadd(
                stream_name,
                message_data,
                maxlen=100000,  # Keep last 100k messages
                approximate=True,
            )

            self.telemetry.record_event_ingested(event.collection)

        except Exception as e:
            logger.error(f"Failed to ingest event {event.event_id}: {e}")
            self.telemetry.record_error()

    def run_ingestion(self, events_queue: queue.Queue):
        """Run the ingestion loop."""
        logger.info("Starting data ingestion")

        while self.running:
            try:
                # Get event from queue with timeout
                event = events_queue.get(timeout=1)
                self.ingest_event(event)
                events_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Ingestion error: {e}")
                self.telemetry.record_error()

        logger.info("Data ingestion stopped")

    def stop(self):
        """Stop ingestion."""
        self.running = False


class DataWriter:
    """Writes data from Redis Streams to Parquet files."""

    def __init__(
        self,
        redis_client: redis.Redis,
        telemetry: TelemetryCollector,
        collection: str,
        writer_id: int,
    ):
        self.redis = redis_client
        self.telemetry = telemetry
        self.collection = collection
        self.writer_id = writer_id
        self.running = True
        self.stream_name = f"firehose:{collection}"
        self.consumer_group = f"writer_group_{collection}"
        self.consumer_name = f"writer_{writer_id}"

        # Set up consumer group
        self._setup_consumer_group()

        # Create output directory
        self.output_dir = (
            Path("data") / datetime.now().strftime("%Y_%m_%d") / collection
        )
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Buffer for batching writes
        self.buffer = []
        self.buffer_size = 100
        self.last_write_time = time.time()

    def _setup_consumer_group(self):
        """Set up the consumer group for the stream."""
        try:
            self.redis.xgroup_create(
                self.stream_name, self.consumer_group, id="0", mkstream=True
            )
            logger.info(
                f"Created consumer group {self.consumer_group} for {self.collection}"
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info(f"Consumer group {self.consumer_group} already exists")
            else:
                raise

    def write_buffer_to_parquet(self):
        """Write buffered data to Parquet file."""
        if not self.buffer:
            return

        try:
            # Convert buffer to Polars DataFrame
            df = pl.DataFrame(self.buffer)

            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data_{timestamp}_{self.writer_id}.parquet"
            filepath = self.output_dir / filename

            # Write to Parquet
            df.write_parquet(filepath)

            # Record written events
            for record in self.buffer:
                self.telemetry.record_event_written(self.collection)

            logger.info(
                f"Writer {self.writer_id} wrote {len(self.buffer)} events to {filepath}"
            )

            # Clear buffer
            self.buffer = []
            self.last_write_time = time.time()

        except Exception as e:
            logger.error(f"Failed to write buffer for {self.collection}: {e}")
            self.telemetry.record_error()

    def process_messages(self, messages: List[Tuple[str, Dict]]):
        """Process a batch of messages."""
        for message_id, message_data in messages:
            try:
                # Parse event data
                event_data = {
                    "event_id": message_data["event_id"],
                    "collection": message_data["collection"],
                    "data": json.loads(message_data["data"]),
                    "timestamp": float(message_data["timestamp"]),
                    "source": message_data["source"],
                    "message_id": message_id,
                }

                # Add to buffer
                self.buffer.append(event_data)

                # Acknowledge message
                self.redis.xack(self.stream_name, self.consumer_group, message_id)

                # Write buffer if it's full or enough time has passed
                if (
                    len(self.buffer) >= self.buffer_size
                    or time.time() - self.last_write_time > 5
                ):
                    self.write_buffer_to_parquet()

            except Exception as e:
                logger.error(f"Failed to process message {message_id}: {e}")
                self.telemetry.record_error()

    def run_writer(self):
        """Run the data writer loop."""
        logger.info(f"Starting data writer {self.writer_id} for {self.collection}")

        while self.running:
            try:
                # Read messages from stream
                messages = self.redis.xreadgroup(
                    self.consumer_group,
                    self.consumer_name,
                    {self.stream_name: ">"},
                    count=50,
                    block=1000,
                )

                if messages:
                    for stream_name, stream_messages in messages:
                        self.process_messages(stream_messages)

                # Write any remaining buffer
                if self.buffer and time.time() - self.last_write_time > 1:
                    self.write_buffer_to_parquet()

            except Exception as e:
                logger.error(f"Writer {self.writer_id} error: {e}")
                self.telemetry.record_error()
                time.sleep(1)

        # Final write
        if self.buffer:
            self.write_buffer_to_parquet()

        logger.info(f"Data writer {self.writer_id} for {self.collection} stopped")

    def stop(self):
        """Stop the writer."""
        self.running = False


def main():
    """Main function to run the load test."""
    print("🚀 Starting Jetstream Load Test")
    print("=" * 50)

    # Initialize Redis connection
    try:
        redis_client = redis.Redis(
            host="localhost",
            port=6379,
            db=0,
            decode_responses=True,  # Changed to True to decode byte strings to regular strings
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        redis_client.ping()
        print("✅ Redis connection established")
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}")
        return

    # Initialize telemetry
    telemetry = TelemetryCollector()
    telemetry.start_telemetry_reporting()

    # Initialize components
    events_queue = queue.Queue(maxsize=10000)
    simulator = JetstreamSimulator(telemetry)
    ingestion = DataIngestion(redis_client, telemetry)

    # Start ingestion thread
    ingestion_thread = threading.Thread(
        target=ingestion.run_ingestion, args=(events_queue,), daemon=True
    )
    ingestion_thread.start()

    # Start data writers (one per collection)
    writer_threads = []
    for collection in VALID_COLLECTIONS:
        writer = DataWriter(redis_client, telemetry, collection, len(writer_threads))
        thread = threading.Thread(target=writer.run_writer, daemon=True)
        thread.start()
        writer_threads.append((writer, thread))

    # Handle graceful shutdown
    def signal_handler(signum, frame):
        print("\n🛑 Shutting down gracefully...")
        simulator.stop()
        ingestion.stop()
        for writer, _ in writer_threads:
            writer.stop()
        telemetry.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Run simulation
        print("🎯 Starting simulation...")
        simulator.run_simulation(
            events_queue, events_per_collection=1000, burst_size=50
        )

        # Wait for queue to be processed
        print("⏳ Waiting for all events to be processed...")
        events_queue.join()

        # Wait a bit more for writers to finish
        time.sleep(5)

        # Final statistics
        final_stats = telemetry.get_current_stats()
        print("\n" + "=" * 60)
        print("📊 FINAL STATISTICS")
        print("=" * 60)
        print(f"Total Events Published: {final_stats['events_published']:,}")
        print(f"Total Events Ingested: {final_stats['events_ingested']:,}")
        print(f"Total Events Written: {final_stats['events_written']:,}")
        print(f"Total Errors: {final_stats['errors']}")
        print(f"Average Publish Rate: {final_stats['publish_rate']:.1f} events/sec")
        print(f"Average Ingest Rate: {final_stats['ingest_rate']:.1f} events/sec")
        print(f"Average Write Rate: {final_stats['write_rate']:.1f} events/sec")

    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    finally:
        # Cleanup
        simulator.stop()
        ingestion.stop()
        for writer, _ in writer_threads:
            writer.stop()
        telemetry.stop()


if __name__ == "__main__":
    main()
