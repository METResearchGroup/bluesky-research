#!/usr/bin/env python3
"""
Mock Data Stream Generator

This script generates realistic Bluesky firehose events and writes them to Redis Streams
for testing the DataWriter flow. It simulates the actual Bluesky data pipeline.

Author: AI Assistant
Date: 2025-08-08
"""

import json
import time
import random
import uuid
from datetime import datetime, timezone
from typing import Dict, Any
import signal
import sys
import copy

import redis

# Bluesky event templates
EVENT_TEMPLATES = {
    "app.bsky.feed.post": {
        "collection": "app.bsky.feed.post",
        "repo": "did:plc:user123",
        "rkey": "3juxa2qj2d22q",
        "cid": "bafyreidf6z6qj2d22q",
        "record": {
            "text": "This is a test post from the mock data stream!",
            "createdAt": "2025-08-08T20:00:00.000Z",
            "langs": ["en"],
            "labels": None,
            "tags": None,
        },
        "blobCids": [],
        "embed": None,
        "reply": None,
        "repost": None,
        "indexedAt": "2025-08-08T20:00:00.000Z",
    },
    "app.bsky.feed.like": {
        "collection": "app.bsky.feed.like",
        "repo": "did:plc:user456",
        "rkey": "3juxa2qj2d22r",
        "cid": "bafyreidf6z6qj2d22r",
        "record": {
            "subject": {
                "uri": "at://did:plc:user123/app.bsky.feed.post/3juxa2qj2d22q",
                "cid": "bafyreidf6z6qj2d22q",
            },
            "createdAt": "2025-08-08T20:00:00.000Z",
        },
        "indexedAt": "2025-08-08T20:00:00.000Z",
    },
    "app.bsky.feed.repost": {
        "collection": "app.bsky.feed.repost",
        "repo": "did:plc:user789",
        "rkey": "3juxa2qj2d22s",
        "cid": "bafyreidf6z6qj2d22s",
        "record": {
            "subject": {
                "uri": "at://did:plc:user123/app.bsky.feed.post/3juxa2qj2d22q",
                "cid": "bafyreidf6z6qj2d22q",
            },
            "createdAt": "2025-08-08T20:00:00.000Z",
        },
        "indexedAt": "2025-08-08T20:00:00.000Z",
    },
    "app.bsky.graph.follow": {
        "collection": "app.bsky.graph.follow",
        "repo": "did:plc:user101",
        "rkey": "3juxa2qj2d22t",
        "cid": "bafyreidf6z6qj2d22t",
        "record": {
            "subject": "did:plc:user123",
            "createdAt": "2025-08-08T20:00:00.000Z",
        },
        "indexedAt": "2025-08-08T20:00:00.000Z",
    },
    "app.bsky.graph.block": {
        "collection": "app.bsky.graph.block",
        "repo": "did:plc:user202",
        "rkey": "3juxa2qj2d22u",
        "cid": "bafyreidf6z6qj2d22u",
        "record": {
            "subject": "did:plc:user123",
            "createdAt": "2025-08-08T20:00:00.000Z",
        },
        "indexedAt": "2025-08-08T20:00:00.000Z",
    },
}

# Stream names mapping
STREAM_NAMES = {
    "app.bsky.feed.post": "bluesky_posts",
    "app.bsky.feed.like": "bluesky_likes",
    "app.bsky.feed.repost": "bluesky_reposts",
    "app.bsky.graph.follow": "bluesky_follows",
    "app.bsky.graph.block": "bluesky_blocks",
}


class MockDataStreamGenerator:
    """Generates realistic Bluesky firehose events and writes to Redis Streams"""

    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        password: str | None = None,
    ):
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            password=password,
            decode_responses=True,
            socket_connect_timeout=10,
            socket_timeout=10,
        )
        self.running = False
        self.stats = {
            "total_events": 0,
            "events_by_type": {},
            "start_time": None,
            "last_event_time": None,
        }

        # Test connection
        try:
            self.redis_client.ping()
            print("✅ Connected to Redis")
        except Exception as e:
            print(f"❌ Failed to connect to Redis: {e}")
            raise

        # Ensure consumer group is created once at initialization
        self._ensure_consumer_groups()

    def _ensure_consumer_groups(self) -> None:
        """Create the consumer group for all streams if it doesn't exist."""
        for stream_name in STREAM_NAMES.values():
            try:
                self.redis_client.xgroup_create(
                    stream_name, "datawriter_group", id="0", mkstream=True
                )
            except redis.exceptions.ResponseError as e:
                if "BUSYGROUP" in str(e):
                    # Group already exists; ignore
                    continue
                # Re-raise unexpected errors
                raise

    def generate_event(self, event_type: str) -> Dict[str, Any]:
        """Generate a realistic Bluesky event of the specified type"""
        template = copy.deepcopy(EVENT_TEMPLATES[event_type])

        # Generate unique identifiers
        # Use timezone-aware UTC timestamp with Z suffix
        timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        unique_id = str(uuid.uuid4())[:8]

        # Update template with unique values
        template["rkey"] = f"3juxa2qj2d22{unique_id}"
        template["cid"] = f"bafyreidf6z6qj2d22{unique_id}"
        template["record"]["createdAt"] = timestamp
        template["indexedAt"] = timestamp

        # Add some randomization for posts
        if event_type == "app.bsky.feed.post":
            post_texts = [
                "Just testing the data pipeline! 📊",
                "Mock data generation is working great! 🚀",
                "Redis Streams are amazing for data processing! 💾",
                "Prefect flows make orchestration easy! ⚡",
                "Parquet files are perfect for analytics! 📈",
                "Bluesky data pipeline is coming together! 🌟",
                "Testing the end-to-end workflow! 🔄",
                "Data processing at scale! 📊",
                "Real-time analytics pipeline! ⚡",
                "Stream processing with Redis! 🔥",
            ]
            template["record"]["text"] = random.choice(post_texts)

        return template

    def write_to_stream(self, event: Dict[str, Any]) -> bool:
        """Write event to the appropriate Redis Stream"""
        try:
            collection = event["collection"]
            stream_name = STREAM_NAMES[collection]

            # Write to stream
            message_data = {
                "data": json.dumps(event),
                "timestamp": str(time.time()),
                "collection": collection,
            }

            self.redis_client.xadd(stream_name, message_data)

            # Update stats
            self.stats["total_events"] += 1
            self.stats["events_by_type"][collection] = (
                self.stats["events_by_type"].get(collection, 0) + 1
            )
            self.stats["last_event_time"] = datetime.now(timezone.utc)

            return True

        except Exception as e:
            print(f"❌ Error writing to stream: {e}")
            return False

    def generate_batch(self, batch_size: int = 50) -> int:
        """Generate a batch of events"""
        events_generated = 0

        for _ in range(batch_size):
            # Randomly select event type with weighted distribution
            event_types = list(EVENT_TEMPLATES.keys())
            weights = [
                0.4,
                0.3,
                0.1,
                0.15,
                0.05,
            ]  # posts, likes, reposts, follows, blocks
            event_type = random.choices(event_types, weights=weights)[0]

            # Generate event
            event = self.generate_event(event_type)

            # Write to stream
            if self.write_to_stream(event):
                events_generated += 1

        return events_generated

    def run_stream(self, duration_minutes: int = 10, events_per_second: int = 10):
        """Run the mock data stream for the specified duration"""
        print(f"🚀 Starting mock data stream for {duration_minutes} minutes")
        print(f"📊 Target rate: {events_per_second} events/second")

        self.running = True
        self.stats["start_time"] = datetime.now(timezone.utc)

        # Calculate timing
        total_events = duration_minutes * 60 * events_per_second
        batch_size = max(1, events_per_second // 2)  # Generate in smaller batches
        batch_interval = 1.0 / (events_per_second / batch_size)

        print(f"📈 Target total events: {total_events}")
        print(f"📦 Batch size: {batch_size}")
        print(f"⏱️ Batch interval: {batch_interval:.2f} seconds")

        events_generated = 0
        start_time = time.time()

        try:
            while self.running and events_generated < total_events:
                batch_start = time.time()

                # Generate batch
                batch_events = self.generate_batch(batch_size)
                events_generated += batch_events

                # Calculate timing
                batch_time = time.time() - batch_start
                sleep_time = max(0, batch_interval - batch_time)

                if sleep_time > 0:
                    time.sleep(sleep_time)

                # Print progress every 100 events
                if events_generated % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = events_generated / elapsed if elapsed > 0 else 0
                    print(
                        f"📊 Progress: {events_generated}/{total_events} events ({rate:.1f} events/sec)"
                    )

                # Check if we should stop
                if time.time() - start_time >= duration_minutes * 60:
                    break

        except KeyboardInterrupt:
            print("\n⏹️ Stopping mock data stream...")

        finally:
            self.running = False
            self.print_final_stats()

    def print_final_stats(self):
        """Print final statistics"""
        if self.stats["start_time"]:
            duration = datetime.now(timezone.utc) - self.stats["start_time"]
            total_events = self.stats["total_events"]
            rate = (
                total_events / duration.total_seconds()
                if duration.total_seconds() > 0
                else 0
            )

            print("\n" + "=" * 60)
            print("📊 MOCK DATA STREAM FINAL STATISTICS")
            print("=" * 60)
            print(f"⏱️ Duration: {duration}")
            print(f"📈 Total Events Generated: {total_events}")
            print(f"🚀 Average Rate: {rate:.2f} events/second")
            print(f"🕐 Last Event: {self.stats['last_event_time']}")

            print("\n📋 Events by Type:")
            for event_type, count in self.stats["events_by_type"].items():
                percentage = (count / total_events * 100) if total_events > 0 else 0
                print(f"  {event_type}: {count} ({percentage:.1f}%)")

            print("=" * 60)

    def cleanup(self):
        """Clean up Redis Streams"""
        print("🧹 Cleaning up Redis Streams...")

        for stream_name in STREAM_NAMES.values():
            try:
                # Get stream length
                stream_info = self.redis_client.xinfo_stream(stream_name)
                length = stream_info["length"]
                print(f"📊 {stream_name}: {length} messages")

                # Optionally trim to keep only recent messages
                if length > 1000:
                    self.redis_client.xtrim(stream_name, maxlen=1000, approximate=True)
                    print(f"🧹 Trimmed {stream_name} to 1000 messages")

            except redis.exceptions.ResponseError as e:
                if "no such key" in str(e).lower():
                    print(f"📭 Stream {stream_name} doesn't exist")
                else:
                    print(f"⚠️ Error with stream {stream_name}: {e}")


def main():
    """Main function to run the mock data stream"""
    import argparse

    parser = argparse.ArgumentParser(description="Mock Bluesky Data Stream Generator")
    parser.add_argument(
        "--duration", type=int, default=10, help="Duration in minutes (default: 10)"
    )
    parser.add_argument(
        "--rate", type=int, default=10, help="Events per second (default: 10)"
    )
    parser.add_argument(
        "--redis-host", default="localhost", help="Redis host (default: localhost)"
    )
    parser.add_argument(
        "--redis-port", type=int, default=6379, help="Redis port (default: 6379)"
    )
    parser.add_argument(
        "--redis-password", default=None, help="Redis password (optional)"
    )
    parser.add_argument(
        "--cleanup", action="store_true", help="Clean up streams after running"
    )

    args = parser.parse_args()

    # Create generator
    generator = MockDataStreamGenerator(
        args.redis_host, args.redis_port, args.redis_password
    )

    # Set up signal handler for graceful shutdown
    def signal_handler(sig, frame):
        print("\n⏹️ Received interrupt signal, stopping...")
        generator.running = False

    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Run the stream
        generator.run_stream(args.duration, args.rate)

        # Cleanup if requested
        if args.cleanup:
            generator.cleanup()

    except Exception as e:
        print(f"❌ Error running mock data stream: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
