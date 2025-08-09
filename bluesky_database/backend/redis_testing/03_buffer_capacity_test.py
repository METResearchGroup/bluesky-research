#!/usr/bin/env python3
"""
Redis Buffer Capacity Testing Script

This script tests Redis buffer capacity with 2.7M realistic Bluesky firehose events
to validate the 8-hour buffer capacity requirement from MET-001.

Usage:
    python 03_buffer_capacity_test.py
"""

import redis
import time
import json
import os
import sys
import uuid
from typing import Dict, Any, List
from datetime import datetime
import statistics


class RedisBufferCapacityTester:
    """Tests Redis buffer capacity with realistic Bluesky firehose events."""

    def __init__(
        self, host: str = "localhost", port: int = 6379, password: str | None = None
    ):
        """Initialize the buffer capacity tester.

        Reads `REDIS_BUFFER_MODE` from environment to choose storage strategy:
        - "list" (default): use LPUSH on key `firehose_buffer`.
        - "stream": use XADD on key `firehose_stream`.
        """
        self.host = host
        self.port = port
        self.password = password
        self.redis_client = None
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "test_config": {},
            "capacity_test": {},
            "memory_analysis": {},
            "recommendations": [],
        }

        # Test configuration
        self.test_config = {
            "target_events": 2700000,  # 2.7M events (8-hour buffer capacity)
            "batch_size": 1000,  # Events per batch
            "max_memory_gb": 2,  # 2GB memory limit
            "memory_threshold_percent": 90,  # Alert at 90% memory usage
            "test_duration_hours": 8,  # Simulate 8 hours of data
            "event_types": [
                "app.bsky.feed.post",
                "app.bsky.feed.like",
                "app.bsky.feed.repost",
                "app.bsky.graph.follow",
                "app.bsky.graph.block",
            ],
        }

        # Storage mode: list (default) or stream
        self.buffer_mode: str = os.getenv("REDIS_BUFFER_MODE", "list").strip().lower()
        self.list_key: str = "firehose_buffer"
        self.stream_key: str = "firehose_stream"

    def connect_to_redis(self) -> bool:
        """Establish connection to Redis server."""
        try:
            self.redis_client = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                decode_responses=True,
                socket_connect_timeout=10,
                socket_timeout=10,
            )

            # Test connection
            self.redis_client.ping()
            print(f"✅ Connected to Redis at {self.host}:{self.port}")
            return True

        except redis.ConnectionError as e:
            print(f"❌ Failed to connect to Redis: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error connecting to Redis: {e}")
            return False

    def generate_realistic_event(
        self, event_type: str, event_id: int
    ) -> Dict[str, Any]:
        """Generate a realistic Bluesky firehose event."""
        timestamp = datetime.now().isoformat()

        # Base event structure
        event = {
            "id": f"event_{event_id:010d}",
            "type": event_type,
            "timestamp": timestamp,
            "uri": f"at://did:plc:{uuid.uuid4().hex[:16]}/app.bsky.feed.post/{uuid.uuid4().hex[:16]}",
            "cid": f"bafyrei{uuid.uuid4().hex[:44]}",
            "created_at": timestamp,
            "indexed_at": timestamp,
        }

        # Add type-specific data
        if event_type == "app.bsky.feed.post":
            event.update(
                {
                    "text": f"This is a test post #{event_id} with some realistic content that might appear in the Bluesky firehose. "
                    * 3,
                    "author": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "langs": ["en"],
                    "labels": [],
                    "tags": ["test", "bluesky", "firehose"],
                    "reply": None,
                    "embed": None,
                    "facets": [],
                }
            )
        elif event_type == "app.bsky.feed.like":
            event.update(
                {
                    "subject": f"at://did:plc:{uuid.uuid4().hex[:16]}/app.bsky.feed.post/{uuid.uuid4().hex[:16]}",
                    "subject_cid": f"bafyrei{uuid.uuid4().hex[:44]}",
                    "author": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "created_at": timestamp,
                }
            )
        elif event_type == "app.bsky.feed.repost":
            event.update(
                {
                    "subject": f"at://did:plc:{uuid.uuid4().hex[:16]}/app.bsky.feed.post/{uuid.uuid4().hex[:16]}",
                    "subject_cid": f"bafyrei{uuid.uuid4().hex[:44]}",
                    "author": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "created_at": timestamp,
                }
            )
        elif event_type == "app.bsky.graph.follow":
            event.update(
                {
                    "subject": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "author": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "created_at": timestamp,
                }
            )
        elif event_type == "app.bsky.graph.block":
            event.update(
                {
                    "subject": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "author": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "created_at": timestamp,
                }
            )

        return event

    def get_memory_usage(self) -> Dict[str, Any]:
        """Get current Redis memory and stats usage.

        Note: `keyspace_hits` and `keyspace_misses` are reported in INFO "stats",
        not in INFO "memory".
        """
        try:
            memory_info = self.redis_client.info("memory")
            stats_info = self.redis_client.info("stats")

            used_memory = memory_info.get("used_memory", 0)
            max_memory = memory_info.get("maxmemory", 0)
            memory_utilization = (
                (used_memory / max_memory * 100) if max_memory > 0 else 0
            )

            return {
                "used_memory_bytes": used_memory,
                "used_memory_mb": used_memory / (1024 * 1024),
                "used_memory_gb": used_memory / (1024 * 1024 * 1024),
                "max_memory_bytes": max_memory,
                "max_memory_gb": max_memory / (1024 * 1024 * 1024),
                "memory_utilization_percent": round(memory_utilization, 2),
                "memory_fragmentation_ratio": memory_info.get(
                    "mem_fragmentation_ratio", 0
                ),
                "keyspace_hits": stats_info.get("keyspace_hits", 0),
                "keyspace_misses": stats_info.get("keyspace_misses", 0),
            }
        except Exception as e:
            print(f"❌ Error getting memory usage: {e}")
            return {}

    def load_events_batch(self, start_id: int, batch_size: int) -> Dict[str, Any]:
        """Load a batch of events into Redis.

        Uses LPUSH into list when `buffer_mode` is "list"; uses XADD into stream
        when `buffer_mode` is "stream". Uses a single pipeline for batching.
        """
        batch_start = time.time()
        events_loaded = 0
        memory_samples = []

        try:
            # Create a single pipeline for the entire chunk to batch operations
            pipe = self.redis_client.pipeline()
            for i in range(batch_size):
                event_id = start_id + i
                event_type = self.test_config["event_types"][
                    event_id % len(self.test_config["event_types"])
                ]
                event = self.generate_realistic_event(event_type, event_id)

                # Serialize once
                value = json.dumps(event, separators=(",", ":"))

                # Queue write per selected mode
                if self.buffer_mode == "stream":
                    # Store as a single field payload for parity with production stream usage
                    pipe.xadd(self.stream_key, {"data": value})
                else:
                    # Default list-based storage
                    pipe.lpush(self.list_key, value)

                events_loaded += 1

                # Sample memory usage every 100 events
                if events_loaded % 100 == 0:
                    memory_usage = self.get_memory_usage()
                    memory_samples.append(memory_usage)

                    # Check memory threshold
                    if (
                        memory_usage.get("memory_utilization_percent", 0)
                        > self.test_config["memory_threshold_percent"]
                    ):
                        print(
                            f"⚠️ Memory threshold exceeded: {memory_usage['memory_utilization_percent']}%"
                        )
                        break

            # Execute the pipeline once after queuing all events in the chunk
            pipe.execute()

            batch_time = time.time() - batch_start

            return {
                "batch_start_id": start_id,
                "events_loaded": events_loaded,
                "batch_time_seconds": batch_time,
                "events_per_second": events_loaded / batch_time
                if batch_time > 0
                else 0,
                "memory_samples": memory_samples,
                "success": True,
            }

        except Exception as e:
            return {
                "batch_start_id": start_id,
                "events_loaded": 0,
                "batch_time_seconds": 0,
                "events_per_second": 0,
                "error": str(e),
                "success": False,
            }

    def test_buffer_capacity(self) -> Dict[str, Any]:
        """Test Redis buffer capacity with 2.7M events."""
        print(
            f"🧪 Testing Redis buffer capacity with {self.test_config['target_events']:,} events..."
        )
        print(f"   Target: {self.test_config['target_events']:,} events")
        print(f"   Batch size: {self.test_config['batch_size']:,} events")
        print(f"   Memory limit: {self.test_config['max_memory_gb']}GB")
        print(f"   Memory threshold: {self.test_config['memory_threshold_percent']}%")

        # Get initial memory state
        initial_memory = self.get_memory_usage()
        print("\n📊 Initial memory state:")
        print(f"   Used memory: {initial_memory.get('used_memory_mb', 0):.2f}MB")
        print(
            f"   Memory utilization: {initial_memory.get('memory_utilization_percent', 0):.2f}%"
        )

        # Calculate batches
        total_events = self.test_config["target_events"]
        batch_size = self.test_config["batch_size"]
        num_batches = (total_events + batch_size - 1) // batch_size

        print("\n📋 Test plan:")
        print(f"   Total batches: {num_batches:,}")
        print(f"   Events per batch: {batch_size:,}")
        print(
            f"   Estimated time: {num_batches * 2:.1f} seconds (assuming 2s per batch)"
        )

        # Start capacity test
        test_start = time.time()
        total_events_loaded = 0
        batch_results = []
        memory_history = []

        try:
            for batch_num in range(num_batches):
                start_id = batch_num * batch_size

                print(
                    f"\n🔄 Loading batch {batch_num + 1:,}/{num_batches:,} (events {start_id:,}-{start_id + batch_size - 1:,})..."
                )

                # Load batch
                batch_result = self.load_events_batch(start_id, batch_size)
                batch_results.append(batch_result)

                if batch_result["success"]:
                    total_events_loaded += batch_result["events_loaded"]

                    # Record memory state
                    current_memory = self.get_memory_usage()
                    memory_history.append(
                        {
                            "batch_num": batch_num + 1,
                            "events_loaded": total_events_loaded,
                            "memory_usage": current_memory,
                            "timestamp": datetime.now().isoformat(),
                        }
                    )

                    print(
                        f"   ✅ Loaded {batch_result['events_loaded']:,} events in {batch_result['batch_time_seconds']:.2f}s"
                    )
                    print(
                        f"   📊 Total loaded: {total_events_loaded:,}/{total_events:,} ({total_events_loaded/total_events*100:.1f}%)"
                    )
                    print(
                        f"   💾 Memory: {current_memory.get('used_memory_mb', 0):.1f}MB ({current_memory.get('memory_utilization_percent', 0):.1f}%)"
                    )

                    # Check if we've reached memory threshold
                    if (
                        current_memory.get("memory_utilization_percent", 0)
                        > self.test_config["memory_threshold_percent"]
                    ):
                        print("⚠️ Memory threshold reached. Stopping test.")
                        break

                    # Check if we've loaded all events
                    if total_events_loaded >= total_events:
                        print(f"✅ All {total_events:,} events loaded successfully!")
                        break

                else:
                    print(
                        f"❌ Batch failed: {batch_result.get('error', 'Unknown error')}"
                    )
                    break

                # Small delay to prevent overwhelming Redis
                time.sleep(0.1)

            test_time = time.time() - test_start

            # Get final memory state
            final_memory = self.get_memory_usage()

            # Calculate statistics
            successful_batches = [r for r in batch_results if r["success"]]
            total_batch_time = sum(r["batch_time_seconds"] for r in successful_batches)
            avg_events_per_second = (
                total_events_loaded / total_batch_time if total_batch_time > 0 else 0
            )

            # Memory analysis
            memory_utilizations = [
                m["memory_usage"]["memory_utilization_percent"]
                for m in memory_history
                if m["memory_usage"]
            ]
            max_memory_utilization = (
                max(memory_utilizations) if memory_utilizations else 0
            )
            avg_memory_utilization = (
                statistics.mean(memory_utilizations) if memory_utilizations else 0
            )

            results = {
                "test_config": self.test_config,
                "test_summary": {
                    "total_events_target": total_events,
                    "total_events_loaded": total_events_loaded,
                    "completion_percent": (total_events_loaded / total_events * 100)
                    if total_events > 0
                    else 0,
                    "test_duration_seconds": test_time,
                    "avg_events_per_second": avg_events_per_second,
                    "total_batches": len(batch_results),
                    "successful_batches": len(successful_batches),
                    "failed_batches": len(batch_results) - len(successful_batches),
                },
                "memory_analysis": {
                    "initial_memory": initial_memory,
                    "final_memory": final_memory,
                    "max_memory_utilization_percent": max_memory_utilization,
                    "avg_memory_utilization_percent": avg_memory_utilization,
                    "memory_history": memory_history,
                },
                "batch_results": batch_results,
                "success": total_events_loaded > 0,
            }

            return results

        except Exception as e:
            print(f"❌ Buffer capacity test failed: {e}")
            return {"error": str(e), "success": False}

    def test_data_integrity(self) -> Dict[str, Any]:
        """Test data integrity by sampling loaded events.

        Reads from list or stream depending on `buffer_mode`.
        """
        print("\n🔍 Testing data integrity...")

        try:
            # Determine source and size
            if self.buffer_mode == "stream":
                # Stream length
                xlen = self.redis_client.xlen(self.stream_key)
                buffer_size = xlen if xlen is not None else 0
                print(f"   Stream size: {buffer_size:,} entries")

                if buffer_size == 0:
                    return {"success": False, "error": "No events found in stream"}

                sample_size = min(100, buffer_size)
                # XRANGE start/end to get the most recent items
                # Use COUNT to limit
                entries = self.redis_client.xrevrange(
                    self.stream_key, count=sample_size
                )
                sample_values = []
                for _id, fields in entries:
                    data = fields.get("data") if isinstance(fields, dict) else None
                    if data is not None:
                        sample_values.append(data)
            else:
                buffer_size = self.redis_client.llen(self.list_key)
                print(f"   Buffer size: {buffer_size:,} events")

                if buffer_size == 0:
                    return {"success": False, "error": "No events found in buffer"}

                sample_size = min(100, buffer_size)
                sample_values = self.redis_client.lrange(
                    self.list_key, 0, sample_size - 1
                )

            valid_events = 0
            invalid_events = 0
            sample_events = []

            for value in sample_values:
                try:
                    event = json.loads(value)

                    # Validate event structure
                    if all(
                        field in event
                        for field in ["id", "type", "timestamp", "uri", "cid"]
                    ):
                        valid_events += 1
                        sample_events.append(
                            {
                                "event_type": event.get("type"),
                                "event_id": event.get("id"),
                                "timestamp": event.get("timestamp"),
                            }
                        )
                    else:
                        invalid_events += 1
                except Exception:
                    invalid_events += 1

            integrity_percent = (
                (valid_events / sample_size * 100) if sample_size > 0 else 0
            )

            results = {
                "buffer_size": buffer_size,
                "sample_size": sample_size,
                "valid_events": valid_events,
                "invalid_events": invalid_events,
                "integrity_percent": integrity_percent,
                "sample_events": sample_events[:5],  # Show first 5 events
                "success": integrity_percent > 95,  # 95% integrity threshold
            }

            print(
                f"   ✅ Data integrity: {integrity_percent:.1f}% ({valid_events}/{sample_size} valid)"
            )

            return results

        except Exception as e:
            print(f"❌ Data integrity test failed: {e}")
            return {"success": False, "error": str(e)}

    def cleanup_test_data(self) -> bool:
        """Clean up test data from Redis.

        Deletes the list or stream depending on `buffer_mode`.
        """
        print("\n🧹 Cleaning up test data...")

        try:
            if self.buffer_mode == "stream":
                xlen = self.redis_client.xlen(self.stream_key)
                count = xlen if xlen is not None else 0
                print(f"   Found {count:,} stream entries to clean up...")
                if count and count > 0:
                    self.redis_client.delete(self.stream_key)
                    print(f"   ✅ Cleaned up {count:,} stream entries")
            else:
                buffer_len = self.redis_client.llen(self.list_key)
                print(f"   Found {buffer_len:,} list entries to clean up...")
                if buffer_len and buffer_len > 0:
                    self.redis_client.delete(self.list_key)
                    print(f"   ✅ Cleaned up {buffer_len:,} list entries")

            # Get final memory state
            final_memory = self.get_memory_usage()
            print(f"   📊 Final memory: {final_memory.get('used_memory_mb', 0):.1f}MB")

            return True

        except Exception as e:
            print(f"❌ Cleanup failed: {e}")
            return False

    def analyze_results(
        self, capacity_results: Dict[str, Any], integrity_results: Dict[str, Any]
    ) -> List[str]:
        """Analyze test results and generate recommendations."""
        recommendations = []

        # Capacity analysis
        test_summary = capacity_results.get("test_summary", {})
        completion_percent = test_summary.get("completion_percent", 0)
        total_events_loaded = test_summary.get("total_events_loaded", 0)
        avg_events_per_second = test_summary.get("avg_events_per_second", 0)

        # Memory analysis
        memory_analysis = capacity_results.get("memory_analysis", {})
        max_memory_utilization = memory_analysis.get(
            "max_memory_utilization_percent", 0
        )
        final_memory = memory_analysis.get("final_memory", {})

        # Integrity analysis
        integrity_percent = integrity_results.get("integrity_percent", 0)

        # Generate recommendations
        if completion_percent >= 95:
            recommendations.append(
                f"✅ Buffer capacity test PASSED: {completion_percent:.1f}% completion ({total_events_loaded:,} events)"
            )
        else:
            recommendations.append(
                f"⚠️ Buffer capacity test PARTIAL: {completion_percent:.1f}% completion ({total_events_loaded:,} events)"
            )

        if avg_events_per_second >= 100:
            recommendations.append(
                f"✅ Performance PASSED: {avg_events_per_second:.1f} events/sec average"
            )
        else:
            recommendations.append(
                f"⚠️ Performance BELOW TARGET: {avg_events_per_second:.1f} events/sec average"
            )

        if max_memory_utilization <= 90:
            recommendations.append(
                f"✅ Memory usage PASSED: Peak utilization {max_memory_utilization:.1f}%"
            )
        else:
            recommendations.append(
                f"⚠️ Memory usage HIGH: Peak utilization {max_memory_utilization:.1f}%"
            )

        if integrity_percent >= 95:
            recommendations.append(
                f"✅ Data integrity PASSED: {integrity_percent:.1f}% integrity"
            )
        else:
            recommendations.append(
                f"⚠️ Data integrity ISSUES: {integrity_percent:.1f}% integrity"
            )

        # Additional insights
        if total_events_loaded >= 2700000:
            recommendations.append("🎯 8-hour buffer capacity validated successfully")

        if final_memory.get("memory_utilization_percent", 0) < 80:
            recommendations.append(
                "💾 Memory headroom available for additional capacity"
            )

        return recommendations

    def run_capacity_test(self) -> Dict[str, Any]:
        """Run the complete buffer capacity test."""
        print("🚀 Redis Buffer Capacity Testing")
        print("=" * 50)

        # Connect to Redis
        if not self.connect_to_redis():
            return self.results

        # Store test configuration
        self.results["test_config"] = self.test_config

        # Run capacity test
        capacity_results = self.test_buffer_capacity()
        self.results["capacity_test"] = capacity_results

        if not capacity_results["success"]:
            print(
                f"❌ Capacity test failed: {capacity_results.get('error', 'Unknown error')}"
            )
            return self.results

        # Test data integrity
        integrity_results = self.test_data_integrity()
        self.results["integrity_test"] = integrity_results

        # Analyze results
        recommendations = self.analyze_results(capacity_results, integrity_results)
        self.results["recommendations"] = recommendations

        return self.results

    def print_report(self, results: Dict[str, Any]):
        """Print a formatted capacity test report."""
        print("\n" + "=" * 60)
        print("📋 REDIS BUFFER CAPACITY TEST REPORT")
        print("=" * 60)
        print(f"Timestamp: {results['timestamp']}")

        # Test configuration
        test_config = results.get("test_config", {})
        print("\n📋 TEST CONFIGURATION")
        print("-" * 30)
        print(f"Target events: {test_config.get('target_events', 0):,}")
        print(f"Batch size: {test_config.get('batch_size', 0):,}")
        print(f"Memory limit: {test_config.get('max_memory_gb', 0)}GB")
        print(f"Memory threshold: {test_config.get('memory_threshold_percent', 0)}%")

        # Capacity test results
        capacity_test = results.get("capacity_test", {})
        if capacity_test.get("success"):
            test_summary = capacity_test.get("test_summary", {})
            print("\n🧪 CAPACITY TEST RESULTS")
            print("-" * 30)
            print(f"Events loaded: {test_summary.get('total_events_loaded', 0):,}")
            print(f"Completion: {test_summary.get('completion_percent', 0):.1f}%")
            print(f"Test duration: {test_summary.get('test_duration_seconds', 0):.1f}s")
            print(
                f"Average rate: {test_summary.get('avg_events_per_second', 0):.1f} events/sec"
            )
            print(f"Successful batches: {test_summary.get('successful_batches', 0)}")

            # Memory analysis
            memory_analysis = capacity_test.get("memory_analysis", {})
            print("\n💾 MEMORY ANALYSIS")
            print("-" * 30)
            initial_memory = memory_analysis.get("initial_memory", {})
            final_memory = memory_analysis.get("final_memory", {})
            print(f"Initial memory: {initial_memory.get('used_memory_mb', 0):.1f}MB")
            print(f"Final memory: {final_memory.get('used_memory_mb', 0):.1f}MB")
            print(
                f"Peak utilization: {memory_analysis.get('max_memory_utilization_percent', 0):.1f}%"
            )
            print(
                f"Average utilization: {memory_analysis.get('avg_memory_utilization_percent', 0):.1f}%"
            )

        # Integrity test results
        integrity_test = results.get("integrity_test", {})
        if integrity_test.get("success"):
            print("\n🔍 DATA INTEGRITY RESULTS")
            print("-" * 30)
            print(f"Buffer size: {integrity_test.get('buffer_size', 0):,} events")
            print(f"Sample size: {integrity_test.get('sample_size', 0)} events")
            print(f"Integrity: {integrity_test.get('integrity_percent', 0):.1f}%")
            print(f"Valid events: {integrity_test.get('valid_events', 0)}")
            print(f"Invalid events: {integrity_test.get('invalid_events', 0)}")

        # Recommendations
        print("\n💡 RECOMMENDATIONS")
        print("-" * 30)
        for recommendation in results.get("recommendations", []):
            print(f"• {recommendation}")

        print("\n" + "=" * 60)

    def save_report(self, results: Dict[str, Any], filename: str = None):
        """Save the capacity test report to a JSON file."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"redis_buffer_capacity_test_{timestamp}.json"

        filepath = os.path.join(os.path.dirname(__file__), filename)

        try:
            with open(filepath, "w") as f:
                json.dump(results, f, indent=2)
            print(f"\n💾 Capacity test report saved to: {filepath}")
        except Exception as e:
            print(f"\n❌ Failed to save capacity test report: {e}")


def main():
    """Main function to run the Redis buffer capacity test."""
    print("🚀 Redis Buffer Capacity Testing for Bluesky Data Pipeline")
    print("=" * 60)

    # Get Redis connection parameters from environment
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_password = os.getenv("REDIS_PASSWORD")

    # Create tester
    tester = RedisBufferCapacityTester(
        host=redis_host, port=redis_port, password=redis_password
    )

    # Run capacity test
    results = tester.run_capacity_test()

    # Print report
    tester.print_report(results)

    # Save report
    tester.save_report(results)

    # Cleanup test data
    if results.get("capacity_test", {}).get("success"):
        cleanup_success = tester.cleanup_test_data()
        if cleanup_success:
            print("\n✅ Test data cleanup completed successfully")
        else:
            print("\n⚠️ Test data cleanup had issues")

    # Return exit code based on results
    capacity_success = results.get("capacity_test", {}).get("success", False)
    integrity_success = results.get("integrity_test", {}).get("success", False)

    if capacity_success and integrity_success:
        print("\n✅ Buffer capacity test completed successfully!")
        return 0
    else:
        print("\n⚠️ Buffer capacity test completed with issues. Review results above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
