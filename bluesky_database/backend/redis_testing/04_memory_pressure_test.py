#!/usr/bin/env python3
"""
Redis Memory Pressure Testing Script

This script tests Redis behavior under memory pressure and eviction scenarios
to validate that the allkeys-lru eviction policy works correctly when approaching
memory limits.

Usage:
    python 04_memory_pressure_test.py
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


class RedisMemoryPressureTester:
    """Tests Redis behavior under memory pressure and eviction scenarios."""

    def __init__(self, host: str = "localhost", port: int = 6379, password: str = None):
        """Initialize the memory pressure tester."""
        self.host = host
        self.port = port
        self.password = password
        self.redis_client = None
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "test_config": {},
            "pressure_test": {},
            "eviction_analysis": {},
            "recovery_test": {},
            "recommendations": [],
        }

        # Test configuration
        self.test_config = {
            "target_memory_percent": 95,  # Target memory utilization
            "pressure_threshold": 90,  # Memory threshold to trigger pressure
            "eviction_batch_size": 1000,  # Events per batch during pressure
            "monitoring_interval": 1,  # Seconds between memory checks
            "pressure_duration": 300,  # Seconds to maintain pressure
            "recovery_duration": 60,  # Seconds to monitor recovery
            "event_types": [
                "app.bsky.feed.post",
                "app.bsky.feed.like",
                "app.bsky.feed.repost",
                "app.bsky.graph.follow",
                "app.bsky.graph.block",
            ],
        }

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

    def generate_pressure_event(self, event_type: str, event_id: int) -> Dict[str, Any]:
        """Generate a realistic Bluesky firehose event for pressure testing."""
        timestamp = datetime.now().isoformat()

        # Base event structure
        event = {
            "id": f"pressure_event_{event_id:010d}",
            "type": event_type,
            "timestamp": timestamp,
            "uri": f"at://did:plc:{uuid.uuid4().hex[:16]}/app.bsky.feed.post/{uuid.uuid4().hex[:16]}",
            "cid": f"bafyrei{uuid.uuid4().hex[:44]}",
            "created_at": timestamp,
            "indexed_at": timestamp,
        }

        # Add type-specific data with larger content for memory pressure
        if event_type == "app.bsky.feed.post":
            event.update(
                {
                    "text": f"This is a pressure test post #{event_id} with extended content to increase memory usage. "
                    * 10,
                    "author": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "langs": ["en"],
                    "labels": [],
                    "tags": ["pressure", "test", "bluesky", "firehose", "memory"],
                    "reply": None,
                    "embed": {
                        "type": "app.bsky.embed.images",
                        "images": [
                            {
                                "alt": f"Pressure test image {event_id}",
                                "image": {
                                    "ref": f"bafkreib{uuid.uuid4().hex[:44]}",
                                    "mimeType": "image/jpeg",
                                    "size": 1024000,
                                },
                            }
                        ],
                    },
                    "facets": [
                        {
                            "index": {"byteStart": 0, "byteEnd": 10},
                            "features": [
                                {
                                    "$type": "app.bsky.richtext.facet#tag",
                                    "tag": "pressure",
                                }
                            ],
                        }
                    ],
                }
            )
        elif event_type == "app.bsky.feed.like":
            event.update(
                {
                    "subject": f"at://did:plc:{uuid.uuid4().hex[:16]}/app.bsky.feed.post/{uuid.uuid4().hex[:16]}",
                    "subject_cid": f"bafyrei{uuid.uuid4().hex[:44]}",
                    "author": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "created_at": timestamp,
                    "metadata": {
                        "pressure_test": True,
                        "event_id": event_id,
                        "timestamp": timestamp,
                    },
                }
            )
        elif event_type == "app.bsky.feed.repost":
            event.update(
                {
                    "subject": f"at://did:plc:{uuid.uuid4().hex[:16]}/app.bsky.feed.post/{uuid.uuid4().hex[:16]}",
                    "subject_cid": f"bafyrei{uuid.uuid4().hex[:44]}",
                    "author": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "created_at": timestamp,
                    "metadata": {
                        "pressure_test": True,
                        "event_id": event_id,
                        "timestamp": timestamp,
                    },
                }
            )
        elif event_type == "app.bsky.graph.follow":
            event.update(
                {
                    "subject": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "author": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "created_at": timestamp,
                    "metadata": {
                        "pressure_test": True,
                        "event_id": event_id,
                        "timestamp": timestamp,
                    },
                }
            )
        elif event_type == "app.bsky.graph.block":
            event.update(
                {
                    "subject": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "author": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "created_at": timestamp,
                    "metadata": {
                        "pressure_test": True,
                        "event_id": event_id,
                        "timestamp": timestamp,
                    },
                }
            )

        return event

    def get_memory_usage(self) -> Dict[str, Any]:
        """Get current Redis memory usage."""
        try:
            memory_info = self.redis_client.info("memory")

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
                "keyspace_hits": memory_info.get("keyspace_hits", 0),
                "keyspace_misses": memory_info.get("keyspace_misses", 0),
                "evicted_keys": memory_info.get("evicted_keys", 0),
                "expired_keys": memory_info.get("expired_keys", 0),
            }
        except Exception as e:
            print(f"❌ Error getting memory usage: {e}")
            return {}

    def get_redis_stats(self) -> Dict[str, Any]:
        """Get comprehensive Redis statistics."""
        try:
            stats = self.redis_client.info()

            return {
                "total_commands_processed": stats.get("total_commands_processed", 0),
                "total_connections_received": stats.get(
                    "total_connections_received", 0
                ),
                "total_net_input_bytes": stats.get("total_net_input_bytes", 0),
                "total_net_output_bytes": stats.get("total_net_output_bytes", 0),
                "instantaneous_ops_per_sec": stats.get("instantaneous_ops_per_sec", 0),
                "instantaneous_input_kbps": stats.get("instantaneous_input_kbps", 0),
                "instantaneous_output_kbps": stats.get("instantaneous_output_kbps", 0),
                "rejected_connections": stats.get("rejected_connections", 0),
                "sync_full": stats.get("sync_full", 0),
                "sync_partial_ok": stats.get("sync_partial_ok", 0),
                "sync_partial_err": stats.get("sync_partial_err", 0),
                "expired_keys": stats.get("expired_keys", 0),
                "evicted_keys": stats.get("evicted_keys", 0),
                "keyspace_hits": stats.get("keyspace_hits", 0),
                "keyspace_misses": stats.get("keyspace_misses", 0),
                "pubsub_channels": stats.get("pubsub_channels", 0),
                "pubsub_patterns": stats.get("pubsub_patterns", 0),
                "latest_fork_usec": stats.get("latest_fork_usec", 0),
                "migrate_cached_sockets": stats.get("migrate_cached_sockets", 0),
                "slave_expires_tracked_keys": stats.get(
                    "slave_expires_tracked_keys", 0
                ),
                "active_defrag_hits": stats.get("active_defrag_hits", 0),
                "active_defrag_misses": stats.get("active_defrag_misses", 0),
                "active_defrag_key_hits": stats.get("active_defrag_key_hits", 0),
                "active_defrag_key_misses": stats.get("active_defrag_key_misses", 0),
            }
        except Exception as e:
            print(f"❌ Error getting Redis stats: {e}")
            return {}

    def load_pressure_events(self, target_memory_percent: float) -> Dict[str, Any]:
        """Load events until reaching target memory utilization."""
        print(
            f"🔄 Loading events to reach {target_memory_percent}% memory utilization..."
        )

        events_loaded = 0
        memory_history = []
        start_time = time.time()

        try:
            while True:
                # Check current memory usage
                current_memory = self.get_memory_usage()
                memory_history.append(
                    {
                        "timestamp": datetime.now().isoformat(),
                        "events_loaded": events_loaded,
                        "memory_usage": current_memory,
                    }
                )

                current_utilization = current_memory.get(
                    "memory_utilization_percent", 0
                )

                print(
                    f"   📊 Memory: {current_memory.get('used_memory_mb', 0):.1f}MB ({current_utilization:.1f}%) - Events: {events_loaded:,}"
                )

                # Check if we've reached target
                if current_utilization >= target_memory_percent:
                    print(
                        f"✅ Reached target memory utilization: {current_utilization:.1f}%"
                    )
                    break

                # Load a batch of events
                batch_size = self.test_config["eviction_batch_size"]
                for i in range(batch_size):
                    event_id = events_loaded + i
                    event_type = self.test_config["event_types"][
                        event_id % len(self.test_config["event_types"])
                    ]
                    event = self.generate_pressure_event(event_type, event_id)

                    # Store event in Redis
                    key = f"pressure:event:{event_id:010d}"
                    value = json.dumps(event, separators=(",", ":"))

                    # Use Redis pipeline for efficiency
                    pipe = self.redis_client.pipeline()
                    pipe.set(key, value)
                    pipe.lpush("pressure_buffer", key)
                    pipe.execute()

                    events_loaded += 1

                    # Check memory every 100 events
                    if events_loaded % 100 == 0:
                        current_memory = self.get_memory_usage()
                        current_utilization = current_memory.get(
                            "memory_utilization_percent", 0
                        )

                        if current_utilization >= target_memory_percent:
                            break

                # Small delay to prevent overwhelming Redis
                time.sleep(0.01)

            load_time = time.time() - start_time

            return {
                "events_loaded": events_loaded,
                "load_time_seconds": load_time,
                "events_per_second": events_loaded / load_time if load_time > 0 else 0,
                "memory_history": memory_history,
                "final_memory": self.get_memory_usage(),
                "success": True,
            }

        except Exception as e:
            return {
                "events_loaded": events_loaded,
                "load_time_seconds": time.time() - start_time,
                "error": str(e),
                "success": False,
            }

    def monitor_eviction_behavior(self, duration_seconds: int) -> Dict[str, Any]:
        """Monitor Redis eviction behavior under memory pressure."""
        print(f"🔍 Monitoring eviction behavior for {duration_seconds} seconds...")

        start_time = time.time()
        monitoring_data = []
        initial_stats = self.get_redis_stats()
        initial_memory = self.get_memory_usage()

        try:
            while time.time() - start_time < duration_seconds:
                current_time = time.time() - start_time

                # Get current state
                current_memory = self.get_memory_usage()
                current_stats = self.get_redis_stats()

                # Calculate changes
                evicted_keys = current_stats.get("evicted_keys", 0) - initial_stats.get(
                    "evicted_keys", 0
                )
                expired_keys = current_stats.get("expired_keys", 0) - initial_stats.get(
                    "expired_keys", 0
                )

                monitoring_data.append(
                    {
                        "timestamp": datetime.now().isoformat(),
                        "elapsed_seconds": current_time,
                        "memory_usage": current_memory,
                        "evicted_keys": evicted_keys,
                        "expired_keys": expired_keys,
                        "instantaneous_ops_per_sec": current_stats.get(
                            "instantaneous_ops_per_sec", 0
                        ),
                    }
                )

                print(
                    f"   ⏱️ {current_time:.1f}s - Memory: {current_memory.get('memory_utilization_percent', 0):.1f}% - Evicted: {evicted_keys:,}"
                )

                # Sleep for monitoring interval
                time.sleep(self.test_config["monitoring_interval"])

            final_stats = self.get_redis_stats()
            final_memory = self.get_memory_usage()

            return {
                "duration_seconds": duration_seconds,
                "monitoring_data": monitoring_data,
                "initial_stats": initial_stats,
                "final_stats": final_stats,
                "initial_memory": initial_memory,
                "final_memory": final_memory,
                "total_evicted": final_stats.get("evicted_keys", 0)
                - initial_stats.get("evicted_keys", 0),
                "total_expired": final_stats.get("expired_keys", 0)
                - initial_stats.get("expired_keys", 0),
                "success": True,
            }

        except Exception as e:
            return {
                "duration_seconds": time.time() - start_time,
                "error": str(e),
                "success": False,
            }

    def test_recovery_after_pressure(self, duration_seconds: int) -> Dict[str, Any]:
        """Test Redis recovery after memory pressure is released."""
        print(f"🔄 Testing recovery for {duration_seconds} seconds...")

        start_time = time.time()
        recovery_data = []
        initial_memory = self.get_memory_usage()

        try:
            # Remove some keys to reduce memory pressure
            print("   🧹 Removing some keys to reduce memory pressure...")
            keys_to_remove = self.redis_client.lrange(
                "pressure_buffer", 0, 999
            )  # Remove 1000 keys
            if keys_to_remove:
                pipe = self.redis_client.pipeline()
                for key in keys_to_remove:
                    pipe.delete(key)
                pipe.ltrim("pressure_buffer", 1000, -1)  # Keep remaining keys
                pipe.execute()
                print(f"   ✅ Removed {len(keys_to_remove)} keys")

            # Monitor recovery
            while time.time() - start_time < duration_seconds:
                current_time = time.time() - start_time
                current_memory = self.get_memory_usage()

                recovery_data.append(
                    {
                        "timestamp": datetime.now().isoformat(),
                        "elapsed_seconds": current_time,
                        "memory_usage": current_memory,
                    }
                )

                print(
                    f"   ⏱️ {current_time:.1f}s - Memory: {current_memory.get('memory_utilization_percent', 0):.1f}%"
                )

                time.sleep(self.test_config["monitoring_interval"])

            final_memory = self.get_memory_usage()

            return {
                "duration_seconds": duration_seconds,
                "recovery_data": recovery_data,
                "initial_memory": initial_memory,
                "final_memory": final_memory,
                "memory_reduction_mb": initial_memory.get("used_memory_mb", 0)
                - final_memory.get("used_memory_mb", 0),
                "success": True,
            }

        except Exception as e:
            return {
                "duration_seconds": time.time() - start_time,
                "error": str(e),
                "success": False,
            }

    def analyze_eviction_patterns(
        self, monitoring_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Analyze eviction patterns from monitoring data."""
        if not monitoring_data:
            return {"success": False, "error": "No monitoring data available"}

        try:
            # Extract key metrics
            memory_utilizations = [
                d["memory_usage"]["memory_utilization_percent"] for d in monitoring_data
            ]
            evicted_keys = [d["evicted_keys"] for d in monitoring_data]
            ops_per_sec = [d["instantaneous_ops_per_sec"] for d in monitoring_data]

            # Calculate statistics
            avg_memory_utilization = statistics.mean(memory_utilizations)
            max_memory_utilization = max(memory_utilizations)
            min_memory_utilization = min(memory_utilizations)

            total_evicted = evicted_keys[-1] if evicted_keys else 0
            eviction_rate = (
                total_evicted / len(monitoring_data) if monitoring_data else 0
            )

            avg_ops_per_sec = statistics.mean(ops_per_sec) if ops_per_sec else 0
            max_ops_per_sec = max(ops_per_sec) if ops_per_sec else 0

            # Analyze eviction patterns
            eviction_events = []
            for i, data in enumerate(monitoring_data):
                if (
                    i > 0
                    and data["evicted_keys"] > monitoring_data[i - 1]["evicted_keys"]
                ):
                    eviction_events.append(
                        {
                            "timestamp": data["timestamp"],
                            "elapsed_seconds": data["elapsed_seconds"],
                            "evicted_keys": data["evicted_keys"]
                            - monitoring_data[i - 1]["evicted_keys"],
                            "memory_utilization": data["memory_usage"][
                                "memory_utilization_percent"
                            ],
                        }
                    )

            return {
                "memory_analysis": {
                    "avg_utilization_percent": avg_memory_utilization,
                    "max_utilization_percent": max_memory_utilization,
                    "min_utilization_percent": min_memory_utilization,
                    "utilization_range": max_memory_utilization
                    - min_memory_utilization,
                },
                "eviction_analysis": {
                    "total_evicted_keys": total_evicted,
                    "eviction_rate_per_second": eviction_rate,
                    "eviction_events_count": len(eviction_events),
                    "eviction_events": eviction_events[:10],  # First 10 events
                },
                "performance_analysis": {
                    "avg_ops_per_sec": avg_ops_per_sec,
                    "max_ops_per_sec": max_ops_per_sec,
                    "performance_stability": "stable"
                    if max_ops_per_sec < avg_ops_per_sec * 2
                    else "variable",
                },
                "success": True,
            }

        except Exception as e:
            return {"success": False, "error": str(e)}

    def cleanup_pressure_data(self) -> bool:
        """Clean up pressure test data from Redis."""
        print("🧹 Cleaning up pressure test data...")

        try:
            # Get all keys in pressure_buffer
            buffer_keys = self.redis_client.lrange("pressure_buffer", 0, -1)
            print(f"   Found {len(buffer_keys):,} keys to clean up...")

            if buffer_keys:
                # Delete all keys in batches
                batch_size = 1000
                deleted_count = 0

                for i in range(0, len(buffer_keys), batch_size):
                    batch = buffer_keys[i : i + batch_size]

                    # Use pipeline for efficiency
                    pipe = self.redis_client.pipeline()
                    for key in batch:
                        pipe.delete(key)
                    pipe.execute()

                    deleted_count += len(batch)

                    if (i + batch_size) % 10000 == 0:
                        print(f"   Deleted {deleted_count:,} keys...")

                # Clear the buffer list
                self.redis_client.delete("pressure_buffer")

                print(f"   ✅ Cleaned up {deleted_count:,} keys")

            # Get final memory state
            final_memory = self.get_memory_usage()
            print(f"   📊 Final memory: {final_memory.get('used_memory_mb', 0):.1f}MB")

            return True

        except Exception as e:
            print(f"❌ Cleanup failed: {e}")
            return False

    def run_memory_pressure_test(self) -> Dict[str, Any]:
        """Run the complete memory pressure test."""
        print("🚀 Redis Memory Pressure Testing")
        print("=" * 50)

        # Connect to Redis
        if not self.connect_to_redis():
            return self.results

        # Store test configuration
        self.results["test_config"] = self.test_config

        # Get initial state
        initial_memory = self.get_memory_usage()
        initial_stats = self.get_redis_stats()

        print("📊 Initial state:")
        print(
            f"   Memory: {initial_memory.get('used_memory_mb', 0):.1f}MB ({initial_memory.get('memory_utilization_percent', 0):.1f}%)"
        )
        print(f"   Evicted keys: {initial_stats.get('evicted_keys', 0):,}")

        # Step 1: Load events to reach target memory utilization
        pressure_load_result = self.load_pressure_events(
            self.test_config["target_memory_percent"]
        )
        self.results["pressure_test"] = pressure_load_result

        if not pressure_load_result["success"]:
            print(
                f"❌ Pressure loading failed: {pressure_load_result.get('error', 'Unknown error')}"
            )
            return self.results

        # Step 2: Monitor eviction behavior under pressure
        eviction_result = self.monitor_eviction_behavior(
            self.test_config["pressure_duration"]
        )
        self.results["eviction_analysis"] = eviction_result

        if not eviction_result["success"]:
            print(
                f"❌ Eviction monitoring failed: {eviction_result.get('error', 'Unknown error')}"
            )
            return self.results

        # Step 3: Test recovery after pressure
        recovery_result = self.test_recovery_after_pressure(
            self.test_config["recovery_duration"]
        )
        self.results["recovery_test"] = recovery_result

        # Step 4: Analyze eviction patterns
        if eviction_result.get("monitoring_data"):
            pattern_analysis = self.analyze_eviction_patterns(
                eviction_result["monitoring_data"]
            )
            self.results["pattern_analysis"] = pattern_analysis

        return self.results

    def print_report(self, results: Dict[str, Any]):
        """Print a formatted memory pressure test report."""
        print("\n" + "=" * 60)
        print("📋 REDIS MEMORY PRESSURE TEST REPORT")
        print("=" * 60)
        print(f"Timestamp: {results['timestamp']}")

        # Test configuration
        test_config = results.get("test_config", {})
        print("\n📋 TEST CONFIGURATION")
        print("-" * 30)
        print(f"Target memory: {test_config.get('target_memory_percent', 0)}%")
        print(f"Pressure threshold: {test_config.get('pressure_threshold', 0)}%")
        print(f"Pressure duration: {test_config.get('pressure_duration', 0)}s")
        print(f"Recovery duration: {test_config.get('recovery_duration', 0)}s")

        # Pressure test results
        pressure_test = results.get("pressure_test", {})
        if pressure_test.get("success"):
            print("\n🧪 PRESSURE TEST RESULTS")
            print("-" * 30)
            print(f"Events loaded: {pressure_test.get('events_loaded', 0):,}")
            print(f"Load time: {pressure_test.get('load_time_seconds', 0):.1f}s")
            print(
                f"Load rate: {pressure_test.get('events_per_second', 0):.1f} events/sec"
            )

            final_memory = pressure_test.get("final_memory", {})
            print(
                f"Final memory: {final_memory.get('used_memory_mb', 0):.1f}MB ({final_memory.get('memory_utilization_percent', 0):.1f}%)"
            )

        # Eviction analysis
        eviction_analysis = results.get("eviction_analysis", {})
        if eviction_analysis.get("success"):
            print("\n🔍 EVICTION ANALYSIS")
            print("-" * 30)
            print(
                f"Monitoring duration: {eviction_analysis.get('duration_seconds', 0):.1f}s"
            )
            print(f"Total evicted keys: {eviction_analysis.get('total_evicted', 0):,}")
            print(f"Total expired keys: {eviction_analysis.get('total_expired', 0):,}")

            final_memory = eviction_analysis.get("final_memory", {})
            print(
                f"Final memory: {final_memory.get('used_memory_mb', 0):.1f}MB ({final_memory.get('memory_utilization_percent', 0):.1f}%)"
            )

        # Pattern analysis
        pattern_analysis = results.get("pattern_analysis", {})
        if pattern_analysis.get("success"):
            print("\n📊 PATTERN ANALYSIS")
            print("-" * 30)

            memory_analysis = pattern_analysis.get("memory_analysis", {})
            print(
                f"Avg memory utilization: {memory_analysis.get('avg_utilization_percent', 0):.1f}%"
            )
            print(
                f"Max memory utilization: {memory_analysis.get('max_utilization_percent', 0):.1f}%"
            )
            print(
                f"Memory utilization range: {memory_analysis.get('utilization_range', 0):.1f}%"
            )

            eviction_analysis = pattern_analysis.get("eviction_analysis", {})
            print(
                f"Eviction rate: {eviction_analysis.get('eviction_rate_per_second', 0):.2f} keys/sec"
            )
            print(
                f"Eviction events: {eviction_analysis.get('eviction_events_count', 0)}"
            )

            performance_analysis = pattern_analysis.get("performance_analysis", {})
            print(f"Avg ops/sec: {performance_analysis.get('avg_ops_per_sec', 0):.1f}")
            print(
                f"Performance stability: {performance_analysis.get('performance_stability', 'unknown')}"
            )

        # Recovery test
        recovery_test = results.get("recovery_test", {})
        if recovery_test.get("success"):
            print("\n🔄 RECOVERY TEST")
            print("-" * 30)
            print(f"Recovery duration: {recovery_test.get('duration_seconds', 0):.1f}s")
            print(
                f"Memory reduction: {recovery_test.get('memory_reduction_mb', 0):.1f}MB"
            )

            final_memory = recovery_test.get("final_memory", {})
            print(
                f"Final memory: {final_memory.get('used_memory_mb', 0):.1f}MB ({final_memory.get('memory_utilization_percent', 0):.1f}%)"
            )

        print("\n" + "=" * 60)

    def save_report(self, results: Dict[str, Any], filename: str = None):
        """Save the memory pressure test report to a JSON file."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"redis_memory_pressure_test_{timestamp}.json"

        filepath = os.path.join(os.path.dirname(__file__), filename)

        try:
            with open(filepath, "w") as f:
                json.dump(results, f, indent=2)
            print(f"\n💾 Memory pressure test report saved to: {filepath}")
        except Exception as e:
            print(f"\n❌ Failed to save memory pressure test report: {e}")


def main():
    """Main function to run the Redis memory pressure test."""
    print("🚀 Redis Memory Pressure Testing for Bluesky Data Pipeline")
    print("=" * 60)

    # Get Redis connection parameters from environment
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_password = os.getenv("REDIS_PASSWORD")

    # Create tester
    tester = RedisMemoryPressureTester(
        host=redis_host, port=redis_port, password=redis_password
    )

    # Run memory pressure test
    results = tester.run_memory_pressure_test()

    # Print report
    tester.print_report(results)

    # Save report
    tester.save_report(results)

    # Cleanup test data
    if results.get("pressure_test", {}).get("success"):
        cleanup_success = tester.cleanup_pressure_data()
        if cleanup_success:
            print("\n✅ Pressure test data cleanup completed successfully")
        else:
            print("\n⚠️ Pressure test data cleanup had issues")

    # Return exit code based on results
    pressure_success = results.get("pressure_test", {}).get("success", False)
    eviction_success = results.get("eviction_analysis", {}).get("success", False)

    if pressure_success and eviction_success:
        print("\n✅ Memory pressure test completed successfully!")
        return 0
    else:
        print("\n⚠️ Memory pressure test completed with issues. Review results above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
