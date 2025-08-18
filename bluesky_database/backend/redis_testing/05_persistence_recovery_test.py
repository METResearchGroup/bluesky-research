#!/usr/bin/env python3
"""
Redis Persistence Recovery Testing Script

This script tests AOF persistence and data recovery scenarios to validate
that Redis can properly recover data after restarts and crashes.

Usage:
    python 05_persistence_recovery_test.py
"""

import redis
import time
import json
import os
import sys
import uuid
import subprocess
from typing import Dict, Any
from datetime import datetime


class RedisPersistenceRecoveryTester:
    """Tests Redis AOF persistence and data recovery scenarios."""

    def __init__(self, host: str = "localhost", port: int = 6379, password: str = None):
        """Initialize the persistence recovery tester.

        Environment overrides (optional):
        - REDIS_PERSISTENCE_TARGET_EVENTS: int number of events to load for testing
        - REDIS_PERSISTENCE_BATCH_SIZE: int events per batch during load
        - REDIS_PERSISTENCE_RESTART_DELAY: int seconds to wait after restart
        - REDIS_PERSISTENCE_VALIDATION_SAMPLE: int events to validate after recovery
        - REDIS_PERSISTENCE_SKIP_RESTART: "true" to skip container restart (for local testing)
        - REDIS_PERSISTENCE_CONTAINER_NAME: string name of Redis container to restart
        """
        self.host = host
        self.port = port
        self.password = password
        self.redis_client = None
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "test_config": {},
            "data_load_test": {},
            "aof_analysis": {},
            "restart_test": {},
            "recovery_validation": {},
            "recommendations": [],
        }

        # Test configuration with environment overrides
        self.test_config = {
            "target_events": int(
                os.getenv("REDIS_PERSISTENCE_TARGET_EVENTS", "100000")
            ),
            "batch_size": int(os.getenv("REDIS_PERSISTENCE_BATCH_SIZE", "1000")),
            "restart_delay": int(os.getenv("REDIS_PERSISTENCE_RESTART_DELAY", "5")),
            "validation_sample_size": int(
                os.getenv("REDIS_PERSISTENCE_VALIDATION_SAMPLE", "1000")
            ),
            "skip_restart": os.getenv("REDIS_PERSISTENCE_SKIP_RESTART", "false").lower()
            == "true",
            "container_name": os.getenv(
                "REDIS_PERSISTENCE_CONTAINER_NAME", "bluesky_redis"
            ),
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

    def generate_persistence_event(
        self, event_type: str, event_id: int
    ) -> Dict[str, Any]:
        """Generate a realistic Bluesky firehose event for persistence testing."""
        timestamp = datetime.now().isoformat()

        # Base event structure
        event = {
            "id": f"persistence_event_{event_id:010d}",
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
                    "text": f"This is a persistence test post #{event_id} to validate AOF recovery. "
                    * 5,
                    "author": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "langs": ["en"],
                    "labels": [],
                    "tags": ["persistence", "test", "bluesky", "firehose", "aof"],
                    "reply": None,
                    "embed": {
                        "type": "app.bsky.embed.images",
                        "images": [
                            {
                                "alt": f"Persistence test image {event_id}",
                                "image": {
                                    "ref": f"bafkreib{uuid.uuid4().hex[:44]}",
                                    "mimeType": "image/jpeg",
                                    "size": 512000,
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
                                    "tag": "persistence",
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
                        "persistence_test": True,
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
                        "persistence_test": True,
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
                        "persistence_test": True,
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
                        "persistence_test": True,
                        "event_id": event_id,
                        "timestamp": timestamp,
                    },
                }
            )

        return event

    def get_redis_info(self) -> Dict[str, Any]:
        """Get comprehensive Redis information."""
        try:
            info = self.redis_client.info()

            return {
                "redis_version": info.get("redis_version", "unknown"),
                "uptime_in_seconds": info.get("uptime_in_seconds", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "total_connections_received": info.get("total_connections_received", 0),
                "used_memory_human": info.get("used_memory_human", "0B"),
                "used_memory_peak_human": info.get("used_memory_peak_human", "0B"),
                "aof_enabled": info.get("aof_enabled", 0),
                "aof_rewrite_in_progress": info.get("aof_rewrite_in_progress", 0),
                "aof_rewrite_scheduled": info.get("aof_rewrite_scheduled", 0),
                "aof_last_rewrite_time_sec": info.get("aof_last_rewrite_time_sec", 0),
                "aof_current_rewrite_time_sec": info.get(
                    "aof_current_rewrite_time_sec", 0
                ),
                "aof_last_bgrewrite_status": info.get(
                    "aof_last_bgrewrite_status", "unknown"
                ),
                "aof_last_write_status": info.get("aof_last_write_status", "unknown"),
                "aof_last_cow_size": info.get("aof_last_cow_size", 0),
                "aof_buffer_length": info.get("aof_buffer_length", 0),
                "aof_rewrite_buffer_length": info.get("aof_rewrite_buffer_length", 0),
                "aof_pending_rewrite": info.get("aof_pending_rewrite", 0),
                "aof_delayed_fsync": info.get("aof_delayed_fsync", 0),
            }
        except Exception as e:
            print(f"❌ Error getting Redis info: {e}")
            return {}

    def get_aof_file_info(self) -> Dict[str, Any]:
        """Get AOF file information."""
        try:
            # Get AOF file path from Redis config
            config = self.redis_client.config_get("dir")
            data_dir = config.get("dir", "/data")

            # Common AOF file locations
            aof_paths = [
                os.path.join(data_dir, "appendonly.aof"),
                "/data/appendonly.aof",
                "./appendonly.aof",
            ]

            aof_info = {}
            for aof_path in aof_paths:
                if os.path.exists(aof_path):
                    stat = os.stat(aof_path)
                    aof_info = {
                        "path": aof_path,
                        "size_bytes": stat.st_size,
                        "size_mb": stat.st_size / (1024 * 1024),
                        "modified_time": datetime.fromtimestamp(
                            stat.st_mtime
                        ).isoformat(),
                        "exists": True,
                    }
                    break

            if not aof_info:
                aof_info = {
                    "exists": False,
                    "error": "AOF file not found in common locations",
                }

            return aof_info

        except Exception as e:
            return {"exists": False, "error": str(e)}

    def load_persistence_data(self) -> Dict[str, Any]:
        """Load test data for persistence testing."""
        print(
            f"🔄 Loading {self.test_config['target_events']:,} events for persistence testing..."
        )

        events_loaded = 0
        start_time = time.time()

        try:
            # Clear any existing persistence test data
            self.redis_client.delete("persistence_buffer")

            while events_loaded < self.test_config["target_events"]:
                batch_size = min(
                    self.test_config["batch_size"],
                    self.test_config["target_events"] - events_loaded,
                )

                for i in range(batch_size):
                    event_id = events_loaded + i
                    event_type = self.test_config["event_types"][
                        event_id % len(self.test_config["event_types"])
                    ]
                    event = self.generate_persistence_event(event_type, event_id)

                    # Store event in Redis
                    key = f"persistence:event:{event_id:010d}"
                    value = json.dumps(event, separators=(",", ":"))

                    # Use Redis pipeline for efficiency
                    pipe = self.redis_client.pipeline()
                    pipe.set(key, value)
                    pipe.lpush("persistence_buffer", key)
                    pipe.execute()

                    events_loaded += 1

                    # Progress update every 10K events
                    if events_loaded % 10000 == 0:
                        print(
                            f"   📊 Loaded {events_loaded:,}/{self.test_config['target_events']:,} events..."
                        )

                # Small delay to allow AOF to sync
                time.sleep(0.1)

            load_time = time.time() - start_time

            # Get final state
            final_info = self.get_redis_info()
            aof_info = self.get_aof_file_info()

            return {
                "events_loaded": events_loaded,
                "load_time_seconds": load_time,
                "events_per_second": events_loaded / load_time if load_time > 0 else 0,
                "final_redis_info": final_info,
                "aof_file_info": aof_info,
                "success": True,
            }

        except Exception as e:
            return {
                "events_loaded": events_loaded,
                "load_time_seconds": time.time() - start_time,
                "error": str(e),
                "success": False,
            }

    def analyze_aof_file(self) -> Dict[str, Any]:
        """Analyze the AOF file for integrity and content."""
        print("🔍 Analyzing AOF file...")

        try:
            aof_info = self.get_aof_file_info()

            if not aof_info.get("exists"):
                return {"success": False, "error": "AOF file not found"}

            aof_path = aof_info["path"]

            # Read AOF file content (first 10KB for analysis)
            with open(aof_path, "r") as f:
                content = f.read(10240)  # Read first 10KB

            # Analyze AOF content
            lines = content.split("\n")
            set_commands = [line for line in lines if line.startswith("SET")]
            lpush_commands = [line for line in lines if line.startswith("LPUSH")]

            # Count persistence events in AOF
            persistence_events = [
                line for line in lines if "persistence:event:" in line
            ]

            analysis = {
                "file_size_mb": aof_info["size_mb"],
                "total_lines_analyzed": len(lines),
                "set_commands": len(set_commands),
                "lpush_commands": len(lpush_commands),
                "persistence_events_found": len(persistence_events),
                "aof_format_valid": all(
                    line.startswith("*")
                    or line.startswith("$")
                    or line.startswith("+")
                    or line.startswith("-")
                    or line.startswith(":")
                    or line.startswith("SET")
                    or line.startswith("LPUSH")
                    or line == ""
                    for line in lines[:100]
                ),
                "sample_content": content[:1000],  # First 1KB for inspection
            }

            print(f"   📊 AOF file size: {analysis['file_size_mb']:.2f}MB")
            print(
                f"   📊 Persistence events in AOF: {analysis['persistence_events_found']}"
            )
            print(f"   📊 AOF format valid: {analysis['aof_format_valid']}")

            return {"aof_file_info": aof_info, "analysis": analysis, "success": True}

        except Exception as e:
            return {"error": str(e), "success": False}

    def restart_redis_container(self) -> bool:
        """Restart the Redis container to test persistence recovery."""
        if self.test_config["skip_restart"]:
            print(
                "⏭️ Skipping Redis container restart (REDIS_PERSISTENCE_SKIP_RESTART=true)"
            )
            print(
                "   Note: This simulates restart testing without actual container restart"
            )
            time.sleep(1)  # Small delay to simulate restart time
            return True

        print("🔄 Restarting Redis container...")

        try:
            # Try to restart Redis container using Docker
            container_name = self.test_config["container_name"]
            restart_cmd = f"docker restart {container_name}"
            result = subprocess.run(
                restart_cmd, shell=True, capture_output=True, text=True
            )

            if result.returncode == 0:
                print(
                    f"   ✅ Redis container '{container_name}' restarted successfully"
                )
                print(
                    f"   ⏳ Waiting {self.test_config['restart_delay']} seconds for Redis to start..."
                )
                time.sleep(self.test_config["restart_delay"])
                return True
            else:
                print(
                    f"   ❌ Failed to restart Redis container '{container_name}': {result.stderr}"
                )
                return False

        except Exception as e:
            print(f"   ❌ Error restarting Redis: {e}")
            return False

    def wait_for_redis_ready(self, max_wait: int = 60) -> bool:
        """Wait for Redis to be ready after restart."""
        print("⏳ Waiting for Redis to be ready...")

        start_time = time.time()

        while time.time() - start_time < max_wait:
            try:
                # Try to connect to Redis
                test_client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    password=self.password,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                )

                # Test connection
                test_client.ping()
                print(f"   ✅ Redis is ready after {time.time() - start_time:.1f}s")
                return True

            except Exception:
                time.sleep(1)

        print(f"   ❌ Redis did not become ready within {max_wait}s")
        return False

    def validate_data_recovery(self) -> Dict[str, Any]:
        """Validate that data was recovered correctly after restart."""
        print("🔍 Validating data recovery...")

        try:
            # Reconnect to Redis
            if not self.connect_to_redis():
                return {"success": False, "error": "Failed to reconnect to Redis"}

            # Get buffer size
            buffer_size = self.redis_client.llen("persistence_buffer")
            print(f"   📊 Buffer size after restart: {buffer_size:,} events")

            if buffer_size == 0:
                return {
                    "success": False,
                    "error": "No data found in buffer after restart",
                    "buffer_size": 0,
                }

            # Sample events for validation
            sample_size = min(self.test_config["validation_sample_size"], buffer_size)
            sample_keys = self.redis_client.lrange(
                "persistence_buffer", 0, sample_size - 1
            )

            valid_events = 0
            invalid_events = 0
            sample_events = []

            for key in sample_keys:
                try:
                    value = self.redis_client.get(key)
                    if value:
                        event = json.loads(value)

                        # Validate event structure
                        if all(
                            field in event
                            for field in ["id", "type", "timestamp", "uri", "cid"]
                        ):
                            valid_events += 1
                            sample_events.append(
                                {
                                    "key": key,
                                    "event_type": event.get("type"),
                                    "event_id": event.get("id"),
                                    "timestamp": event.get("timestamp"),
                                }
                            )
                        else:
                            invalid_events += 1
                    else:
                        invalid_events += 1
                except Exception:
                    invalid_events += 1

            recovery_percent = (
                (valid_events / sample_size * 100) if sample_size > 0 else 0
            )

            # Get post-restart Redis info
            post_restart_info = self.get_redis_info()

            results = {
                "buffer_size": buffer_size,
                "sample_size": sample_size,
                "valid_events": valid_events,
                "invalid_events": invalid_events,
                "recovery_percent": recovery_percent,
                "sample_events": sample_events[:5],  # Show first 5 events
                "post_restart_info": post_restart_info,
                "success": recovery_percent > 95,  # 95% recovery threshold
            }

            print(
                f"   ✅ Data recovery: {recovery_percent:.1f}% ({valid_events}/{sample_size} valid)"
            )

            return results

        except Exception as e:
            return {"success": False, "error": str(e)}

    def test_aof_rewrite(self) -> Dict[str, Any]:
        """Test AOF rewrite functionality."""
        print("🔄 Testing AOF rewrite...")

        try:
            # Get initial AOF info
            initial_aof = self.get_aof_file_info()

            # Trigger AOF rewrite
            print("   📊 Triggering AOF rewrite...")
            self.redis_client.bgrewriteaof()

            # Wait for rewrite to complete
            max_wait = 60
            start_time = time.time()

            while time.time() - start_time < max_wait:
                info = self.get_redis_info()

                if info.get("aof_rewrite_in_progress", 0) == 0:
                    print(
                        f"   ✅ AOF rewrite completed in {time.time() - start_time:.1f}s"
                    )
                    break

                time.sleep(1)
            else:
                print(f"   ⚠️ AOF rewrite did not complete within {max_wait}s")

            # Get final AOF info
            final_aof = self.get_aof_file_info()

            return {
                "initial_aof": initial_aof,
                "final_aof": final_aof,
                "rewrite_duration": time.time() - start_time,
                "rewrite_completed": info.get("aof_rewrite_in_progress", 0) == 0,
                "success": True,
            }

        except Exception as e:
            return {"success": False, "error": str(e)}

    def cleanup_persistence_data(self) -> bool:
        """Clean up persistence test data from Redis."""
        print("🧹 Cleaning up persistence test data...")

        try:
            # Get all keys in persistence_buffer
            buffer_keys = self.redis_client.lrange("persistence_buffer", 0, -1)
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
                self.redis_client.delete("persistence_buffer")

                print(f"   ✅ Cleaned up {deleted_count:,} keys")

            return True

        except Exception as e:
            print(f"❌ Cleanup failed: {e}")
            return False

    def run_persistence_recovery_test(self) -> Dict[str, Any]:
        """Run the complete persistence recovery test."""
        print("🚀 Redis Persistence Recovery Testing")
        print("=" * 50)

        # Connect to Redis
        if not self.connect_to_redis():
            return self.results

        # Store test configuration
        self.results["test_config"] = self.test_config

        # Get initial Redis state
        initial_info = self.get_redis_info()
        print("📊 Initial Redis state:")
        print(f"   Version: {initial_info.get('redis_version', 'unknown')}")
        print(f"   AOF enabled: {initial_info.get('aof_enabled', 0)}")
        print(f"   Uptime: {initial_info.get('uptime_in_seconds', 0)}s")

        # Step 1: Load test data
        load_result = self.load_persistence_data()
        self.results["data_load_test"] = load_result

        if not load_result["success"]:
            print(
                f"❌ Data loading failed: {load_result.get('error', 'Unknown error')}"
            )
            return self.results

        # Step 2: Analyze AOF file before restart
        aof_analysis = self.analyze_aof_file()
        self.results["aof_analysis"] = aof_analysis

        # Step 3: Test AOF rewrite
        rewrite_result = self.test_aof_rewrite()
        self.results["aof_rewrite_test"] = rewrite_result

        # Step 4: Restart Redis container
        restart_success = self.restart_redis_container()
        self.results["restart_test"] = {
            "restart_success": restart_success,
            "timestamp": datetime.now().isoformat(),
        }

        if not restart_success:
            print("❌ Redis restart failed")
            return self.results

        # Step 5: Wait for Redis to be ready
        redis_ready = self.wait_for_redis_ready()
        self.results["restart_test"]["redis_ready"] = redis_ready

        if not redis_ready:
            print("❌ Redis did not become ready after restart")
            return self.results

        # Step 6: Validate data recovery
        recovery_result = self.validate_data_recovery()
        self.results["recovery_validation"] = recovery_result

        return self.results

    def print_report(self, results: Dict[str, Any]):
        """Print a formatted persistence recovery test report."""
        print("\n" + "=" * 60)
        print("📋 REDIS PERSISTENCE RECOVERY TEST REPORT")
        print("=" * 60)
        print(f"Timestamp: {results['timestamp']}")

        # Test configuration
        test_config = results.get("test_config", {})
        print("\n📋 TEST CONFIGURATION")
        print("-" * 30)
        print(f"Target events: {test_config.get('target_events', 0):,}")
        print(f"Batch size: {test_config.get('batch_size', 0):,}")
        print(f"Restart delay: {test_config.get('restart_delay', 0)}s")
        print(f"Validation sample: {test_config.get('validation_sample_size', 0):,}")

        # Data load test results
        data_load = results.get("data_load_test", {})
        if data_load.get("success"):
            print("\n🧪 DATA LOAD TEST RESULTS")
            print("-" * 30)
            print(f"Events loaded: {data_load.get('events_loaded', 0):,}")
            print(f"Load time: {data_load.get('load_time_seconds', 0):.1f}s")
            print(f"Load rate: {data_load.get('events_per_second', 0):.1f} events/sec")

            aof_info = data_load.get("aof_file_info", {})
            if aof_info.get("exists"):
                print(f"AOF file size: {aof_info.get('size_mb', 0):.2f}MB")

        # AOF analysis
        aof_analysis = results.get("aof_analysis", {})
        if aof_analysis.get("success"):
            print("\n🔍 AOF ANALYSIS")
            print("-" * 30)
            analysis = aof_analysis.get("analysis", {})
            print(f"AOF file size: {analysis.get('file_size_mb', 0):.2f}MB")
            print(
                f"Persistence events: {analysis.get('persistence_events_found', 0):,}"
            )
            print(f"AOF format valid: {analysis.get('aof_format_valid', False)}")

        # AOF rewrite test
        rewrite_test = results.get("aof_rewrite_test", {})
        if rewrite_test.get("success"):
            print("\n🔄 AOF REWRITE TEST")
            print("-" * 30)
            print(f"Rewrite duration: {rewrite_test.get('rewrite_duration', 0):.1f}s")
            print(f"Rewrite completed: {rewrite_test.get('rewrite_completed', False)}")

        # Restart test
        restart_test = results.get("restart_test", {})
        print("\n🔄 RESTART TEST")
        print("-" * 30)
        print(f"Restart success: {restart_test.get('restart_success', False)}")
        print(f"Redis ready: {restart_test.get('redis_ready', False)}")

        # Recovery validation
        recovery_validation = results.get("recovery_validation", {})
        if recovery_validation.get("success"):
            print("\n✅ RECOVERY VALIDATION")
            print("-" * 30)
            print(f"Buffer size: {recovery_validation.get('buffer_size', 0):,} events")
            print(f"Sample size: {recovery_validation.get('sample_size', 0)} events")
            print(
                f"Recovery percent: {recovery_validation.get('recovery_percent', 0):.1f}%"
            )
            print(f"Valid events: {recovery_validation.get('valid_events', 0)}")
            print(f"Invalid events: {recovery_validation.get('invalid_events', 0)}")
        else:
            print("\n❌ RECOVERY VALIDATION")
            print("-" * 30)
            print(f"Error: {recovery_validation.get('error', 'Unknown error')}")

        print("\n" + "=" * 60)

    def save_report(self, results: Dict[str, Any], filename: str = None):
        """Save the persistence recovery test report to a JSON file."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"redis_persistence_recovery_test_{timestamp}.json"

        filepath = os.path.join(os.path.dirname(__file__), filename)

        try:
            with open(filepath, "w") as f:
                json.dump(results, f, indent=2)
            print(f"\n💾 Persistence recovery test report saved to: {filepath}")
        except Exception as e:
            print(f"\n❌ Failed to save persistence recovery test report: {e}")


def main():
    """Main function to run the Redis persistence recovery test."""
    # Ensure all prints flush immediately for real-time visibility
    import functools

    try:
        # Make all print calls in this module flush by default
        global print  # noqa: PLW0603
        print = functools.partial(print, flush=True)  # type: ignore[assignment]
    except Exception:
        pass

    # Mirror stdout/stderr to a timestamped log file alongside the JSON report
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = os.path.join(
            os.path.dirname(__file__), f"05_persistence_recovery_test_{ts}.log"
        )

        class _Tee:
            def __init__(self, stream_a, stream_b):
                self.a = stream_a
                self.b = stream_b

            def write(self, data: str):
                try:
                    self.a.write(data)
                    self.a.flush()
                except Exception:
                    pass
                try:
                    self.b.write(data)
                    self.b.flush()
                except Exception:
                    pass

            def flush(self):
                try:
                    self.a.flush()
                except Exception:
                    pass
                try:
                    self.b.flush()
                except Exception:
                    pass

        log_fp = open(log_path, "a", buffering=1)
        sys.stdout = _Tee(sys.stdout, log_fp)
        sys.stderr = _Tee(sys.stderr, log_fp)
        print(f"📝 Logging to: {log_path}")
    except Exception:
        pass

    print("🚀 Redis Persistence Recovery Testing for Bluesky Data Pipeline")
    print("=" * 60)

    # Get Redis connection parameters from environment
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_password = os.getenv("REDIS_PASSWORD")

    # Create tester
    tester = RedisPersistenceRecoveryTester(
        host=redis_host, port=redis_port, password=redis_password
    )

    # Run persistence recovery test
    results = tester.run_persistence_recovery_test()

    # Print report
    tester.print_report(results)

    # Save report
    tester.save_report(results)

    # Cleanup test data
    if results.get("data_load_test", {}).get("success"):
        cleanup_success = tester.cleanup_persistence_data()
        if cleanup_success:
            print("\n✅ Persistence test data cleanup completed successfully")
        else:
            print("\n⚠️ Persistence test data cleanup had issues")

    # Return exit code based on results
    load_success = results.get("data_load_test", {}).get("success", False)
    restart_success = results.get("restart_test", {}).get("restart_success", False)
    recovery_success = results.get("recovery_validation", {}).get("success", False)

    if load_success and restart_success and recovery_success:
        print("\n✅ Persistence recovery test completed successfully!")
        return 0
    else:
        print(
            "\n⚠️ Persistence recovery test completed with issues. Review results above."
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
