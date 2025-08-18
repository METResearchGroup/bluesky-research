#!/usr/bin/env python3
"""
Redis Throughput Validation Testing Script

This script validates sustained throughput under production-like conditions
to ensure Redis can maintain performance over extended periods.

Usage:
    python 06_throughput_validation.py
"""

import redis
import time
import json
import uuid
import os
import sys
import functools
from typing import Dict, Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


class RedisThroughputValidator:
    """Validates Redis sustained throughput under production-like conditions."""

    def __init__(self, host: str = "localhost", port: int = 6379, password: str = None):
        """Initialize the throughput validator.

        Environment overrides (optional):
        - REDIS_THROUGHPUT_BASELINE_DURATION: int seconds for baseline test
        - REDIS_THROUGHPUT_SUSTAINED_DURATION: int seconds for sustained test
        - REDIS_THROUGHPUT_CONCURRENT_DURATION: int seconds for concurrent test
        - REDIS_THROUGHPUT_TARGET: int target ops/sec
        - REDIS_THROUGHPUT_CONCURRENT_THREADS: int number of concurrent threads
        - REDIS_THROUGHPUT_BATCH_SIZE: int operations per batch
        """
        self.host = host
        self.port = port
        self.password = password
        self.redis_client = None
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "test_config": {},
            "baseline_test": {},
            "sustained_test": {},
            "concurrent_test": {},
            "performance_analysis": {},
            "recommendations": [],
        }

        # Test configuration with environment overrides
        self.test_config = {
            "baseline_duration": int(
                os.getenv("REDIS_THROUGHPUT_BASELINE_DURATION", "60")
            ),
            "sustained_duration": int(
                os.getenv("REDIS_THROUGHPUT_SUSTAINED_DURATION", "300")
            ),
            "concurrent_duration": int(
                os.getenv("REDIS_THROUGHPUT_CONCURRENT_DURATION", "120")
            ),
            "target_throughput": int(os.getenv("REDIS_THROUGHPUT_TARGET", "1000")),
            "concurrent_threads": int(
                os.getenv("REDIS_THROUGHPUT_CONCURRENT_THREADS", "10")
            ),
            "batch_size": int(os.getenv("REDIS_THROUGHPUT_BATCH_SIZE", "100")),
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

    def generate_throughput_event(
        self, event_type: str, event_id: int
    ) -> Dict[str, Any]:
        """Generate a realistic Bluesky firehose event for throughput testing."""
        timestamp = datetime.now().isoformat()

        # Base event structure
        event = {
            "id": f"throughput_event_{event_id:010d}",
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
                    "text": f"Test post content for throughput validation {event_id}",
                    "author": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "langs": ["en"],
                    "labels": [],
                    "tags": [],
                }
            )
        elif event_type == "app.bsky.feed.like":
            event.update(
                {
                    "subject": f"at://did:plc:{uuid.uuid4().hex[:16]}/app.bsky.feed.post/{uuid.uuid4().hex[:16]}",
                    "subjectCid": f"bafyrei{uuid.uuid4().hex[:44]}",
                    "createdAt": timestamp,
                }
            )
        elif event_type == "app.bsky.feed.repost":
            event.update(
                {
                    "subject": f"at://did:plc:{uuid.uuid4().hex[:16]}/app.bsky.feed.post/{uuid.uuid4().hex[:16]}",
                    "subjectCid": f"bafyrei{uuid.uuid4().hex[:44]}",
                    "createdAt": timestamp,
                }
            )
        elif event_type == "app.bsky.graph.follow":
            event.update(
                {
                    "subject": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "subjectDeclarationCid": f"bafyrei{uuid.uuid4().hex[:44]}",
                    "createdAt": timestamp,
                }
            )
        elif event_type == "app.bsky.graph.block":
            event.update(
                {
                    "subject": f"did:plc:{uuid.uuid4().hex[:16]}",
                    "subjectDeclarationCid": f"bafyrei{uuid.uuid4().hex[:44]}",
                    "createdAt": timestamp,
                }
            )

        return event

    def get_redis_info(self) -> Dict[str, Any]:
        """Get Redis server information."""
        try:
            info = self.redis_client.info()
            return {
                "version": info.get("redis_version", "unknown"),
                "uptime": info.get("uptime_in_seconds", 0),
                "connected_clients": info.get("connected_clients", 0),
                "used_memory": info.get("used_memory", 0),
                "used_memory_peak": info.get("used_memory_peak", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "instantaneous_ops_per_sec": info.get("instantaneous_ops_per_sec", 0),
            }
        except Exception as e:
            print(f"❌ Error getting Redis info: {e}")
            return {}

    def run_baseline_throughput_test(self) -> Dict[str, Any]:
        """Run baseline throughput test to establish performance baseline."""
        print(
            f"🧪 Running baseline throughput test ({self.test_config['baseline_duration']}s)..."
        )

        start_time = time.time()
        end_time = start_time + self.test_config["baseline_duration"]

        operations = 0
        event_id = 0
        latencies = []

        while time.time() < end_time:
            # Perform batch operations
            for _ in range(self.test_config["batch_size"]):
                event_type = self.test_config["event_types"][
                    event_id % len(self.test_config["event_types"])
                ]
                event = self.generate_throughput_event(event_type, event_id)

                op_start = time.time()
                key = f"throughput_baseline:{event_id}"
                self.redis_client.set(key, json.dumps(event))
                self.redis_client.lpush("throughput_baseline_buffer", key)
                op_end = time.time()

                latencies.append((op_end - op_start) * 1000)  # Convert to milliseconds
                operations += 1
                event_id += 1

            batch_end = time.time()

            # Calculate current throughput
            elapsed = batch_end - start_time
            current_throughput = operations / elapsed

            print(
                f"   📊 Operations: {operations:,}, Throughput: {current_throughput:.1f} ops/sec"
            )

        total_duration = time.time() - start_time
        avg_throughput = operations / total_duration

        # Calculate latency percentiles
        latencies.sort()
        p50 = latencies[len(latencies) // 2] if latencies else 0
        p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0
        p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0

        return {
            "duration": total_duration,
            "operations": operations,
            "avg_throughput": avg_throughput,
            "latency_p50": p50,
            "latency_p95": p95,
            "latency_p99": p99,
            "success": True,
        }

    def run_sustained_throughput_test(self) -> Dict[str, Any]:
        """Run sustained throughput test to validate long-term performance."""
        print(
            f"🔄 Running sustained throughput test ({self.test_config['sustained_duration']}s)..."
        )

        start_time = time.time()
        end_time = start_time + self.test_config["sustained_duration"]

        operations = 0
        event_id = 0
        throughput_samples = []
        latencies = []

        while time.time() < end_time:
            # Perform batch operations
            for _ in range(self.test_config["batch_size"]):
                event_type = self.test_config["event_types"][
                    event_id % len(self.test_config["event_types"])
                ]
                event = self.generate_throughput_event(event_type, event_id)

                op_start = time.time()
                key = f"throughput_sustained:{event_id}"
                self.redis_client.set(key, json.dumps(event))
                self.redis_client.lpush("throughput_sustained_buffer", key)
                op_end = time.time()

                latencies.append((op_end - op_start) * 1000)  # Convert to milliseconds
                operations += 1
                event_id += 1

            batch_end = time.time()

            # Calculate current throughput
            elapsed = batch_end - start_time
            current_throughput = operations / elapsed
            throughput_samples.append(current_throughput)

            # Print progress every 30 seconds
            if int(elapsed) % 30 == 0 and int(elapsed) > 0:
                print(
                    f"   📊 {int(elapsed)}s: {operations:,} ops, {current_throughput:.1f} ops/sec"
                )

        total_duration = time.time() - start_time
        avg_throughput = operations / total_duration

        # Calculate throughput stability
        throughput_variance = (
            sum((t - avg_throughput) ** 2 for t in throughput_samples)
            / len(throughput_samples)
            if throughput_samples
            else 0
        )
        throughput_std = throughput_variance**0.5

        # Calculate latency percentiles
        latencies.sort()
        p50 = latencies[len(latencies) // 2] if latencies else 0
        p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0
        p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0

        return {
            "duration": total_duration,
            "operations": operations,
            "avg_throughput": avg_throughput,
            "throughput_std": throughput_std,
            "throughput_samples": len(throughput_samples),
            "latency_p50": p50,
            "latency_p95": p95,
            "latency_p99": p99,
            "success": True,
        }

    def worker_thread(self, thread_id: int, duration: int) -> Dict[str, Any]:
        """Worker thread for concurrent throughput testing."""
        start_time = time.time()
        end_time = start_time + duration

        operations = 0
        event_id = thread_id * 1000000  # Ensure unique event IDs across threads
        latencies = []

        while time.time() < end_time:
            event_type = self.test_config["event_types"][
                event_id % len(self.test_config["event_types"])
            ]
            event = self.generate_throughput_event(event_type, event_id)

            op_start = time.time()
            key = f"throughput_concurrent:{thread_id}:{event_id}"
            self.redis_client.set(key, json.dumps(event))
            self.redis_client.lpush(f"throughput_concurrent_buffer:{thread_id}", key)
            op_end = time.time()

            latencies.append((op_end - op_start) * 1000)  # Convert to milliseconds
            operations += 1
            event_id += 1

        total_duration = time.time() - start_time
        avg_throughput = operations / total_duration

        # Calculate latency percentiles
        latencies.sort()
        p50 = latencies[len(latencies) // 2] if latencies else 0
        p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0
        p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0

        return {
            "thread_id": thread_id,
            "duration": total_duration,
            "operations": operations,
            "avg_throughput": avg_throughput,
            "latency_p50": p50,
            "latency_p95": p95,
            "latency_p99": p99,
        }

    def run_concurrent_throughput_test(self) -> Dict[str, Any]:
        """Run concurrent throughput test with multiple threads."""
        print(
            f"🔄 Running concurrent throughput test ({self.test_config['concurrent_threads']} threads, {self.test_config['concurrent_duration']}s)..."
        )

        start_time = time.time()

        # Create thread pool
        with ThreadPoolExecutor(
            max_workers=self.test_config["concurrent_threads"]
        ) as executor:
            # Submit tasks
            futures = [
                executor.submit(
                    self.worker_thread, i, self.test_config["concurrent_duration"]
                )
                for i in range(self.test_config["concurrent_threads"])
            ]

            # Collect results
            thread_results = []
            total_operations = 0
            all_latencies = []

            for future in as_completed(futures):
                result = future.result()
                thread_results.append(result)
                total_operations += result["operations"]
                print(
                    f"   📊 Thread {result['thread_id']}: {result['operations']:,} ops, {result['avg_throughput']:.1f} ops/sec"
                )

        total_duration = time.time() - start_time
        total_throughput = total_operations / total_duration

        # Aggregate latency data
        for result in thread_results:
            # Estimate latency distribution (simplified)
            all_latencies.extend([result["latency_p50"]] * (result["operations"] // 3))
            all_latencies.extend([result["latency_p95"]] * (result["operations"] // 20))
            all_latencies.extend(
                [result["latency_p99"]] * (result["operations"] // 100)
            )

        all_latencies.sort()
        p50 = all_latencies[len(all_latencies) // 2] if all_latencies else 0
        p95 = all_latencies[int(len(all_latencies) * 0.95)] if all_latencies else 0
        p99 = all_latencies[int(len(all_latencies) * 0.99)] if all_latencies else 0

        return {
            "duration": total_duration,
            "threads": self.test_config["concurrent_threads"],
            "total_operations": total_operations,
            "total_throughput": total_throughput,
            "thread_results": thread_results,
            "latency_p50": p50,
            "latency_p95": p95,
            "latency_p99": p99,
            "success": True,
        }

    def cleanup_throughput_data(self) -> bool:
        """Clean up throughput test data."""
        print("🧹 Cleaning up throughput test data...")

        try:
            # Get all throughput test keys
            baseline_keys = self.redis_client.keys("throughput_baseline:*")
            sustained_keys = self.redis_client.keys("throughput_sustained:*")
            concurrent_keys = self.redis_client.keys("throughput_concurrent:*")

            total_keys = len(baseline_keys) + len(sustained_keys) + len(concurrent_keys)
            print(f"   Found {total_keys:,} keys to clean up...")

            # Delete keys in batches
            batch_size = 1000
            deleted_count = 0

            # Delete baseline keys
            for i in range(0, len(baseline_keys), batch_size):
                batch = baseline_keys[i : i + batch_size]
                if batch:
                    self.redis_client.delete(*batch)
                    deleted_count += len(batch)
                    print(f"   Deleted {deleted_count:,} keys...")

            # Delete sustained keys
            for i in range(0, len(sustained_keys), batch_size):
                batch = sustained_keys[i : i + batch_size]
                if batch:
                    self.redis_client.delete(*batch)
                    deleted_count += len(batch)
                    print(f"   Deleted {deleted_count:,} keys...")

            # Delete concurrent keys
            for i in range(0, len(concurrent_keys), batch_size):
                batch = concurrent_keys[i : i + batch_size]
                if batch:
                    self.redis_client.delete(*batch)
                    deleted_count += len(batch)
                    print(f"   Deleted {deleted_count:,} keys...")

            # Delete buffer lists
            self.redis_client.delete("throughput_baseline_buffer")
            self.redis_client.delete("throughput_sustained_buffer")

            for i in range(self.test_config["concurrent_threads"]):
                self.redis_client.delete(f"throughput_concurrent_buffer:{i}")

            print(f"   ✅ Cleaned up {deleted_count:,} keys")
            return True

        except Exception as e:
            print(f"   ❌ Error cleaning up throughput data: {e}")
            return False

    def run_throughput_validation_test(self) -> Dict[str, Any]:
        """Run the complete throughput validation test suite."""
        print("🚀 Redis Throughput Validation Testing for Bluesky Data Pipeline")
        print("=" * 60)

        # Connect to Redis
        if not self.connect_to_redis():
            return {"success": False, "error": "Failed to connect to Redis"}

        # Get initial Redis state
        initial_info = self.get_redis_info()
        print("📊 Initial Redis state:")
        print(f"   Version: {initial_info.get('version', 'unknown')}")
        print(f"   Uptime: {initial_info.get('uptime', 0)}s")
        print(f"   Connected clients: {initial_info.get('connected_clients', 0)}")
        print(f"   Used memory: {initial_info.get('used_memory', 0):,} bytes")

        # Run baseline throughput test
        print("\n" + "=" * 60)
        baseline_results = self.run_baseline_throughput_test()
        self.results["baseline_test"] = baseline_results

        # Run sustained throughput test
        print("\n" + "=" * 60)
        sustained_results = self.run_sustained_throughput_test()
        self.results["sustained_test"] = sustained_results

        # Run concurrent throughput test
        print("\n" + "=" * 60)
        concurrent_results = self.run_concurrent_throughput_test()
        self.results["concurrent_test"] = concurrent_results

        # Analyze performance
        self.analyze_performance()

        # Print report
        self.print_report(self.results)

        # Save report
        self.save_report(self.results)

        # Cleanup
        print("\n" + "=" * 60)
        self.cleanup_throughput_data()
        print("✅ Throughput test data cleanup completed successfully")

        # Set overall success flag
        self.results["success"] = True

        return self.results

    def analyze_performance(self):
        """Analyze performance results and generate recommendations."""
        baseline = self.results["baseline_test"]
        sustained = self.results["sustained_test"]
        concurrent = self.results["concurrent_test"]

        # Performance analysis
        analysis = {
            "baseline_throughput": baseline.get("avg_throughput", 0),
            "sustained_throughput": sustained.get("avg_throughput", 0),
            "concurrent_throughput": concurrent.get("total_throughput", 0),
            "target_met": True,
            "performance_degradation": 0,
            "throughput_stability": "stable",
        }

        # Check if target throughput is met
        target = self.test_config["target_throughput"]
        if baseline.get("avg_throughput", 0) < target:
            analysis["target_met"] = False

        # Calculate performance degradation
        if baseline.get("avg_throughput", 0) > 0:
            degradation = (
                (baseline.get("avg_throughput", 0) - sustained.get("avg_throughput", 0))
                / baseline.get("avg_throughput", 0)
            ) * 100
            analysis["performance_degradation"] = degradation

        # Assess throughput stability
        if sustained.get("throughput_std", 0) < 100:
            analysis["throughput_stability"] = "stable"
        elif sustained.get("throughput_std", 0) < 200:
            analysis["throughput_stability"] = "moderate"
        else:
            analysis["throughput_stability"] = "unstable"

        # Generate recommendations
        recommendations = []

        if not analysis["target_met"]:
            recommendations.append("❌ Throughput below target of 1000 ops/sec")
        else:
            recommendations.append("✅ Throughput exceeds target of 1000 ops/sec")

        if analysis["performance_degradation"] > 10:
            recommendations.append(
                f"⚠️ Performance degradation of {analysis['performance_degradation']:.1f}% detected"
            )
        else:
            recommendations.append("✅ Minimal performance degradation detected")

        if analysis["throughput_stability"] == "unstable":
            recommendations.append("⚠️ Throughput stability issues detected")
        else:
            recommendations.append("✅ Throughput stability is good")

        if concurrent.get("total_throughput", 0) > baseline.get("avg_throughput", 0):
            recommendations.append("✅ Concurrent performance scales well")
        else:
            recommendations.append("⚠️ Concurrent performance scaling issues detected")

        self.results["performance_analysis"] = analysis
        self.results["recommendations"] = recommendations

    def print_report(self, results: Dict[str, Any]):
        """Print the throughput validation test report."""
        print("\n" + "=" * 60)
        print("📋 REDIS THROUGHPUT VALIDATION TEST REPORT")
        print("=" * 60)
        print(f"Timestamp: {results['timestamp']}")

        print("\n📋 TEST CONFIGURATION")
        print("-" * 30)
        config = self.test_config
        print(f"Baseline duration: {config['baseline_duration']}s")
        print(f"Sustained duration: {config['sustained_duration']}s")
        print(f"Concurrent duration: {config['concurrent_duration']}s")
        print(f"Target throughput: {config['target_throughput']} ops/sec")
        print(f"Concurrent threads: {config['concurrent_threads']}")

        print("\n🧪 BASELINE THROUGHPUT TEST RESULTS")
        print("-" * 30)
        baseline = results["baseline_test"]
        if baseline.get("success"):
            print(f"Duration: {baseline['duration']:.1f}s")
            print(f"Operations: {baseline['operations']:,}")
            print(f"Average throughput: {baseline['avg_throughput']:.1f} ops/sec")
            print(f"Latency P50: {baseline['latency_p50']:.2f}ms")
            print(f"Latency P95: {baseline['latency_p95']:.2f}ms")
            print(f"Latency P99: {baseline['latency_p99']:.2f}ms")
        else:
            print("❌ Baseline test failed")

        print("\n🔄 SUSTAINED THROUGHPUT TEST RESULTS")
        print("-" * 30)
        sustained = results["sustained_test"]
        if sustained.get("success"):
            print(f"Duration: {sustained['duration']:.1f}s")
            print(f"Operations: {sustained['operations']:,}")
            print(f"Average throughput: {sustained['avg_throughput']:.1f} ops/sec")
            print(f"Throughput std dev: {sustained['throughput_std']:.1f}")
            print(f"Latency P50: {sustained['latency_p50']:.2f}ms")
            print(f"Latency P95: {sustained['latency_p95']:.2f}ms")
            print(f"Latency P99: {sustained['latency_p99']:.2f}ms")
        else:
            print("❌ Sustained test failed")

        print("\n🔄 CONCURRENT THROUGHPUT TEST RESULTS")
        print("-" * 30)
        concurrent = results["concurrent_test"]
        if concurrent.get("success"):
            print(f"Duration: {concurrent['duration']:.1f}s")
            print(f"Threads: {concurrent['threads']}")
            print(f"Total operations: {concurrent['total_operations']:,}")
            print(f"Total throughput: {concurrent['total_throughput']:.1f} ops/sec")
            print(f"Latency P50: {concurrent['latency_p50']:.2f}ms")
            print(f"Latency P95: {concurrent['latency_p95']:.2f}ms")
            print(f"Latency P99: {concurrent['latency_p99']:.2f}ms")
        else:
            print("❌ Concurrent test failed")

        print("\n📊 PERFORMANCE ANALYSIS")
        print("-" * 30)
        analysis = results["performance_analysis"]
        print(f"Target met: {'✅ Yes' if analysis['target_met'] else '❌ No'}")
        print(f"Performance degradation: {analysis['performance_degradation']:.1f}%")
        print(f"Throughput stability: {analysis['throughput_stability']}")

        print("\n💡 RECOMMENDATIONS")
        print("-" * 30)
        for rec in results["recommendations"]:
            print(f"   {rec}")

        print("\n" + "=" * 60)

    def save_report(self, results: Dict[str, Any], filename: str = None):
        """Save the test report to a JSON file."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"redis_throughput_validation_test_{timestamp}.json"

        filepath = os.path.join(os.path.dirname(__file__), filename)
        try:
            with open(filepath, "w") as f:
                json.dump(results, f, indent=2, default=str)
            print(f"💾 Throughput validation test report saved to: {filepath}")
        except Exception as e:
            print(f"❌ Error saving report: {e}")


def main():
    """Main function to run the throughput validation test."""
    # Ensure all prints flush immediately for real-time visibility
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
            os.path.dirname(__file__), f"06_throughput_validation_{ts}.log"
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

    # Get Redis connection parameters from environment
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_password = os.getenv("REDIS_PASSWORD")

    # Create validator
    validator = RedisThroughputValidator(
        host=redis_host, port=redis_port, password=redis_password
    )

    results = validator.run_throughput_validation_test()

    if results.get("success", False):
        print("✅ Throughput validation test completed successfully!")
        return 0
    else:
        print("❌ Throughput validation test failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
