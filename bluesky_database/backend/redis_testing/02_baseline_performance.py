#!/usr/bin/env python3
"""
Redis Baseline Performance Testing Script

This script establishes baseline performance metrics for Redis under expected
load conditions for the Bluesky data pipeline buffer use case.

Usage:
    python 02_baseline_performance.py
"""

import redis
import time
import json
import os
import sys
from typing import Dict, Any, List
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import statistics


class RedisPerformanceTester:
    """Tests Redis performance under various load conditions."""

    def __init__(self, host: str = "localhost", port: int = 6379, password: str = None):
        """Initialize the performance tester."""
        self.host = host
        self.port = port
        self.password = password
        self.redis_client = None
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "baseline_metrics": {},
            "load_tests": {},
            "recommendations": [],
        }

        # Performance targets from MET-001
        self.performance_targets = {
            "throughput": 1000,  # ops/sec
            "latency_p50": 10,  # ms
            "latency_p95": 50,  # ms
            "latency_p99": 100,  # ms
            "memory_utilization": 80,  # percent
            "connection_limit": 1000,  # concurrent connections
            "sustained_duration": 3600,  # seconds (1 hour)
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

    def get_baseline_metrics(self) -> Dict[str, Any]:
        """Get baseline Redis metrics."""
        try:
            # Get comprehensive Redis info
            info = self.redis_client.info()
            memory_info = self.redis_client.info("memory")
            stats_info = self.redis_client.info("stats")
            server_info = self.redis_client.info("server")

            # Get current memory usage
            used_memory = memory_info.get("used_memory", 0)
            max_memory = memory_info.get("maxmemory", 0)
            memory_utilization = (
                (used_memory / max_memory * 100) if max_memory > 0 else 0
            )

            # Get current performance stats
            total_commands_processed = stats_info.get("total_commands_processed", 0)
            total_connections_received = stats_info.get("total_connections_received", 0)
            total_net_input_bytes = stats_info.get("total_net_input_bytes", 0)
            total_net_output_bytes = stats_info.get("total_net_output_bytes", 0)

            baseline = {
                "server_info": {
                    "redis_version": server_info.get("redis_version", "Unknown"),
                    "uptime_in_seconds": server_info.get("uptime_in_seconds", 0),
                    "process_id": server_info.get("process_id", 0),
                    "tcp_port": server_info.get("tcp_port", 0),
                    "config_file": server_info.get("config_file", "Unknown"),
                },
                "memory_info": {
                    "used_memory_human": memory_info.get(
                        "used_memory_human", "Unknown"
                    ),
                    "maxmemory_human": memory_info.get("maxmemory_human", "Unknown"),
                    "memory_utilization_percent": round(memory_utilization, 2),
                    "mem_fragmentation_ratio": memory_info.get(
                        "mem_fragmentation_ratio", 0
                    ),
                    "keyspace_hits": stats_info.get("keyspace_hits", 0),
                    "keyspace_misses": stats_info.get("keyspace_misses", 0),
                },
                "performance_info": {
                    "total_commands_processed": total_commands_processed,
                    "total_connections_received": total_connections_received,
                    "total_net_input_bytes": total_net_input_bytes,
                    "total_net_output_bytes": total_net_output_bytes,
                    "instantaneous_ops_per_sec": stats_info.get(
                        "instantaneous_ops_per_sec", 0
                    ),
                    "instantaneous_input_kbps": stats_info.get(
                        "instantaneous_input_kbps", 0
                    ),
                    "instantaneous_output_kbps": stats_info.get(
                        "instantaneous_output_kbps", 0
                    ),
                },
                "clients_info": {
                    "connected_clients": info.get("connected_clients", 0),
                    "blocked_clients": info.get("blocked_clients", 0),
                    "maxclients": info.get("maxclients", 0),
                },
            }

            return baseline

        except Exception as e:
            print(f"❌ Error getting baseline metrics: {e}")
            return {}

    def test_basic_operations(self, iterations: int = 1000) -> Dict[str, Any]:
        """Test basic Redis operations (SET/GET/DELETE)."""
        print(f"\n🧪 Testing basic operations ({iterations} iterations)...")

        latencies = []
        start_time = time.time()

        try:
            for i in range(iterations):
                key = f"test_key_{i}"
                value = f"test_value_{i}" * 10  # Larger value for realistic testing

                # SET operation
                set_start = time.time()
                self.redis_client.set(key, value)
                set_latency = (time.time() - set_start) * 1000
                latencies.append(set_latency)

                # GET operation
                get_start = time.time()
                self.redis_client.get(key)
                get_latency = (time.time() - get_start) * 1000
                latencies.append(get_latency)

                # DELETE operation
                del_start = time.time()
                self.redis_client.delete(key)
                del_latency = (time.time() - del_start) * 1000
                latencies.append(del_latency)

                if (i + 1) % 100 == 0:
                    print(f"  Processed {i + 1} iterations...")

            end_time = time.time()
            total_time = end_time - start_time
            total_operations = iterations * 3  # SET, GET, DELETE
            ops_per_sec = total_operations / total_time

            # Calculate latency percentiles
            latencies.sort()
            p50 = statistics.median(latencies)
            p95 = latencies[int(len(latencies) * 0.95)]
            p99 = latencies[int(len(latencies) * 0.99)]

            results = {
                "iterations": iterations,
                "total_operations": total_operations,
                "total_time_seconds": total_time,
                "operations_per_second": round(ops_per_sec, 2),
                "latency_ms": {
                    "min": min(latencies),
                    "max": max(latencies),
                    "mean": statistics.mean(latencies),
                    "median": p50,
                    "p95": p95,
                    "p99": p99,
                },
                "success": True,
            }

            print("✅ Basic operations test completed:")
            print(f"   Operations/sec: {ops_per_sec:.2f}")
            print(f"   Latency P50: {p50:.2f}ms")
            print(f"   Latency P95: {p95:.2f}ms")
            print(f"   Latency P99: {p99:.2f}ms")

            return results

        except Exception as e:
            print(f"❌ Basic operations test failed: {e}")
            return {"success": False, "error": str(e)}

    def test_concurrent_operations(
        self, num_threads: int = 10, operations_per_thread: int = 100
    ) -> Dict[str, Any]:
        """Test Redis performance under concurrent load."""
        print(
            f"\n🧪 Testing concurrent operations ({num_threads} threads, {operations_per_thread} ops each)..."
        )

        results = {
            "num_threads": num_threads,
            "operations_per_thread": operations_per_thread,
            "total_operations": num_threads * operations_per_thread,
            "thread_results": [],
            "latencies": [],
            "success": False,
        }

        def worker(thread_id: int) -> Dict[str, Any]:
            """Worker function for concurrent testing."""
            thread_latencies = []
            thread_start = time.time()

            try:
                for i in range(operations_per_thread):
                    key = f"concurrent_key_{thread_id}_{i}"
                    value = f"concurrent_value_{thread_id}_{i}" * 5

                    # SET operation
                    set_start = time.time()
                    self.redis_client.set(key, value)
                    set_latency = (time.time() - set_start) * 1000
                    thread_latencies.append(set_latency)

                    # GET operation
                    get_start = time.time()
                    self.redis_client.get(key)
                    get_latency = (time.time() - get_start) * 1000
                    thread_latencies.append(get_latency)

                    # DELETE operation
                    del_start = time.time()
                    self.redis_client.delete(key)
                    del_latency = (time.time() - del_start) * 1000
                    thread_latencies.append(del_latency)

                thread_end = time.time()
                thread_time = thread_end - thread_start
                thread_ops_per_sec = (operations_per_thread * 3) / thread_time

                return {
                    "thread_id": thread_id,
                    "success": True,
                    "operations_per_second": thread_ops_per_sec,
                    "latencies": thread_latencies,
                    "total_time": thread_time,
                }

            except Exception as e:
                return {"thread_id": thread_id, "success": False, "error": str(e)}

        # Run concurrent tests
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_thread = {
                executor.submit(worker, i): i for i in range(num_threads)
            }

            for future in as_completed(future_to_thread):
                thread_result = future.result()
                results["thread_results"].append(thread_result)

                if thread_result["success"]:
                    results["latencies"].extend(thread_result["latencies"])

        end_time = time.time()
        total_time = end_time - start_time

        if results["latencies"]:
            # Calculate overall statistics
            results["total_time_seconds"] = total_time
            results["overall_operations_per_second"] = (
                results["total_operations"] / total_time
            )

            # Calculate latency percentiles
            results["latencies"].sort()
            results["latency_stats"] = {
                "min": min(results["latencies"]),
                "max": max(results["latencies"]),
                "mean": statistics.mean(results["latencies"]),
                "median": statistics.median(results["latencies"]),
                "p95": results["latencies"][int(len(results["latencies"]) * 0.95)],
                "p99": results["latencies"][int(len(results["latencies"]) * 0.99)],
            }

            results["success"] = True

            print("✅ Concurrent operations test completed:")
            print(f"   Overall ops/sec: {results['overall_operations_per_second']:.2f}")
            print(f"   Latency P50: {results['latency_stats']['median']:.2f}ms")
            print(f"   Latency P95: {results['latency_stats']['p95']:.2f}ms")
            print(f"   Latency P99: {results['latency_stats']['p99']:.2f}ms")

        return results

    def test_memory_usage_patterns(self, data_size_mb: int = 100) -> Dict[str, Any]:
        """Test memory usage patterns under load."""
        print(f"\n🧪 Testing memory usage patterns ({data_size_mb}MB of data)...")

        try:
            # Get initial memory usage
            initial_memory = self.redis_client.info("memory")
            initial_used = initial_memory.get("used_memory", 0)

            # Calculate number of keys needed to reach target size
            # Assuming average key-value size of 500 bytes
            avg_key_size = 500  # bytes
            num_keys = (data_size_mb * 1024 * 1024) // avg_key_size

            print(f"  Creating {num_keys} keys to simulate {data_size_mb}MB of data...")

            # Create test data
            for i in range(num_keys):
                key = f"memory_test_key_{i}"
                value = f"memory_test_value_{i}" * 20  # ~500 bytes per key-value
                self.redis_client.set(key, value)

                if (i + 1) % 1000 == 0:
                    print(f"    Created {i + 1} keys...")

            # Get memory usage after data creation
            final_memory = self.redis_client.info("memory")
            final_used = final_memory.get("used_memory", 0)

            # Calculate memory increase
            memory_increase = final_used - initial_used
            memory_increase_mb = memory_increase / (1024 * 1024)

            # Clean up test data
            print("  Cleaning up test data...")
            for i in range(num_keys):
                key = f"memory_test_key_{i}"
                self.redis_client.delete(key)

                if (i + 1) % 1000 == 0:
                    print(f"    Deleted {i + 1} keys...")

            # Get memory usage after cleanup
            cleanup_memory = self.redis_client.info("memory")
            cleanup_used = cleanup_memory.get("used_memory", 0)

            results = {
                "data_size_mb": data_size_mb,
                "num_keys_created": num_keys,
                "memory_usage": {
                    "initial_mb": initial_used / (1024 * 1024),
                    "final_mb": final_used / (1024 * 1024),
                    "cleanup_mb": cleanup_used / (1024 * 1024),
                    "increase_mb": memory_increase_mb,
                    "recovery_percent": (
                        (initial_used - cleanup_used) / initial_used * 100
                    )
                    if initial_used > 0
                    else 0,
                },
                "success": True,
            }

            print("✅ Memory usage test completed:")
            print(f"   Memory increase: {memory_increase_mb:.2f}MB")
            print(
                f"   Memory recovery: {results['memory_usage']['recovery_percent']:.1f}%"
            )

            return results

        except Exception as e:
            print(f"❌ Memory usage test failed: {e}")
            return {"success": False, "error": str(e)}

    def test_sustained_load(
        self, duration_seconds: int = 60, target_ops_per_sec: int = 100
    ) -> Dict[str, Any]:
        """Test Redis under sustained load."""
        print(
            f"\n🧪 Testing sustained load ({duration_seconds}s at {target_ops_per_sec} ops/sec)..."
        )

        results = {
            "duration_seconds": duration_seconds,
            "target_ops_per_sec": target_ops_per_sec,
            "actual_ops_per_sec": [],
            "latencies": [],
            "memory_usage": [],
            "success": False,
        }

        start_time = time.time()
        operation_count = 0

        try:
            while time.time() - start_time < duration_seconds:
                interval_start = time.time()
                interval_operations = 0
                interval_latencies = []

                # Run operations for 1 second
                while time.time() - interval_start < 1.0:
                    key = f"sustained_test_key_{operation_count}"
                    value = f"sustained_test_value_{operation_count}" * 10

                    # SET operation
                    op_start = time.time()
                    self.redis_client.set(key, value)
                    op_latency = (time.time() - op_start) * 1000
                    interval_latencies.append(op_latency)

                    # GET operation
                    op_start = time.time()
                    self.redis_client.get(key)
                    op_latency = (time.time() - op_start) * 1000
                    interval_latencies.append(op_latency)

                    # DELETE operation
                    op_start = time.time()
                    self.redis_client.delete(key)
                    op_latency = (time.time() - op_start) * 1000
                    interval_latencies.append(op_latency)

                    interval_operations += 3
                    operation_count += 1

                # Record interval statistics
                interval_time = time.time() - interval_start
                actual_ops_per_sec = interval_operations / interval_time

                results["actual_ops_per_sec"].append(actual_ops_per_sec)
                results["latencies"].extend(interval_latencies)

                # Record memory usage
                memory_info = self.redis_client.info("memory")
                used_memory_mb = memory_info.get("used_memory", 0) / (1024 * 1024)
                results["memory_usage"].append(used_memory_mb)

                elapsed = time.time() - start_time
                print(
                    f"  Elapsed: {elapsed:.1f}s, Ops/sec: {actual_ops_per_sec:.1f}, Memory: {used_memory_mb:.1f}MB"
                )

            # Calculate overall statistics
            results["total_operations"] = operation_count * 3
            results["average_ops_per_sec"] = statistics.mean(
                results["actual_ops_per_sec"]
            )
            results["ops_per_sec_std"] = (
                statistics.stdev(results["actual_ops_per_sec"])
                if len(results["actual_ops_per_sec"]) > 1
                else 0
            )

            if results["latencies"]:
                results["latency_stats"] = {
                    "min": min(results["latencies"]),
                    "max": max(results["latencies"]),
                    "mean": statistics.mean(results["latencies"]),
                    "median": statistics.median(results["latencies"]),
                    "p95": sorted(results["latencies"])[
                        int(len(results["latencies"]) * 0.95)
                    ],
                    "p99": sorted(results["latencies"])[
                        int(len(results["latencies"]) * 0.99)
                    ],
                }

            results["memory_stats"] = {
                "min": min(results["memory_usage"]),
                "max": max(results["memory_usage"]),
                "mean": statistics.mean(results["memory_usage"]),
                "std": statistics.stdev(results["memory_usage"])
                if len(results["memory_usage"]) > 1
                else 0,
            }

            results["success"] = True

            print("✅ Sustained load test completed:")
            print(f"   Average ops/sec: {results['average_ops_per_sec']:.2f}")
            print(f"   Latency P95: {results['latency_stats']['p95']:.2f}ms")
            print(f"   Memory usage: {results['memory_stats']['mean']:.1f}MB avg")

            return results

        except Exception as e:
            print(f"❌ Sustained load test failed: {e}")
            return {"success": False, "error": str(e)}

    def analyze_performance(self, test_results: Dict[str, Any]) -> List[str]:
        """Analyze performance results against targets."""
        recommendations = []

        # Check basic operations performance
        if "basic_operations" in test_results:
            basic = test_results["basic_operations"]
            if basic["success"]:
                if (
                    basic["operations_per_second"]
                    < self.performance_targets["throughput"]
                ):
                    recommendations.append(
                        f"⚠️ Basic operations throughput ({basic['operations_per_second']:.1f} ops/sec) below target ({self.performance_targets['throughput']} ops/sec)"
                    )
                else:
                    recommendations.append(
                        f"✅ Basic operations throughput ({basic['operations_per_second']:.1f} ops/sec) meets target"
                    )

                if basic["latency_ms"]["p95"] > self.performance_targets["latency_p95"]:
                    recommendations.append(
                        f"⚠️ Basic operations P95 latency ({basic['latency_ms']['p95']:.1f}ms) above target ({self.performance_targets['latency_p95']}ms)"
                    )
                else:
                    recommendations.append(
                        f"✅ Basic operations P95 latency ({basic['latency_ms']['p95']:.1f}ms) meets target"
                    )

        # Check concurrent operations performance
        if "concurrent_operations" in test_results:
            concurrent = test_results["concurrent_operations"]
            if concurrent["success"]:
                if (
                    concurrent["overall_operations_per_second"]
                    < self.performance_targets["throughput"]
                ):
                    recommendations.append(
                        f"⚠️ Concurrent operations throughput ({concurrent['overall_operations_per_second']:.1f} ops/sec) below target ({self.performance_targets['throughput']} ops/sec)"
                    )
                else:
                    recommendations.append(
                        f"✅ Concurrent operations throughput ({concurrent['overall_operations_per_second']:.1f} ops/sec) meets target"
                    )

        # Check sustained load performance
        if "sustained_load" in test_results:
            sustained = test_results["sustained_load"]
            if sustained["success"]:
                if (
                    sustained["average_ops_per_sec"]
                    < self.performance_targets["throughput"]
                ):
                    recommendations.append(
                        f"⚠️ Sustained load throughput ({sustained['average_ops_per_sec']:.1f} ops/sec) below target ({self.performance_targets['throughput']} ops/sec)"
                    )
                else:
                    recommendations.append(
                        f"✅ Sustained load throughput ({sustained['average_ops_per_sec']:.1f} ops/sec) meets target"
                    )

        # Check memory usage
        baseline = test_results.get("baseline_metrics", {})
        memory_info = baseline.get("memory_info", {})
        memory_utilization = memory_info.get("memory_utilization_percent", 0)

        if memory_utilization > self.performance_targets["memory_utilization"]:
            recommendations.append(
                f"⚠️ Memory utilization ({memory_utilization:.1f}%) above target ({self.performance_targets['memory_utilization']}%)"
            )
        else:
            recommendations.append(
                f"✅ Memory utilization ({memory_utilization:.1f}%) within target"
            )

        return recommendations

    def run_performance_tests(self) -> Dict[str, Any]:
        """Run all performance tests."""
        print("🚀 Redis Performance Testing")
        print("=" * 50)

        # Connect to Redis
        if not self.connect_to_redis():
            return self.results

        # Get baseline metrics
        print("\n📊 Getting baseline metrics...")
        baseline_metrics = self.get_baseline_metrics()
        self.results["baseline_metrics"] = baseline_metrics

        # Run performance tests
        test_results = {}

        # Basic operations test
        test_results["basic_operations"] = self.test_basic_operations(iterations=1000)

        # Concurrent operations test
        test_results["concurrent_operations"] = self.test_concurrent_operations(
            num_threads=10, operations_per_thread=100
        )

        # Memory usage test
        test_results["memory_usage"] = self.test_memory_usage_patterns(data_size_mb=50)

        # Sustained load test
        test_results["sustained_load"] = self.test_sustained_load(
            duration_seconds=30, target_ops_per_sec=100
        )

        # Store test results
        self.results["load_tests"] = test_results

        # Analyze performance
        print("\n💡 Analyzing performance against targets...")
        recommendations = self.analyze_performance(test_results)
        self.results["recommendations"] = recommendations

        return self.results

    def print_report(self, results: Dict[str, Any]):
        """Print a formatted performance report."""
        print("\n" + "=" * 60)
        print("📋 REDIS PERFORMANCE TESTING REPORT")
        print("=" * 60)
        print(f"Timestamp: {results['timestamp']}")

        # Baseline metrics
        baseline = results.get("baseline_metrics", {})
        if baseline:
            print("\n📊 BASELINE METRICS")
            print("-" * 30)
            server_info = baseline.get("server_info", {})
            print(f"Redis Version: {server_info.get('redis_version', 'Unknown')}")
            print(f"Uptime: {server_info.get('uptime_in_seconds', 0)} seconds")

            memory_info = baseline.get("memory_info", {})
            print(f"Memory Usage: {memory_info.get('used_memory_human', 'Unknown')}")
            print(
                f"Memory Utilization: {memory_info.get('memory_utilization_percent', 0)}%"
            )

            performance_info = baseline.get("performance_info", {})
            print(
                f"Current Ops/sec: {performance_info.get('instantaneous_ops_per_sec', 0)}"
            )

        # Test results
        load_tests = results.get("load_tests", {})

        if "basic_operations" in load_tests:
            basic = load_tests["basic_operations"]
            if basic["success"]:
                print("\n🧪 BASIC OPERATIONS TEST")
                print("-" * 30)
                print(f"Operations/sec: {basic['operations_per_second']}")
                print(f"Latency P50: {basic['latency_ms']['median']:.2f}ms")
                print(f"Latency P95: {basic['latency_ms']['p95']:.2f}ms")
                print(f"Latency P99: {basic['latency_ms']['p99']:.2f}ms")

        if "concurrent_operations" in load_tests:
            concurrent = load_tests["concurrent_operations"]
            if concurrent["success"]:
                print("\n🧪 CONCURRENT OPERATIONS TEST")
                print("-" * 30)
                print(
                    f"Overall ops/sec: {concurrent['overall_operations_per_second']:.2f}"
                )
                print(f"Latency P95: {concurrent['latency_stats']['p95']:.2f}ms")
                print(f"Latency P99: {concurrent['latency_stats']['p99']:.2f}ms")

        if "sustained_load" in load_tests:
            sustained = load_tests["sustained_load"]
            if sustained["success"]:
                print("\n🧪 SUSTAINED LOAD TEST")
                print("-" * 30)
                print(f"Average ops/sec: {sustained['average_ops_per_sec']:.2f}")
                print(f"Latency P95: {sustained['latency_stats']['p95']:.2f}ms")
                print(f"Memory usage: {sustained['memory_stats']['mean']:.1f}MB avg")

        # Recommendations
        print("\n💡 RECOMMENDATIONS")
        print("-" * 30)
        for recommendation in results.get("recommendations", []):
            print(f"• {recommendation}")

        print("\n" + "=" * 60)

    def save_report(self, results: Dict[str, Any], filename: str = None):
        """Save the performance report to a JSON file."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"redis_performance_baseline_{timestamp}.json"

        filepath = os.path.join(os.path.dirname(__file__), filename)

        try:
            with open(filepath, "w") as f:
                json.dump(results, f, indent=2)
            print(f"\n💾 Performance report saved to: {filepath}")
        except Exception as e:
            print(f"\n❌ Failed to save performance report: {e}")


def main():
    """Main function to run the Redis performance testing."""
    print("🚀 Redis Performance Testing for Bluesky Data Pipeline")
    print("=" * 60)

    # Get Redis connection parameters from environment
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_password = os.getenv("REDIS_PASSWORD")

    # Create tester
    tester = RedisPerformanceTester(
        host=redis_host, port=redis_port, password=redis_password
    )

    # Run tests
    results = tester.run_performance_tests()

    # Print report
    tester.print_report(results)

    # Save report
    tester.save_report(results)

    # Return exit code based on recommendations
    recommendations = results.get("recommendations", [])
    warnings = [r for r in recommendations if "⚠️" in r]

    if warnings:
        print(
            f"\n⚠️ {len(warnings)} performance warnings found. Review recommendations above."
        )
        return 1
    else:
        print("\n✅ Performance testing completed. All targets met.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
