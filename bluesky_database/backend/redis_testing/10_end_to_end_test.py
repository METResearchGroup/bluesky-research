#!/usr/bin/env python3
"""
End-to-End DataWriter Pipeline Test

This script runs a comprehensive 10-minute test of the complete DataWriter pipeline:
- Mock data stream generation
- DataWriter Prefect flow execution
- Service validation
- Parquet file validation
- Performance monitoring

Author: AI Assistant
Date: 2025-08-08
"""

import json
import time
import subprocess
import threading
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
import logging

import redis
import requests
import polars as pl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            f'end_to_end_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        ),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class EndToEndTest:
    """Comprehensive end-to-end test for the DataWriter pipeline"""

    def __init__(self, test_duration_minutes: int = 10):
        self.test_duration = test_duration_minutes
        self.start_time = None
        self.end_time = None
        self.test_results = {}
        self.mock_stream_process = None
        self.datawriter_process = None
        self.running = False

        # Test configuration
        self.redis_host = "localhost"
        self.redis_port = 6379
        self.prefect_api_url = "http://localhost:4200/api"
        self.output_dir = "./data"

        # Expected results
        self.expected_events_per_minute = 600  # 10 events/sec * 60 seconds
        self.expected_total_events = (
            test_duration_minutes * self.expected_events_per_minute
        )

    def log_test_result(
        self, test_name: str, success: bool, details: str = "", metrics: Dict = None
    ):
        """Log test result with details"""
        result = {
            "test_name": test_name,
            "success": success,
            "details": details,
            "timestamp": datetime.now().isoformat(),
            "metrics": metrics or {},
        }
        self.test_results[test_name] = result

        if success:
            logger.info(f"✅ {test_name}: PASSED - {details}")
        else:
            logger.error(f"❌ {test_name}: FAILED - {details}")

    def test_redis_connectivity(self) -> bool:
        """Test Redis connectivity and basic operations"""
        logger.info("🔗 Testing Redis connectivity...")

        try:
            r = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                decode_responses=True,
                socket_connect_timeout=10,
            )
            r.ping()

            # Test basic operations
            r.set("test_key", "test_value")
            value = r.get("test_key")
            r.delete("test_key")

            if value == "test_value":
                self.log_test_result(
                    "redis_connectivity",
                    True,
                    "Redis connection and basic operations working",
                    {"host": self.redis_host, "port": self.redis_port},
                )
                return True
            else:
                self.log_test_result(
                    "redis_connectivity", False, "Redis basic operations failed"
                )
                return False

        except Exception as e:
            self.log_test_result(
                "redis_connectivity", False, f"Redis connection failed: {e}"
            )
            return False

    def test_prefect_server(self) -> bool:
        """Test Prefect server connectivity"""
        logger.info("🔗 Testing Prefect server connectivity...")

        try:
            response = requests.get(f"{self.prefect_api_url}/health", timeout=10)
            if response.status_code == 200:
                self.log_test_result(
                    "prefect_server",
                    True,
                    "Prefect server responding",
                    {"status_code": response.status_code},
                )
                return True
            else:
                self.log_test_result(
                    "prefect_server",
                    False,
                    f"Prefect server returned status {response.status_code}",
                )
                return False

        except Exception as e:
            self.log_test_result(
                "prefect_server", False, f"Prefect server connection failed: {e}"
            )
            return False

    def test_monitoring_stack(self) -> bool:
        """Test monitoring stack (Prometheus, Grafana)"""
        logger.info("📊 Testing monitoring stack...")

        try:
            # Test Prometheus
            prometheus_response = requests.get(
                "http://localhost:9090/-/healthy", timeout=10
            )
            if prometheus_response.status_code != 200:
                self.log_test_result(
                    "monitoring_stack", False, "Prometheus not accessible"
                )
                return False

            # Test Grafana
            grafana_response = requests.get(
                "http://localhost:3000/api/health", timeout=10
            )
            if grafana_response.status_code != 200:
                self.log_test_result(
                    "monitoring_stack", False, "Grafana not accessible"
                )
                return False

            self.log_test_result(
                "monitoring_stack",
                True,
                "All monitoring components accessible",
                {
                    "prometheus_status": prometheus_response.status_code,
                    "grafana_status": grafana_response.status_code,
                },
            )
            return True

        except Exception as e:
            self.log_test_result(
                "monitoring_stack", False, f"Monitoring stack test failed: {e}"
            )
            return False

    def start_mock_data_stream(self) -> bool:
        """Start the mock data stream in a separate process"""
        logger.info("🚀 Starting mock data stream...")

        try:
            # Start mock data stream
            cmd = [
                sys.executable,
                "09_mock_data_stream.py",
                "--duration",
                str(self.test_duration),
                "--rate",
                "10",
                "--redis-host",
                self.redis_host,
                "--redis-port",
                str(self.redis_port),
            ]

            self.mock_stream_process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )

            # Wait a moment for the process to start
            time.sleep(2)

            if self.mock_stream_process.poll() is None:
                self.log_test_result(
                    "mock_stream_start", True, "Mock data stream started successfully"
                )
                return True
            else:
                stdout, stderr = self.mock_stream_process.communicate()
                self.log_test_result(
                    "mock_stream_start",
                    False,
                    f"Mock data stream failed to start: {stderr}",
                )
                return False

        except Exception as e:
            self.log_test_result(
                "mock_stream_start", False, f"Failed to start mock data stream: {e}"
            )
            return False

    def start_datawriter_flow(self) -> bool:
        """Start the DataWriter Prefect flow"""
        logger.info("🔄 Starting DataWriter flow...")

        try:
            # Start DataWriter flow
            cmd = [
                sys.executable,
                "-m",
                "prefect",
                "deployment",
                "run",
                "datawriter-flow/datawriter-flow",
                "--params",
                json.dumps(
                    {
                        "stream_names": [
                            "bluesky_posts",
                            "bluesky_likes",
                            "bluesky_reposts",
                            "bluesky_follows",
                            "bluesky_blocks",
                        ],
                        "batch_size": 1000,
                        "output_dir": self.output_dir,
                        "max_stream_length": 100000,
                    }
                ),
            ]

            self.datawriter_process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )

            # Wait a moment for the process to start
            time.sleep(2)

            if self.datawriter_process.poll() is None:
                self.log_test_result(
                    "datawriter_start", True, "DataWriter flow started successfully"
                )
                return True
            else:
                stdout, stderr = self.datawriter_process.communicate()
                self.log_test_result(
                    "datawriter_start",
                    False,
                    f"DataWriter flow failed to start: {stderr}",
                )
                return False

        except Exception as e:
            self.log_test_result(
                "datawriter_start", False, f"Failed to start DataWriter flow: {e}"
            )
            return False

    def monitor_test_progress(self):
        """Monitor the test progress and collect metrics"""
        logger.info("📊 Monitoring test progress...")

        start_time = time.time()
        last_check = start_time

        while self.running and (time.time() - start_time) < (self.test_duration * 60):
            current_time = time.time()
            elapsed_minutes = (current_time - start_time) / 60

            # Check every 30 seconds
            if current_time - last_check >= 30:
                logger.info(
                    f"⏱️ Test progress: {elapsed_minutes:.1f}/{self.test_duration} minutes"
                )

                # Check if processes are still running
                if (
                    self.mock_stream_process
                    and self.mock_stream_process.poll() is not None
                ):
                    logger.warning("⚠️ Mock data stream process stopped unexpectedly")

                if (
                    self.datawriter_process
                    and self.datawriter_process.poll() is not None
                ):
                    logger.warning("⚠️ DataWriter process stopped unexpectedly")

                last_check = current_time

            time.sleep(5)

    def validate_redis_streams(self) -> bool:
        """Validate that Redis streams contain expected data"""
        logger.info("📊 Validating Redis streams...")

        try:
            r = redis.Redis(
                host=self.redis_host, port=self.redis_port, decode_responses=True
            )

            stream_names = [
                "bluesky_posts",
                "bluesky_likes",
                "bluesky_reposts",
                "bluesky_follows",
                "bluesky_blocks",
            ]

            total_messages = 0
            stream_stats = {}

            for stream_name in stream_names:
                try:
                    stream_info = r.xinfo_stream(stream_name)
                    message_count = stream_info["length"]
                    stream_stats[stream_name] = message_count
                    total_messages += message_count
                except redis.exceptions.ResponseError as e:
                    if "no such key" in str(e).lower():
                        stream_stats[stream_name] = 0
                    else:
                        raise

            # Check if we have reasonable number of messages
            expected_min_messages = (
                self.expected_total_events * 0.8
            )  # Allow 20% tolerance
            expected_max_messages = self.expected_total_events * 1.2

            if expected_min_messages <= total_messages <= expected_max_messages:
                self.log_test_result(
                    "redis_streams_validation",
                    True,
                    f"Redis streams contain {total_messages} messages (expected: {self.expected_total_events})",
                    {"total_messages": total_messages, "stream_stats": stream_stats},
                )
                return True
            else:
                self.log_test_result(
                    "redis_streams_validation",
                    False,
                    f"Redis streams contain {total_messages} messages (expected: {self.expected_total_events})",
                    {"total_messages": total_messages, "stream_stats": stream_stats},
                )
                return False

        except Exception as e:
            self.log_test_result(
                "redis_streams_validation",
                False,
                f"Redis streams validation failed: {e}",
            )
            return False

    def validate_parquet_files(self) -> bool:
        """Validate that Parquet files were created with correct structure and data"""
        logger.info("📁 Validating Parquet files...")

        try:
            output_path = Path(self.output_dir)
            if not output_path.exists():
                self.log_test_result(
                    "parquet_files_validation",
                    False,
                    f"Output directory {self.output_dir} does not exist",
                )
                return False

            # Find all Parquet files
            parquet_files = list(output_path.rglob("*.parquet"))

            if not parquet_files:
                self.log_test_result(
                    "parquet_files_validation", False, "No Parquet files found"
                )
                return False

            # Validate file structure and content
            total_records = 0
            file_stats = {}

            for file_path in parquet_files:
                try:
                    # Read Parquet file
                    df = pl.read_parquet(str(file_path))
                    record_count = len(df)
                    total_records += record_count

                    # Validate file path structure (year/month/day/hour/type)
                    relative_path = file_path.relative_to(output_path)
                    path_parts = relative_path.parts

                    if len(path_parts) >= 5:
                        year, month, day, hour, data_type = path_parts[:5]

                        # Validate path structure
                        if (
                            year.isdigit()
                            and month.isdigit()
                            and day.isdigit()
                            and hour.isdigit()
                            and data_type.startswith("app_bsky_")
                        ):
                            file_stats[str(file_path)] = {
                                "records": record_count,
                                "year": year,
                                "month": month,
                                "day": day,
                                "hour": hour,
                                "type": data_type,
                            }
                        else:
                            logger.warning(
                                f"⚠️ Invalid file path structure: {file_path}"
                            )
                    else:
                        logger.warning(f"⚠️ Unexpected file path: {file_path}")

                except Exception as e:
                    logger.error(f"❌ Error reading Parquet file {file_path}: {e}")

            # Check if we have reasonable number of records
            expected_min_records = (
                self.expected_total_events * 0.8
            )  # Allow 20% tolerance
            expected_max_records = self.expected_total_events * 1.2

            if expected_min_records <= total_records <= expected_max_records:
                self.log_test_result(
                    "parquet_files_validation",
                    True,
                    f"Parquet files contain {total_records} records (expected: {self.expected_total_events})",
                    {
                        "total_records": total_records,
                        "files_count": len(parquet_files),
                        "file_stats": file_stats,
                    },
                )
                return True
            else:
                self.log_test_result(
                    "parquet_files_validation",
                    False,
                    f"Parquet files contain {total_records} records (expected: {self.expected_total_events})",
                    {
                        "total_records": total_records,
                        "files_count": len(parquet_files),
                        "file_stats": file_stats,
                    },
                )
                return False

        except Exception as e:
            self.log_test_result(
                "parquet_files_validation",
                False,
                f"Parquet files validation failed: {e}",
            )
            return False

    def cleanup_processes(self):
        """Clean up running processes"""
        logger.info("🧹 Cleaning up processes...")

        if self.mock_stream_process:
            try:
                self.mock_stream_process.terminate()
                self.mock_stream_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.mock_stream_process.kill()
            except Exception as e:
                logger.warning(f"⚠️ Error cleaning up mock stream process: {e}")

        if self.datawriter_process:
            try:
                self.datawriter_process.terminate()
                self.datawriter_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.datawriter_process.kill()
            except Exception as e:
                logger.warning(f"⚠️ Error cleaning up DataWriter process: {e}")

    def run_test(self) -> Dict[str, Any]:
        """Run the complete end-to-end test"""
        logger.info("🚀 Starting End-to-End DataWriter Pipeline Test")
        logger.info(f"⏱️ Test duration: {self.test_duration} minutes")

        self.start_time = datetime.now()
        self.running = True

        try:
            # Step 1: Test infrastructure
            logger.info("=" * 60)
            logger.info("STEP 1: Testing Infrastructure")
            logger.info("=" * 60)

            if not self.test_redis_connectivity():
                return {"success": False, "error": "Redis connectivity test failed"}

            if not self.test_prefect_server():
                return {"success": False, "error": "Prefect server test failed"}

            if not self.test_monitoring_stack():
                return {"success": False, "error": "Monitoring stack test failed"}

            # Step 2: Start mock data stream
            logger.info("=" * 60)
            logger.info("STEP 2: Starting Mock Data Stream")
            logger.info("=" * 60)

            if not self.start_mock_data_stream():
                return {"success": False, "error": "Failed to start mock data stream"}

            # Step 3: Start DataWriter flow
            logger.info("=" * 60)
            logger.info("STEP 3: Starting DataWriter Flow")
            logger.info("=" * 60)

            if not self.start_datawriter_flow():
                return {"success": False, "error": "Failed to start DataWriter flow"}

            # Step 4: Monitor test progress
            logger.info("=" * 60)
            logger.info("STEP 4: Monitoring Test Progress")
            logger.info("=" * 60)

            monitor_thread = threading.Thread(target=self.monitor_test_progress)
            monitor_thread.daemon = True
            monitor_thread.start()

            # Wait for test duration
            time.sleep(self.test_duration * 60)

            # Step 5: Stop processes and validate results
            logger.info("=" * 60)
            logger.info("STEP 5: Validating Results")
            logger.info("=" * 60)

            self.running = False
            self.cleanup_processes()

            # Wait a moment for processes to finish
            time.sleep(5)

            # Validate results
            redis_valid = self.validate_redis_streams()
            parquet_valid = self.validate_parquet_files()

            # Calculate test results
            self.end_time = datetime.now()
            test_duration = (self.end_time - self.start_time).total_seconds()

            passed_tests = sum(
                1 for result in self.test_results.values() if result["success"]
            )
            total_tests = len(self.test_results)
            success_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0

            # Generate summary
            summary = {
                "test_name": "End-to-End DataWriter Pipeline Test",
                "start_time": self.start_time.isoformat(),
                "end_time": self.end_time.isoformat(),
                "duration_seconds": test_duration,
                "duration_minutes": test_duration / 60,
                "total_tests": total_tests,
                "passed_tests": passed_tests,
                "failed_tests": total_tests - passed_tests,
                "success_rate": success_rate,
                "overall_success": success_rate >= 80 and redis_valid and parquet_valid,
                "test_results": self.test_results,
            }

            # Print summary
            logger.info("=" * 60)
            logger.info("END-TO-END TEST SUMMARY")
            logger.info("=" * 60)
            logger.info(f"⏱️ Duration: {test_duration/60:.1f} minutes")
            logger.info(
                f"📊 Tests: {passed_tests}/{total_tests} passed ({success_rate:.1f}%)"
            )
            logger.info(
                f"📊 Redis Validation: {'✅ PASSED' if redis_valid else '❌ FAILED'}"
            )
            logger.info(
                f"📁 Parquet Validation: {'✅ PASSED' if parquet_valid else '❌ FAILED'}"
            )
            logger.info(
                f"🎯 Overall Result: {'✅ SUCCESS' if summary['overall_success'] else '❌ FAILED'}"
            )
            logger.info("=" * 60)

            return summary

        except Exception as e:
            logger.error(f"❌ End-to-end test failed: {e}")
            self.running = False
            self.cleanup_processes()
            return {"success": False, "error": str(e)}

        finally:
            self.running = False
            self.cleanup_processes()


def main():
    """Main function to run the end-to-end test"""
    import argparse

    parser = argparse.ArgumentParser(description="End-to-End DataWriter Pipeline Test")
    parser.add_argument(
        "--duration",
        type=int,
        default=10,
        help="Test duration in minutes (default: 10)",
    )
    parser.add_argument(
        "--output-dir",
        default="./data",
        help="Output directory for Parquet files (default: ./data)",
    )

    args = parser.parse_args()

    # Set up signal handler for graceful shutdown
    def signal_handler(sig, frame):
        print("\n⏹️ Received interrupt signal, stopping test...")
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)

    # Run the test
    test = EndToEndTest(args.duration)
    test.output_dir = args.output_dir

    result = test.run_test()

    # Save results to file
    output_file = (
        f"end_to_end_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    with open(output_file, "w") as f:
        json.dump(result, f, indent=2)

    logger.info(f"📄 Test results saved to: {output_file}")

    # Exit with appropriate code
    if result.get("overall_success", False):
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
