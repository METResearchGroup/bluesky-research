#!/usr/bin/env python3
"""
Redis Monitoring Stack Validation Script

This script validates the Prometheus + Grafana monitoring stack for Redis.
It checks that all containers start correctly and metrics are being collected.

Author: AI Assistant
Date: 2025-08-07
"""

import json
import time
import requests
import subprocess
import sys
from datetime import datetime
from typing import Dict, Any
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            f'redis_monitoring_validation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        ),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class MonitoringStackValidator:
    """Validator for Redis monitoring stack (Prometheus + Grafana)"""

    def __init__(self):
        self.test_results = {}
        self.start_time = datetime.now()
        self.validation_id = (
            f"monitoring_validation_{self.start_time.strftime('%Y%m%d_%H%M%S')}"
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

    def check_container_status(self, container_name: str) -> bool:
        """Check if a Docker container is running"""
        try:
            result = subprocess.run(
                [
                    "docker",
                    "ps",
                    "--filter",
                    f"name={container_name}",
                    "--format",
                    "{{.Names}}",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            return container_name in result.stdout.strip()
        except subprocess.CalledProcessError as e:
            logger.error(f"Error checking container {container_name}: {e}")
            return False

    def test_docker_compose_startup(self) -> bool:
        """Test that Docker Compose starts all services correctly"""
        logger.info("🔧 Testing Docker Compose startup...")

        try:
            # Start the monitoring stack
            result = subprocess.run(
                [
                    "docker",
                    "compose",
                    "-f",
                    "../docker-compose.monitoring.yml",
                    "up",
                    "-d",
                ],
                capture_output=True,
                text=True,
                cwd=".",
            )

            if result.returncode != 0:
                self.log_test_result(
                    "docker_compose_startup", False, f"Failed to start: {result.stderr}"
                )
                return False

            # Wait for containers to start
            logger.info("⏳ Waiting for containers to start...")
            time.sleep(30)

            # Check all containers are running
            containers = ["bluesky_redis", "redis_exporter", "prometheus", "grafana"]
            all_running = True

            for container in containers:
                if not self.check_container_status(container):
                    self.log_test_result(
                        "docker_compose_startup",
                        False,
                        f"Container {container} not running",
                    )
                    all_running = False

            if all_running:
                self.log_test_result(
                    "docker_compose_startup",
                    True,
                    "All containers started successfully",
                )
                return True
            else:
                return False

        except Exception as e:
            self.log_test_result(
                "docker_compose_startup", False, f"Exception: {str(e)}"
            )
            return False

    def test_redis_connectivity(self) -> bool:
        """Test Redis connectivity and basic operations"""
        logger.info("🔍 Testing Redis connectivity...")

        try:
            # Test Redis connectivity using redis-cli
            result = subprocess.run(
                ["redis-cli", "-h", "localhost", "-p", "6379", "ping"],
                capture_output=True,
                text=True,
                check=True,
            )

            if result.stdout.strip() == "PONG":
                # Test basic operations
                set_result = subprocess.run(
                    [
                        "redis-cli",
                        "-h",
                        "localhost",
                        "-p",
                        "6379",
                        "set",
                        "test_key",
                        "test_value",
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )

                get_result = subprocess.run(
                    ["redis-cli", "-h", "localhost", "-p", "6379", "get", "test_key"],
                    capture_output=True,
                    text=True,
                    check=True,
                )

                del_result = subprocess.run(
                    ["redis-cli", "-h", "localhost", "-p", "6379", "del", "test_key"],
                    capture_output=True,
                    text=True,
                    check=True,
                )

                # Debug logging
                logger.info(f"SET result: '{set_result.stdout.strip()}'")
                logger.info(f"GET result: '{get_result.stdout.strip()}'")
                logger.info(f"DEL result: '{del_result.stdout.strip()}'")

                if (
                    set_result.stdout.strip() == "OK"
                    and get_result.stdout.strip() == "test_value"
                    and del_result.stdout.strip() == "1"
                ):
                    self.log_test_result(
                        "redis_connectivity",
                        True,
                        "Redis connectivity and operations working via redis-cli",
                    )
                    return True
                else:
                    self.log_test_result(
                        "redis_connectivity",
                        False,
                        f"Redis operations failed - SET:'{set_result.stdout.strip()}' GET:'{get_result.stdout.strip()}' DEL:'{del_result.stdout.strip()}'",
                    )
                    return False
            else:
                self.log_test_result("redis_connectivity", False, "Redis ping failed")
                return False

        except subprocess.CalledProcessError as e:
            self.log_test_result(
                "redis_connectivity", False, f"Redis connection failed: {e.stderr}"
            )
            return False
        except Exception as e:
            self.log_test_result(
                "redis_connectivity", False, f"Redis test failed: {str(e)}"
            )
            return False

    def test_redis_exporter_metrics(self) -> bool:
        """Test that Redis Exporter is providing metrics"""
        logger.info("📊 Testing Redis Exporter metrics...")

        try:
            # Wait for exporter to start
            time.sleep(10)

            # Get metrics from Redis Exporter
            response = requests.get("http://localhost:9121/metrics", timeout=10)

            if response.status_code == 200:
                metrics = response.text

                # Check for key Redis metrics
                required_metrics = [
                    "redis_up",
                    "redis_connected_clients",
                    "redis_memory_used_bytes",
                    "redis_commands_processed_total",
                ]

                missing_metrics = []
                for metric in required_metrics:
                    if metric not in metrics:
                        missing_metrics.append(metric)

                if not missing_metrics:
                    self.log_test_result(
                        "redis_exporter_metrics",
                        True,
                        "All required Redis metrics available",
                    )
                    return True
                else:
                    self.log_test_result(
                        "redis_exporter_metrics",
                        False,
                        f"Missing metrics: {missing_metrics}",
                    )
                    return False
            else:
                self.log_test_result(
                    "redis_exporter_metrics", False, f"HTTP {response.status_code}"
                )
                return False

        except Exception as e:
            self.log_test_result(
                "redis_exporter_metrics", False, f"Redis Exporter test failed: {str(e)}"
            )
            return False

    def test_prometheus_targets(self) -> bool:
        """Test that Prometheus is scraping Redis metrics"""
        logger.info("🎯 Testing Prometheus targets...")

        try:
            # Wait for Prometheus to start
            time.sleep(15)

            # Check Prometheus targets
            response = requests.get("http://localhost:9090/api/v1/targets", timeout=10)

            if response.status_code == 200:
                targets_data = response.json()

                # Check for Redis target
                redis_target_found = False
                redis_target_up = False

                for target in targets_data.get("data", {}).get("activeTargets", []):
                    if "redis" in target.get("labels", {}).get("job", ""):
                        redis_target_found = True
                        if target.get("health") == "up":
                            redis_target_up = True
                            break

                if redis_target_found and redis_target_up:
                    self.log_test_result(
                        "prometheus_targets",
                        True,
                        "Redis target is up and being scraped",
                    )
                    return True
                elif redis_target_found:
                    self.log_test_result(
                        "prometheus_targets", False, "Redis target found but not up"
                    )
                    return False
                else:
                    self.log_test_result(
                        "prometheus_targets", False, "Redis target not found"
                    )
                    return False
            else:
                self.log_test_result(
                    "prometheus_targets",
                    False,
                    f"Prometheus API error: {response.status_code}",
                )
                return False

        except Exception as e:
            self.log_test_result(
                "prometheus_targets", False, f"Prometheus targets test failed: {str(e)}"
            )
            return False

    def test_prometheus_metrics(self) -> bool:
        """Test that Prometheus has collected Redis metrics"""
        logger.info("📈 Testing Prometheus metrics collection...")

        try:
            # Query for Redis metrics in Prometheus
            queries = [
                "redis_up",
                "redis_connected_clients",
                "redis_memory_used_bytes",
                "rate(redis_commands_processed_total[5m])",
            ]

            all_metrics_found = True
            metrics_data = {}

            for query in queries:
                response = requests.get(
                    "http://localhost:9090/api/v1/query",
                    params={"query": query},
                    timeout=10,
                )

                if response.status_code == 200:
                    result = response.json()
                    if result.get("status") == "success" and result.get("data", {}).get(
                        "result"
                    ):
                        metrics_data[query] = result["data"]["result"]
                    else:
                        all_metrics_found = False
                        logger.warning(f"Query {query} returned no data")
                else:
                    all_metrics_found = False
                    logger.error(f"Query {query} failed: {response.status_code}")

            if all_metrics_found:
                self.log_test_result(
                    "prometheus_metrics",
                    True,
                    "All Redis metrics collected successfully",
                    metrics_data,
                )
                return True
            else:
                self.log_test_result(
                    "prometheus_metrics", False, "Some Redis metrics not collected"
                )
                return False

        except Exception as e:
            self.log_test_result(
                "prometheus_metrics", False, f"Prometheus metrics test failed: {str(e)}"
            )
            return False

    def test_grafana_connectivity(self) -> bool:
        """Test Grafana connectivity and basic functionality"""
        logger.info("📊 Testing Grafana connectivity...")

        try:
            # Wait for Grafana to start
            time.sleep(20)

            # Test Grafana health endpoint
            response = requests.get("http://localhost:3000/api/health", timeout=10)

            if response.status_code == 200:
                health_data = response.json()
                if health_data.get("database") == "ok":
                    self.log_test_result(
                        "grafana_connectivity",
                        True,
                        "Grafana is healthy and accessible",
                    )
                    return True
                else:
                    self.log_test_result(
                        "grafana_connectivity",
                        False,
                        f"Grafana health check failed: {health_data}",
                    )
                    return False
            else:
                self.log_test_result(
                    "grafana_connectivity",
                    False,
                    f"Grafana health endpoint error: {response.status_code}",
                )
                return False

        except Exception as e:
            self.log_test_result(
                "grafana_connectivity",
                False,
                f"Grafana connectivity test failed: {str(e)}",
            )
            return False

    def test_grafana_datasource(self) -> bool:
        """Test that Grafana has Prometheus datasource configured"""
        logger.info("🔗 Testing Grafana datasource...")

        try:
            # Test datasource configuration (using admin/admin credentials)
            response = requests.get(
                "http://localhost:3000/api/datasources",
                auth=("admin", "admin"),
                timeout=10,
            )

            if response.status_code == 200:
                datasources = response.json()

                prometheus_ds = None
                for ds in datasources:
                    if ds.get("type") == "prometheus":
                        prometheus_ds = ds
                        break

                if (
                    prometheus_ds
                    and prometheus_ds.get("url") == "http://prometheus:9090"
                ):
                    self.log_test_result(
                        "grafana_datasource",
                        True,
                        "Prometheus datasource configured correctly",
                    )
                    return True
                else:
                    self.log_test_result(
                        "grafana_datasource",
                        False,
                        "Prometheus datasource not found or misconfigured",
                    )
                    return False
            else:
                self.log_test_result(
                    "grafana_datasource",
                    False,
                    f"Grafana API error: {response.status_code}",
                )
                return False

        except Exception as e:
            self.log_test_result(
                "grafana_datasource", False, f"Grafana datasource test failed: {str(e)}"
            )
            return False

    def test_grafana_dashboard(self) -> bool:
        """Test that Redis dashboard is available in Grafana"""
        logger.info("📋 Testing Grafana dashboard...")

        try:
            # Test dashboard availability
            response = requests.get(
                "http://localhost:3000/api/search",
                auth=("admin", "admin"),
                params={"query": "redis"},
                timeout=10,
            )

            if response.status_code == 200:
                dashboards = response.json()

                redis_dashboard = None
                for dashboard in dashboards:
                    if "redis" in dashboard.get("title", "").lower():
                        redis_dashboard = dashboard
                        break

                if redis_dashboard:
                    self.log_test_result(
                        "grafana_dashboard",
                        True,
                        f"Redis dashboard found: {redis_dashboard.get('title')}",
                    )
                    return True
                else:
                    self.log_test_result(
                        "grafana_dashboard", False, "Redis dashboard not found"
                    )
                    return False
            else:
                self.log_test_result(
                    "grafana_dashboard",
                    False,
                    f"Grafana search API error: {response.status_code}",
                )
                return False

        except Exception as e:
            self.log_test_result(
                "grafana_dashboard", False, f"Grafana dashboard test failed: {str(e)}"
            )
            return False

    def run_validation_suite(self) -> Dict[str, Any]:
        """Run the complete validation suite"""
        logger.info("🚀 Starting Redis Monitoring Stack Validation Suite")
        logger.info(f"Validation ID: {self.validation_id}")

        # Test sequence
        tests = [
            ("Docker Compose Startup", self.test_docker_compose_startup),
            ("Redis Connectivity", self.test_redis_connectivity),
            ("Redis Exporter Metrics", self.test_redis_exporter_metrics),
            ("Prometheus Targets", self.test_prometheus_targets),
            ("Prometheus Metrics", self.test_prometheus_metrics),
            ("Grafana Connectivity", self.test_grafana_connectivity),
            ("Grafana Datasource", self.test_grafana_datasource),
            ("Grafana Dashboard", self.test_grafana_dashboard),
        ]

        # Run tests
        for test_name, test_func in tests:
            try:
                test_func()
                time.sleep(5)  # Brief pause between tests
            except Exception as e:
                logger.error(f"Test {test_name} failed with exception: {str(e)}")
                self.log_test_result(test_name, False, f"Exception: {str(e)}")

        # Generate summary
        self.generate_summary()

        return self.test_results

    def generate_summary(self):
        """Generate validation summary"""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()

        # Count results
        total_tests = len(self.test_results)
        passed_tests = sum(
            1 for result in self.test_results.values() if result["success"]
        )
        failed_tests = total_tests - passed_tests

        # Create summary
        summary = {
            "validation_id": self.validation_id,
            "start_time": self.start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "failed_tests": failed_tests,
            "success_rate": (passed_tests / total_tests * 100)
            if total_tests > 0
            else 0,
            "overall_status": "PASSED" if failed_tests == 0 else "FAILED",
            "test_results": self.test_results,
        }

        # Save results
        output_file = f"redis_monitoring_validation_{self.validation_id}.json"
        with open(output_file, "w") as f:
            json.dump(summary, f, indent=2)

        # Log summary
        logger.info("=" * 80)
        logger.info("📊 VALIDATION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Validation ID: {self.validation_id}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Total Tests: {total_tests}")
        logger.info(f"Passed: {passed_tests}")
        logger.info(f"Failed: {failed_tests}")
        logger.info(f"Success Rate: {summary['success_rate']:.1f}%")
        logger.info(f"Overall Status: {summary['overall_status']}")
        logger.info("=" * 80)

        if failed_tests > 0:
            logger.error("❌ VALIDATION FAILED - Some tests did not pass")
            for test_name, result in self.test_results.items():
                if not result["success"]:
                    logger.error(f"  - {test_name}: {result['details']}")
        else:
            logger.info("✅ VALIDATION PASSED - All tests successful!")

        logger.info(f"Detailed results saved to: {output_file}")
        logger.info("=" * 80)

        return summary


def main():
    """Main validation function"""
    print("Redis Monitoring Stack Validation")
    print("=" * 50)
    print("This script validates the Prometheus + Grafana monitoring stack for Redis.")
    print(
        "It checks that all containers start correctly and metrics are being collected."
    )
    print()

    # Check if Docker is available
    try:
        subprocess.run(["docker", "--version"], check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ Error: Docker is not available. Please install Docker and try again.")
        sys.exit(1)

    # Check if Docker Compose is available
    try:
        subprocess.run(
            ["docker", "compose", "--version"], check=True, capture_output=True
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        print(
            "❌ Error: Docker Compose is not available. Please install Docker Compose and try again."
        )
        sys.exit(1)

    # Check if required files exist
    required_files = [
        "../docker-compose.monitoring.yml",
        "../prometheus.yml",
        "../redis.conf",
        "../grafana/provisioning/datasources/prometheus.yml",
        "../grafana/provisioning/dashboards/dashboard.yml",
        "../grafana/provisioning/dashboards/redis-dashboard.json",
    ]

    missing_files = []
    for file_path in required_files:
        if not Path(file_path).exists():
            missing_files.append(file_path)

    if missing_files:
        print("❌ Error: Missing required files:")
        for file_path in missing_files:
            print(f"  - {file_path}")
        sys.exit(1)

    print("✅ Prerequisites check passed")
    print()

    # Run validation
    validator = MonitoringStackValidator()
    results = validator.run_validation_suite()

    # Exit with appropriate code
    if results.get("overall_status") == "PASSED":
        print("\n🎉 Validation completed successfully!")
        sys.exit(0)
    else:
        print("\n💥 Validation failed!")
        sys.exit(1)


if __name__ == "__main__":
    from pathlib import Path

    main()
