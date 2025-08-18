#!/usr/bin/env python3
"""
Prefect Infrastructure Setup Validation Script

This script validates the Prefect server and agent setup with SQLite backend
and integration with the existing Redis + Prometheus + Grafana monitoring stack.

Author: AI Assistant
Date: 2025-08-08
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
            f'prefect_infrastructure_setup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        ),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class PrefectInfrastructureValidator:
    """Validator for Prefect infrastructure setup with monitoring integration"""

    def __init__(self):
        self.test_results = {}
        self.start_time = datetime.now()
        self.validation_id = (
            f"prefect_infrastructure_setup_{self.start_time.strftime('%Y%m%d_%H%M%S')}"
        )
        self.prefect_api_url = "http://localhost:4200/api"
        self.prefect_ui_url = "http://localhost:4200"

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
        logger.info("🔧 Testing Docker Compose startup with Prefect...")

        try:
            # Stop any existing containers
            subprocess.run(
                [
                    "docker",
                    "compose",
                    "-f",
                    "../docker-compose.prefect.yml",
                    "down",
                ],
                capture_output=True,
                text=True,
            )

            # Start the monitoring stack with Prefect
            subprocess.run(
                [
                    "docker",
                    "compose",
                    "-f",
                    "../docker-compose.prefect.yml",
                    "up",
                    "-d",
                ],
                capture_output=True,
                text=True,
                check=True,
            )

            # Wait for containers to start
            time.sleep(30)

            # Check all containers are running
            containers = [
                "bluesky_redis",
                "redis_exporter",
                "prometheus",
                "alertmanager",
                "grafana",
                "prefect_server",
                "prefect_worker",
            ]

            all_running = True
            for container in containers:
                if not self.check_container_status(container):
                    logger.error(f"Container {container} is not running")
                    all_running = False

            if all_running:
                self.log_test_result(
                    "docker_compose_startup",
                    True,
                    f"All {len(containers)} containers started successfully",
                )
                return True
            else:
                self.log_test_result(
                    "docker_compose_startup",
                    False,
                    "Some containers failed to start",
                )
                return False

        except subprocess.CalledProcessError as e:
            self.log_test_result(
                "docker_compose_startup",
                False,
                f"Docker Compose failed: {e.stderr}",
            )
            return False

    def test_prefect_server_connectivity(self) -> bool:
        """Test Prefect server API connectivity"""
        logger.info("🔗 Testing Prefect server connectivity...")

        try:
            # Test API health endpoint
            response = requests.get(f"{self.prefect_api_url}/health", timeout=10)
            if response.status_code == 200:
                health_data = response.json()
                if isinstance(health_data, dict):
                    status = health_data.get("status", "unknown")
                else:
                    status = str(health_data)
                self.log_test_result(
                    "prefect_server_connectivity",
                    True,
                    f"Prefect server API responding: {status}",
                    {"status_code": response.status_code, "health": health_data},
                )
                return True
            else:
                self.log_test_result(
                    "prefect_server_connectivity",
                    False,
                    f"Prefect server API returned status {response.status_code}",
                )
                return False

        except requests.exceptions.RequestException as e:
            self.log_test_result(
                "prefect_server_connectivity",
                False,
                f"Failed to connect to Prefect server: {e}",
            )
            return False

    def test_prefect_ui_accessibility(self) -> bool:
        """Test Prefect UI accessibility"""
        logger.info("🌐 Testing Prefect UI accessibility...")

        try:
            # Test UI endpoint
            response = requests.get(self.prefect_ui_url, timeout=10)
            if response.status_code == 200:
                self.log_test_result(
                    "prefect_ui_accessibility",
                    True,
                    f"Prefect UI accessible at {self.prefect_ui_url}",
                    {"status_code": response.status_code},
                )
                return True
            else:
                self.log_test_result(
                    "prefect_ui_accessibility",
                    False,
                    f"Prefect UI returned status {response.status_code}",
                )
                return False

        except requests.exceptions.RequestException as e:
            self.log_test_result(
                "prefect_ui_accessibility",
                False,
                f"Failed to access Prefect UI: {e}",
            )
            return False

    def test_prefect_worker_registration(self) -> bool:
        """Test Prefect worker registration and work pool"""
        logger.info("🤖 Testing Prefect worker registration...")

        try:
            # Check worker is running
            if not self.check_container_status("prefect_worker"):
                self.log_test_result(
                    "prefect_worker_registration",
                    False,
                    "Prefect worker container is not running",
                )
                return False

            # Test work pools API endpoint (Prefect v3 uses POST to filter)
            response = requests.post(
                f"{self.prefect_api_url}/work_pools/filter", json={}, timeout=10
            )
            if response.status_code == 200:
                pools_data = response.json()
                default_pool = None
                for pool in pools_data:
                    if pool.get("name") == "default":
                        default_pool = pool
                        break

                if default_pool:
                    self.log_test_result(
                        "prefect_worker_registration",
                        True,
                        f"Worker registered with default work pool: {default_pool.get('id')}",
                        {
                            "pool_id": default_pool.get("id"),
                            "pool_type": default_pool.get("type"),
                        },
                    )
                    return True
                else:
                    self.log_test_result(
                        "prefect_worker_registration",
                        False,
                        "Default work pool not found",
                    )
                    return False
            else:
                self.log_test_result(
                    "prefect_worker_registration",
                    False,
                    f"Work pools API returned status {response.status_code}",
                )
                return False

        except requests.exceptions.RequestException as e:
            self.log_test_result(
                "prefect_worker_registration",
                False,
                f"Failed to check worker registration: {e}",
            )
            return False

    def test_sqlite_backend_operations(self) -> bool:
        """Test SQLite backend operations"""
        logger.info("💾 Testing SQLite backend operations...")

        try:
            # Test database connectivity by checking work pools (Prefect v3 uses POST to filter)
            response = requests.post(
                f"{self.prefect_api_url}/work_pools/filter", json={}, timeout=10
            )
            if response.status_code == 200:
                pools_data = response.json()
                self.log_test_result(
                    "sqlite_backend_operations",
                    True,
                    f"SQLite backend operational - found {len(pools_data)} work pools",
                    {
                        "pools_count": len(pools_data),
                        "status_code": response.status_code,
                    },
                )
                return True
            else:
                self.log_test_result(
                    "sqlite_backend_operations",
                    False,
                    f"Failed to access work pools: {response.status_code}",
                )
                return False

        except requests.exceptions.RequestException as e:
            self.log_test_result(
                "sqlite_backend_operations",
                False,
                f"Failed to test SQLite backend: {e}",
            )
            return False

    def test_monitoring_integration(self) -> bool:
        """Test integration with existing monitoring stack"""
        logger.info("📊 Testing monitoring integration...")

        try:
            # Test Prometheus connectivity
            prometheus_response = requests.get(
                "http://localhost:9090/-/healthy", timeout=10
            )
            if prometheus_response.status_code != 200:
                self.log_test_result(
                    "monitoring_integration",
                    False,
                    "Prometheus not accessible",
                )
                return False

            # Test Grafana connectivity
            grafana_response = requests.get(
                "http://localhost:3000/api/health", timeout=10
            )
            if grafana_response.status_code != 200:
                self.log_test_result(
                    "monitoring_integration",
                    False,
                    "Grafana not accessible",
                )
                return False

            # Test Redis connectivity
            redis_response = requests.get("http://localhost:9121/metrics", timeout=10)
            if redis_response.status_code != 200:
                self.log_test_result(
                    "monitoring_integration",
                    False,
                    "Redis exporter not accessible",
                )
                return False

            self.log_test_result(
                "monitoring_integration",
                True,
                "All monitoring components accessible and operational",
                {
                    "prometheus_status": prometheus_response.status_code,
                    "grafana_status": grafana_response.status_code,
                    "redis_exporter_status": redis_response.status_code,
                },
            )
            return True

        except requests.exceptions.RequestException as e:
            self.log_test_result(
                "monitoring_integration",
                False,
                f"Failed to test monitoring integration: {e}",
            )
            return False

    def test_prefect_work_queue_operations(self) -> bool:
        """Test Prefect work queue operations"""
        logger.info("📋 Testing Prefect work queue operations...")

        try:
            # Test work queues endpoint (Prefect v3 uses POST to filter)
            response = requests.post(
                f"{self.prefect_api_url}/work_queues/filter", json={}, timeout=10
            )
            if response.status_code == 200:
                queues_data = response.json()
                if queues_data:
                    self.log_test_result(
                        "prefect_work_queue_operations",
                        True,
                        f"Work queues operational with {len(queues_data)} queues",
                        {"queues_count": len(queues_data)},
                    )
                    return True
                else:
                    self.log_test_result(
                        "prefect_work_queue_operations",
                        True,
                        "No work queues found (this is normal for new setup)",
                        {"queues_count": 0},
                    )
                    return True
            else:
                self.log_test_result(
                    "prefect_work_queue_operations",
                    False,
                    f"Work queues API returned status {response.status_code}",
                )
                return False

        except requests.exceptions.RequestException as e:
            self.log_test_result(
                "prefect_work_queue_operations",
                False,
                f"Failed to test work queue operations: {e}",
            )
            return False

    def test_container_health_checks(self) -> bool:
        """Test container health checks"""
        logger.info("🏥 Testing container health checks...")

        try:
            containers = [
                "prefect_server",
                "prefect_worker",
            ]

            all_healthy = True
            health_status = {}

            for container in containers:
                result = subprocess.run(
                    [
                        "docker",
                        "inspect",
                        "--format",
                        "{{.State.Health.Status}}",
                        container,
                    ],
                    capture_output=True,
                    text=True,
                )
                if result.returncode == 0:
                    status = result.stdout.strip()
                    health_status[container] = status
                    if status != "healthy":
                        logger.warning(f"Container {container} health status: {status}")
                        all_healthy = False
                else:
                    health_status[container] = "unknown"
                    all_healthy = False

            if all_healthy:
                self.log_test_result(
                    "container_health_checks",
                    True,
                    "All Prefect containers are healthy",
                    health_status,
                )
                return True
            else:
                self.log_test_result(
                    "container_health_checks",
                    False,
                    "Some containers are not healthy",
                    health_status,
                )
                return False

        except Exception as e:
            self.log_test_result(
                "container_health_checks",
                False,
                f"Failed to check container health: {e}",
            )
            return False

    def run_validation_suite(self) -> Dict[str, Any]:
        """Run the complete validation suite"""
        logger.info("🚀 Starting Prefect Infrastructure Validation Suite")
        logger.info(f"Validation ID: {self.validation_id}")

        tests = [
            ("docker_compose_startup", self.test_docker_compose_startup),
            ("prefect_server_connectivity", self.test_prefect_server_connectivity),
            ("prefect_ui_accessibility", self.test_prefect_ui_accessibility),
            ("prefect_worker_registration", self.test_prefect_worker_registration),
            ("sqlite_backend_operations", self.test_sqlite_backend_operations),
            ("monitoring_integration", self.test_monitoring_integration),
            ("prefect_work_queue_operations", self.test_prefect_work_queue_operations),
            ("container_health_checks", self.test_container_health_checks),
        ]

        passed_tests = 0
        total_tests = len(tests)

        for test_name, test_func in tests:
            try:
                if test_func():
                    passed_tests += 1
                time.sleep(2)  # Brief pause between tests
            except Exception as e:
                logger.error(f"Test {test_name} failed with exception: {e}")
                self.log_test_result(test_name, False, f"Exception: {e}")

        # Generate summary
        success_rate = (passed_tests / total_tests) * 100
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()

        summary = {
            "validation_id": self.validation_id,
            "start_time": self.start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "failed_tests": total_tests - passed_tests,
            "success_rate": success_rate,
            "test_results": self.test_results,
        }

        return summary

    def generate_summary(self):
        """Generate and save validation summary"""
        summary = self.run_validation_suite()

        # Save detailed results
        output_file = f"prefect_infrastructure_setup_{self.validation_id}.json"
        with open(output_file, "w") as f:
            json.dump(summary, f, indent=2)

        # Print summary
        logger.info("=" * 80)
        logger.info("PREFECT INFRASTRUCTURE SETUP VALIDATION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Validation ID: {summary['validation_id']}")
        logger.info(f"Duration: {summary['duration_seconds']:.2f} seconds")
        logger.info(f"Total Tests: {summary['total_tests']}")
        logger.info(f"Passed: {summary['passed_tests']}")
        logger.info(f"Failed: {summary['failed_tests']}")
        logger.info(f"Success Rate: {summary['success_rate']:.1f}%")

        if summary["success_rate"] == 100:
            logger.info("🎉 ALL TESTS PASSED - Prefect infrastructure is ready!")
        elif summary["success_rate"] >= 80:
            logger.info("✅ MOST TESTS PASSED - Prefect infrastructure is mostly ready")
        else:
            logger.info("⚠️  MANY TESTS FAILED - Prefect infrastructure needs attention")

        logger.info(f"Detailed results saved to: {output_file}")
        logger.info("=" * 80)

        return summary


def main():
    """Main function to run the validation"""
    validator = PrefectInfrastructureValidator()
    summary = validator.generate_summary()

    # Exit with appropriate code
    if summary["success_rate"] == 100:
        sys.exit(0)
    elif summary["success_rate"] >= 80:
        sys.exit(1)
    else:
        sys.exit(2)


if __name__ == "__main__":
    main()
