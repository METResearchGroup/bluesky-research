#!/usr/bin/env python3
"""
Test Flow for Prefect Infrastructure Validation

This is a simple test flow to validate that the Prefect infrastructure
is working correctly with the SQLite backend and agent.

Author: AI Assistant
Date: 2025-08-08
"""

from prefect import flow, task
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task
def test_task_1():
    """Simple test task that returns a message"""
    logger.info("Executing test task 1")
    time.sleep(1)  # Simulate some work
    return "Task 1 completed successfully"


@task
def test_task_2(message: str):
    """Test task that depends on task 1"""
    logger.info(f"Executing test task 2 with message: {message}")
    time.sleep(1)  # Simulate some work
    return f"Task 2 completed with: {message}"


@task
def test_task_3():
    """Another independent test task"""
    logger.info("Executing test task 3")
    time.sleep(1)  # Simulate some work
    return "Task 3 completed successfully"


@flow(name="test-infrastructure-flow")
def test_infrastructure_flow():
    """Test flow to validate Prefect infrastructure"""
    logger.info("Starting test infrastructure flow")

    # Execute tasks
    result1 = test_task_1()
    result2 = test_task_2(result1)
    result3 = test_task_3()

    # Combine results
    final_result = f"Flow completed: {result2} and {result3}"
    logger.info(final_result)

    return final_result


if __name__ == "__main__":
    # Run the flow directly for testing
    result = test_infrastructure_flow()
    print(f"Flow result: {result}")
