#!/usr/bin/env python3
"""
Test runner for the scalable backfill system.

This script:
1. Generates test data
2. Starts a Redis server if needed
3. Launches the coordinator
4. Spawns multiple workers
5. Starts the monitor
6. Collects and reports metrics
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
import multiprocessing

# Check if Redis server is running
def is_redis_running() -> bool:
    """Check if Redis server is running.

    Returns:
        True if Redis server is running, False otherwise
    """
    try:
        import redis
        client = redis.Redis(host="localhost", port=6379)
        client.ping()
        return True
    except:
        return False


def start_redis_server() -> subprocess.Popen:
    """Start a Redis server.

    Returns:
        Subprocess object for the Redis server
    """
    print("Starting Redis server...")
    return subprocess.Popen(
        ["redis-server", "--port", "6379"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def start_coordinator(input_file: str, batch_size: int, job_id: str) -> subprocess.Popen:
    """Start the coordinator.

    Args:
        input_file: Path to the input file containing DIDs
        batch_size: Batch size for processing
        job_id: Job ID for this run

    Returns:
        Subprocess object for the coordinator
    """
    print(f"Starting coordinator with job ID {job_id}...")
    return subprocess.Popen(
        [
            sys.executable,
            "demos/scalable_backfill/coordinator.py",
            "--input-file", input_file,
            "--batch-size", str(batch_size),
            "--job-id", job_id,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def start_worker(worker_id: str, job_id: str, simulated: bool = False) -> subprocess.Popen:
    """Start a worker.

    Args:
        worker_id: Worker ID
        job_id: Job ID
        simulated: Whether to use simulated mode

    Returns:
        Subprocess object for the worker
    """
    env = os.environ.copy()
    if simulated:
        # Set environment variable to indicate simulated mode
        # This will be used by the backfill code to simulate API calls
        env["BACKFILL_SIMULATED"] = "1"
        
    print(f"Starting worker {worker_id}...")
    return subprocess.Popen(
        [
            sys.executable,
            "demos/scalable_backfill/worker.py",
            "--worker-id", worker_id,
            "--job-id", job_id,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )


def start_monitor(job_id: str) -> subprocess.Popen:
    """Start the monitor.

    Args:
        job_id: Job ID

    Returns:
        Subprocess object for the monitor
    """
    print("Starting monitor...")
    return subprocess.Popen(
        [
            sys.executable,
            "demos/scalable_backfill/monitor.py",
            "--job-id", job_id,
        ],
    )


def patch_for_simulation() -> None:
    """Patch the backfill code for simulation.
    
    This creates a simple mock version of the backfill logic that doesn't
    actually make API calls but generates realistic-looking data.
    """
    os.makedirs("demos/scalable_backfill/patches", exist_ok=True)
    
    # Create a patch file for the do_backfill_for_user function
    patch_file = "demos/scalable_backfill/patches/backfill_mock.py"
    
    with open(patch_file, "w") as f:
        f.write('''
import os
import json
import random
import time
from typing import Dict, List, Tuple, Any
from services.backfill.sync.models import UserBackfillMetadata
from services.backfill.sync.backfill import create_user_backfill_metadata

def mock_do_backfill_for_user(did: str, start_timestamp: str, end_timestamp: str) -> Tuple[Dict[str, int], Dict[str, List[Dict]], UserBackfillMetadata]:
    """Mock implementation of do_backfill_for_user for simulation."""
    # Simulate API call delay (100-300ms)
    time.sleep(random.uniform(0.1, 0.3))
    
    # Generate random record counts
    record_types = ["post", "reply", "like", "follow", "repost", "block"]
    count_map = {}
    record_map = {}
    
    for record_type in record_types:
        # Generate a random count (more posts than other types)
        count = random.randint(5, 50) if record_type == "post" else random.randint(0, 20)
        if count > 0:
            count_map[record_type] = count
            
            # Generate mock records
            records = []
            for i in range(count):
                record = {
                    "$type": f"app.bsky.feed.{record_type}",
                    "createdAt": f"2023-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}T{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}Z",
                    "did": did,
                    "record_type": record_type,
                    "synctimestamp": start_timestamp,  # Just use the start timestamp for simplicity
                }
                records.append(record)
            
            record_map[record_type] = records
    
    # Create metadata
    metadata = create_user_backfill_metadata(
        did=did,
        record_count_map=count_map,
        bluesky_handle=f"{did.split(':')[-1]}@bsky.social",
        pds_service_endpoint="https://mock-pds.bsky.social",
    )
    
    return count_map, record_map, metadata

# Monkey patch the real function if in simulation mode
if os.environ.get("BACKFILL_SIMULATED") == "1":
    import sys
    from services.backfill.sync import backfill
    
    # Replace the real function with our mock
    print("Monkey patching backfill.do_backfill_for_user with mock implementation")
    backfill.do_backfill_for_user = mock_do_backfill_for_user
''')
    
    # Add the patch to Python's import path
    sys.path.insert(0, os.path.abspath("demos/scalable_backfill/patches"))
    
    # Import the patch to apply it
    import backfill_mock
    print("Applied simulation patch")


def main() -> None:
    """Main function."""
    parser = argparse.ArgumentParser(description="Run the scalable backfill system")
    parser.add_argument(
        "--did-count",
        type=int,
        default=3000,
        help="Number of DIDs to generate for testing",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=multiprocessing.cpu_count(),
        help="Number of workers to spawn",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Batch size for processing",
    )
    parser.add_argument(
        "--simulated",
        action="store_true",
        help="Use simulated mode (no actual API calls)",
    )
    parser.add_argument(
        "--input-file",
        type=str,
        default=None,
        help="Optional input file containing DIDs (if not specified, will generate DIDs)",
    )
    
    args = parser.parse_args()
    
    # Generate a unique job ID based on timestamp
    job_id = f"test-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Check if Redis is running
    redis_process = None
    if not is_redis_running():
        redis_process = start_redis_server()
        # Wait for Redis to start
        time.sleep(2)
    
    try:
        # Apply simulation patch if requested
        if args.simulated:
            patch_for_simulation()
        
        # Generate DIDs if no input file is specified
        input_file = args.input_file
        if not input_file:
            print(f"Generating {args.did_count} DIDs for testing...")
            input_file = f"test_dids_{job_id}.txt"
            subprocess.run(
                [
                    sys.executable,
                    "demos/scalable_backfill/generate_test_data.py",
                    "--count", str(args.did_count),
                    "--output", input_file,
                ],
                check=True,
            )
        
        # Start the coordinator
        coordinator = start_coordinator(input_file, args.batch_size, job_id)
        
        # Wait for the coordinator to initialize
        time.sleep(5)
        
        # Start workers
        workers = []
        for i in range(args.workers):
            worker = start_worker(f"worker-{i+1}", job_id, args.simulated)
            workers.append(worker)
        
        # Start the monitor in the foreground
        monitor = start_monitor(job_id)
        
        # Wait for the monitor to exit (when the user presses Ctrl+C)
        monitor.wait()
        
    except KeyboardInterrupt:
        print("\nStopping all processes...")
    finally:
        # Terminate all processes
        processes = []
        if 'coordinator' in locals():
            processes.append(coordinator)
        if 'workers' in locals():
            processes.extend(workers)
        if 'monitor' in locals():
            processes.append(monitor)
        
        for process in processes:
            if process.poll() is None:  # If process is still running
                process.terminate()
        
        # Stop Redis if we started it
        if redis_process and redis_process.poll() is None:
            redis_process.terminate()
        
        print("All processes stopped")


if __name__ == "__main__":
    main() 