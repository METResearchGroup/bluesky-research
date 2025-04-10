#!/usr/bin/env python3
"""
Coordinator for the scalable backfill system.

The coordinator:
1. Initializes the Redis state store
2. Reads the list of DIDs to process
3. Creates task batches and places them in the task queue
4. Monitors overall progress
5. Handles restart logic for interrupted jobs
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

import redis
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("coordinator.log"),
    ],
)
logger = logging.getLogger("coordinator")

# Redis key constants
TASK_QUEUE = "backfill:task_queue"
PROCESSING_QUEUE = "backfill:processing_queue"
COMPLETED_QUEUE = "backfill:completed_queue"
FAILED_QUEUE = "backfill:failed_queue"
JOB_CONFIG = "backfill:config"
JOB_STATUS = "backfill:status"
WORKER_HEARTBEATS = "backfill:worker_heartbeats"
METRICS = "backfill:metrics"
RATE_LIMITS = "backfill:rate_limits"


class BackfillCoordinator:
    """Coordinator for the distributed backfill process."""

    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        batch_size: int = 50,
        job_id: Optional[str] = None,
    ):
        """Initialize the coordinator.

        Args:
            redis_host: Hostname of the Redis server
            redis_port: Port of the Redis server
            batch_size: Number of DIDs to process in each batch
            job_id: Optional job ID for resuming an existing job
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.batch_size = batch_size
        self.job_id = job_id or datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Connect to Redis
        self.redis = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True,
        )
        
        logger.info(f"Coordinator initialized with job ID: {self.job_id}")
        logger.info(f"Connected to Redis at {redis_host}:{redis_port}")
        logger.info(f"Batch size: {batch_size}")

    def check_existing_job(self) -> bool:
        """Check if there's an existing job that can be resumed.

        Returns:
            True if an existing job was found and loaded, False otherwise
        """
        if not self.job_id:
            return False
            
        # Check for existing job configuration
        existing_config = self.redis.get(f"{JOB_CONFIG}:{self.job_id}")
        if not existing_config:
            logger.info(f"No existing job found with ID: {self.job_id}")
            return False
            
        try:
            config = json.loads(existing_config)
            self.batch_size = config.get("batch_size", self.batch_size)
            logger.info(f"Resuming existing job with ID: {self.job_id}")
            logger.info(f"Loaded configuration: {config}")
            return True
        except Exception as e:
            logger.error(f"Error loading existing job: {e}")
            return False

    def initialize_job(self, dids: List[str]) -> None:
        """Initialize a new job or reset an existing one.

        Args:
            dids: List of DIDs to process
        """
        # Store job configuration
        config = {
            "job_id": self.job_id,
            "batch_size": self.batch_size,
            "total_dids": len(dids),
            "start_time": datetime.now().isoformat(),
            "status": "initializing",
        }
        self.redis.set(f"{JOB_CONFIG}:{self.job_id}", json.dumps(config))
        
        # Clear any existing queues for this job
        self.redis.delete(f"{TASK_QUEUE}:{self.job_id}")
        self.redis.delete(f"{PROCESSING_QUEUE}:{self.job_id}")
        self.redis.delete(f"{COMPLETED_QUEUE}:{self.job_id}")
        self.redis.delete(f"{FAILED_QUEUE}:{self.job_id}")
        
        # Initialize metrics
        self.redis.hset(
            f"{METRICS}:{self.job_id}",
            mapping={
                "total_dids": len(dids),
                "processed_dids": 0,
                "completed_dids": 0,
                "failed_dids": 0,
                "start_time": time.time(),
                "api_calls": 0,
                "rate_limit_delays": 0,
            },
        )
        
        logger.info(f"Initialized job with {len(dids)} DIDs")

    def create_task_batches(self, dids: List[str]) -> None:
        """Create task batches and add them to the task queue.

        Args:
            dids: List of DIDs to process
        """
        # Create batches of DIDs
        total_batches = (len(dids) + self.batch_size - 1) // self.batch_size
        logger.info(f"Creating {total_batches} batches of size {self.batch_size}")
        
        for i in range(0, len(dids), self.batch_size):
            batch_dids = dids[i:i+self.batch_size]
            batch_id = f"{self.job_id}:batch:{i//self.batch_size}"
            batch_data = {
                "batch_id": batch_id,
                "dids": batch_dids,
                "created_at": datetime.now().isoformat(),
                "status": "pending",
            }
            
            # Add batch to task queue
            self.redis.rpush(
                f"{TASK_QUEUE}:{self.job_id}",
                json.dumps(batch_data)
            )
        
        # Update job status
        self.redis.hset(
            f"{JOB_STATUS}:{self.job_id}",
            mapping={
                "status": "ready",
                "total_batches": total_batches,
                "pending_batches": total_batches,
                "processing_batches": 0,
                "completed_batches": 0,
                "failed_batches": 0,
            },
        )
        
        logger.info(f"Added {total_batches} batches to task queue")

    def recover_failed_tasks(self) -> int:
        """Recover failed tasks and requeue them.

        Returns:
            Number of recovered tasks
        """
        # Get failed batches
        failed_batches = []
        while True:
            batch_json = self.redis.lpop(f"{FAILED_QUEUE}:{self.job_id}")
            if not batch_json:
                break
            failed_batches.append(batch_json)
        
        if not failed_batches:
            return 0
            
        # Requeue failed batches
        for batch_json in failed_batches:
            self.redis.rpush(f"{TASK_QUEUE}:{self.job_id}", batch_json)
            
        # Update job status
        failed_count = len(failed_batches)
        self.redis.hincrby(
            f"{JOB_STATUS}:{self.job_id}", "pending_batches", failed_count
        )
        self.redis.hincrby(
            f"{JOB_STATUS}:{self.job_id}", "failed_batches", -failed_count
        )
        
        logger.info(f"Recovered {failed_count} failed batches")
        return failed_count

    def recover_stalled_tasks(self, stall_threshold_seconds: int = 300) -> int:
        """Recover stalled tasks (those being processed but not updated).

        Args:
            stall_threshold_seconds: Time in seconds after which a task is considered stalled

        Returns:
            Number of recovered tasks
        """
        current_time = time.time()
        stalled_batches = []
        
        # Get all processing batches
        processing_batches = self.redis.lrange(
            f"{PROCESSING_QUEUE}:{self.job_id}", 0, -1
        )
        
        for batch_json in processing_batches:
            try:
                batch = json.loads(batch_json)
                last_update = self.redis.hget(
                    f"{WORKER_HEARTBEATS}:{self.job_id}", batch["batch_id"]
                )
                
                if not last_update or (current_time - float(last_update)) > stall_threshold_seconds:
                    # This batch is stalled
                    stalled_batches.append(batch_json)
                    self.redis.lrem(
                        f"{PROCESSING_QUEUE}:{self.job_id}", 0, batch_json
                    )
            except Exception as e:
                logger.error(f"Error checking stalled batch: {e}")
        
        # Requeue stalled batches
        for batch_json in stalled_batches:
            self.redis.rpush(f"{TASK_QUEUE}:{self.job_id}", batch_json)
            
        # Update job status
        stalled_count = len(stalled_batches)
        if stalled_count > 0:
            self.redis.hincrby(
                f"{JOB_STATUS}:{self.job_id}", "pending_batches", stalled_count
            )
            self.redis.hincrby(
                f"{JOB_STATUS}:{self.job_id}", "processing_batches", -stalled_count
            )
            
            logger.info(f"Recovered {stalled_count} stalled batches")
            
        return stalled_count

    def monitor_progress(self, poll_interval: int = 5) -> None:
        """Monitor job progress.

        Args:
            poll_interval: Time in seconds between progress updates
        """
        logger.info("Starting progress monitoring")
        
        try:
            while True:
                # Get job status
                status = self.redis.hgetall(f"{JOB_STATUS}:{self.job_id}")
                metrics = self.redis.hgetall(f"{METRICS}:{self.job_id}")
                
                if not status or not metrics:
                    logger.warning("No job status or metrics found")
                    time.sleep(poll_interval)
                    continue
                    
                # Check active workers
                current_time = time.time()
                active_workers = 0
                worker_heartbeats = self.redis.hgetall(f"{WORKER_HEARTBEATS}:{self.job_id}")
                
                for worker_id, last_heartbeat in worker_heartbeats.items():
                    if not worker_id.startswith("batch:") and current_time - float(last_heartbeat) < 30:
                        active_workers += 1
                
                # Calculate progress
                total_batches = int(status.get("total_batches", 0))
                completed_batches = int(status.get("completed_batches", 0))
                progress = (completed_batches / total_batches) * 100 if total_batches > 0 else 0
                
                # Calculate throughput
                processed_dids = int(metrics.get("processed_dids", 0))
                start_time = float(metrics.get("start_time", time.time()))
                elapsed_seconds = max(1, time.time() - start_time)
                throughput = processed_dids / (elapsed_seconds / 60)  # DIDs per minute
                
                # Calculate estimated time remaining
                total_dids = int(metrics.get("total_dids", 0))
                remaining_dids = total_dids - processed_dids
                est_seconds_remaining = (remaining_dids / throughput) * 60 if throughput > 0 else 0
                
                # Display progress
                logger.info(
                    f"Progress: {progress:.1f}% | "
                    f"Batches: {completed_batches}/{total_batches} | "
                    f"DIDs: {processed_dids}/{total_dids} | "
                    f"Active Workers: {active_workers} | "
                    f"Throughput: {throughput:.1f} DIDs/min | "
                    f"ETA: {est_seconds_remaining//3600:.0f}h {(est_seconds_remaining%3600)//60:.0f}m"
                )
                
                # Check for completion
                if completed_batches >= total_batches and total_batches > 0:
                    logger.info("All batches completed!")
                    
                    # Update final status
                    self.redis.hset(
                        f"{JOB_CONFIG}:{self.job_id}",
                        "status", "completed"
                    )
                    self.redis.hset(
                        f"{JOB_CONFIG}:{self.job_id}",
                        "end_time", datetime.now().isoformat()
                    )
                    
                    # Final stats
                    logger.info(f"Final stats: {metrics}")
                    return
                    
                # Recover failed/stalled tasks periodically
                if int(time.time()) % 60 < poll_interval:
                    recovered_failed = self.recover_failed_tasks()
                    recovered_stalled = self.recover_stalled_tasks()
                    if recovered_failed or recovered_stalled:
                        logger.info(
                            f"Recovery: {recovered_failed} failed, {recovered_stalled} stalled tasks"
                        )
                
                time.sleep(poll_interval)
                
        except KeyboardInterrupt:
            logger.info("Monitoring interrupted")
        except Exception as e:
            logger.error(f"Error in monitoring: {e}")
    
    def run(self, dids: List[str]) -> None:
        """Run the coordinator.

        Args:
            dids: List of DIDs to process
        """
        # Check for existing job
        existing_job = self.check_existing_job()
        
        if not existing_job:
            # Initialize new job
            self.initialize_job(dids)
            self.create_task_batches(dids)
        else:
            # Recover any failed/stalled tasks
            self.recover_failed_tasks()
            self.recover_stalled_tasks()
        
        # Monitor progress
        self.monitor_progress()


def read_dids_from_file(filename: str) -> List[str]:
    """Read DIDs from a file.

    Args:
        filename: Name of the file containing DIDs (one per line)

    Returns:
        List of DIDs
    """
    with open(filename, "r") as f:
        dids = [line.strip() for line in f if line.strip()]
    return dids


def main() -> None:
    """Main function."""
    parser = argparse.ArgumentParser(description="Backfill Coordinator")
    parser.add_argument(
        "--input-file", 
        type=str, 
        required=True,
        help="File containing DIDs to process (one per line)"
    )
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=50,
        help="Number of DIDs to process in each batch"
    )
    parser.add_argument(
        "--redis-host", 
        type=str, 
        default="localhost",
        help="Redis server hostname"
    )
    parser.add_argument(
        "--redis-port", 
        type=int, 
        default=6379,
        help="Redis server port"
    )
    parser.add_argument(
        "--job-id", 
        type=str, 
        default=None,
        help="Job ID for resuming an existing job"
    )
    
    args = parser.parse_args()
    
    # Read DIDs from file
    try:
        dids = read_dids_from_file(args.input_file)
        logger.info(f"Read {len(dids)} DIDs from {args.input_file}")
    except Exception as e:
        logger.error(f"Error reading DIDs from file: {e}")
        sys.exit(1)
    
    # Initialize and run coordinator
    coordinator = BackfillCoordinator(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        batch_size=args.batch_size,
        job_id=args.job_id,
    )
    
    coordinator.run(dids)


if __name__ == "__main__":
    main()