#!/usr/bin/env python3
"""
Worker for the scalable backfill system.

The worker:
1. Connects to Redis and pulls tasks from the queue
2. Processes batches of DIDs by calling the backfill logic
3. Reports progress and updates metrics
4. Handles rate limiting and retries
5. Sends heartbeats to the coordinator
"""

import argparse
import json
import logging
import os
import socket
import sys
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import redis
import psutil
from tqdm import tqdm

# Import existing backfill code (adjust path as needed)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from services.backfill.sync.backfill import (
    do_backfill_for_user, 
    create_user_backfill_metadata,
)
from services.backfill.sync.export_data import write_records_to_cache
from services.backfill.sync.constants import (
    default_start_timestamp,
    default_end_timestamp,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("worker.log"),
    ],
)
logger = logging.getLogger("worker")

# Redis key constants (matching coordinator.py)
TASK_QUEUE = "backfill:task_queue"
PROCESSING_QUEUE = "backfill:processing_queue"
COMPLETED_QUEUE = "backfill:completed_queue"
FAILED_QUEUE = "backfill:failed_queue"
JOB_CONFIG = "backfill:config"
JOB_STATUS = "backfill:status"
WORKER_HEARTBEATS = "backfill:worker_heartbeats"
METRICS = "backfill:metrics"
RATE_LIMITS = "backfill:rate_limits"


class BackfillWorker:
    """Worker for processing backfill tasks."""

    def __init__(
        self,
        worker_id: str,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        job_id: Optional[str] = None,
        heartbeat_interval: int = 5,
        start_timestamp: Optional[str] = None,
        end_timestamp: Optional[str] = None,
    ):
        """Initialize the worker.

        Args:
            worker_id: Unique identifier for this worker
            redis_host: Hostname of the Redis server
            redis_port: Port of the Redis server
            job_id: Job ID to work on (if None, will use the latest job)
            heartbeat_interval: Time in seconds between heartbeats
            start_timestamp: Start timestamp for backfill (format: YYYY-MM-DD-HH:MM:SS)
            end_timestamp: End timestamp for backfill (format: YYYY-MM-DD-HH:MM:SS)
        """
        self.worker_id = worker_id
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.job_id = job_id
        self.heartbeat_interval = heartbeat_interval
        self.start_timestamp = start_timestamp or default_start_timestamp
        self.end_timestamp = end_timestamp or default_end_timestamp
        self.hostname = socket.gethostname()
        
        # Connect to Redis
        self.redis = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True,
        )
        
        # Get job ID if not provided
        if not self.job_id:
            self._find_latest_job()
        
        logger.info(f"Worker {worker_id} initialized for job {self.job_id}")
        logger.info(f"Connected to Redis at {redis_host}:{redis_port}")
        logger.info(f"Backfill range: {self.start_timestamp} to {self.end_timestamp}")
        
        # Register worker
        self._register_worker()
    
    def _find_latest_job(self) -> None:
        """Find the latest job in Redis."""
        # Get all job configs
        job_keys = self.redis.keys(f"{JOB_CONFIG}:*")
        
        if not job_keys:
            logger.error("No jobs found in Redis")
            sys.exit(1)
        
        # Get the latest job by creation time
        latest_job = None
        latest_time = None
        
        for job_key in job_keys:
            job_config = json.loads(self.redis.get(job_key))
            start_time = job_config.get("start_time")
            
            if start_time and (not latest_time or start_time > latest_time):
                latest_time = start_time
                latest_job = job_config.get("job_id")
        
        if not latest_job:
            logger.error("Could not determine latest job")
            sys.exit(1)
            
        self.job_id = latest_job
        logger.info(f"Found latest job: {self.job_id}")
    
    def _register_worker(self) -> None:
        """Register the worker with the coordinator."""
        worker_info = {
            "worker_id": self.worker_id,
            "hostname": self.hostname,
            "start_time": datetime.now().isoformat(),
            "pid": os.getpid(),
        }
        
        self.redis.set(
            f"backfill:worker:{self.job_id}:{self.worker_id}",
            json.dumps(worker_info)
        )
        
        # Add initial heartbeat
        self._send_heartbeat()
    
    def _send_heartbeat(self) -> None:
        """Send a heartbeat to the coordinator."""
        self.redis.hset(
            f"{WORKER_HEARTBEATS}:{self.job_id}",
            self.worker_id,
            time.time()
        )
    
    def _update_metrics(self, metrics_update: Dict[str, Any]) -> None:
        """Update job metrics in Redis.

        Args:
            metrics_update: Dictionary of metrics to update
        """
        for key, value in metrics_update.items():
            if isinstance(value, int):
                self.redis.hincrby(f"{METRICS}:{self.job_id}", key, value)
            else:
                self.redis.hset(f"{METRICS}:{self.job_id}", key, value)
    
    def _get_batch(self) -> Optional[Dict]:
        """Get a batch from the task queue.

        Returns:
            Batch data or None if no batches are available
        """
        # Use a Redis transaction to atomically move a batch from the task queue
        # to the processing queue
        pipeline = self.redis.pipeline()
        pipeline.lpop(f"{TASK_QUEUE}:{self.job_id}")
        result = pipeline.execute()
        
        batch_json = result[0]
        if not batch_json:
            return None
            
        # Parse the batch
        try:
            batch = json.loads(batch_json)
            
            # Add the batch to the processing queue
            self.redis.rpush(
                f"{PROCESSING_QUEUE}:{self.job_id}",
                batch_json
            )
            
            # Update job status
            self.redis.hincrby(
                f"{JOB_STATUS}:{self.job_id}", "pending_batches", -1
            )
            self.redis.hincrby(
                f"{JOB_STATUS}:{self.job_id}", "processing_batches", 1
            )
            
            # Record batch assignment
            self.redis.hset(
                f"{WORKER_HEARTBEATS}:{self.job_id}",
                f"batch:{batch['batch_id']}",
                time.time()
            )
            
            return batch
            
        except Exception as e:
            logger.error(f"Error parsing batch: {e}")
            return None
    
    def _complete_batch(self, batch: Dict, success: bool = True) -> None:
        """Mark a batch as completed or failed.

        Args:
            batch: Batch data
            success: Whether the batch was processed successfully
        """
        batch_json = json.dumps(batch)
        
        # Remove batch from processing queue
        self.redis.lrem(f"{PROCESSING_QUEUE}:{self.job_id}", 0, batch_json)
        
        if success:
            # Add batch to completed queue
            self.redis.rpush(f"{COMPLETED_QUEUE}:{self.job_id}", batch_json)
            
            # Update job status
            self.redis.hincrby(
                f"{JOB_STATUS}:{self.job_id}", "processing_batches", -1
            )
            self.redis.hincrby(
                f"{JOB_STATUS}:{self.job_id}", "completed_batches", 1
            )
        else:
            # Add batch to failed queue
            self.redis.rpush(f"{FAILED_QUEUE}:{self.job_id}", batch_json)
            
            # Update job status
            self.redis.hincrby(
                f"{JOB_STATUS}:{self.job_id}", "processing_batches", -1
            )
            self.redis.hincrby(
                f"{JOB_STATUS}:{self.job_id}", "failed_batches", 1
            )
    
    def _check_rate_limit(self, endpoint: Optional[str] = None) -> bool:
        """Check if we're within rate limits.

        Args:
            endpoint: Optional specific PDS endpoint to check

        Returns:
            True if we can proceed, False if we should wait
        """
        now = time.time()
        window_key = f"{RATE_LIMITS}:{self.job_id}:window"
        count_key = f"{RATE_LIMITS}:{self.job_id}:count"
        
        # Get current window and count
        window_start = float(self.redis.get(window_key) or now)
        current_count = int(self.redis.get(count_key) or 0)
        
        # Check if we need to reset the window (5 minute window)
        if now - window_start > 300:
            # Reset window
            pipeline = self.redis.pipeline()
            pipeline.set(window_key, now)
            pipeline.set(count_key, 1)
            pipeline.execute()
            return True
        
        # Check if we're within limits (3000 requests per 5 minutes)
        if current_count < 3000:
            # Increment count
            self.redis.incr(count_key)
            return True
        
        return False
    
    def _wait_for_rate_limit(self, endpoint: Optional[str] = None) -> None:
        """Wait until we're within rate limits.

        Args:
            endpoint: Optional specific PDS endpoint to check
        """
        while not self._check_rate_limit(endpoint):
            logger.info("Rate limit reached, waiting...")
            self._update_metrics({"rate_limit_delays": 1})
            time.sleep(1)
    
    def process_batch(self, batch: Dict) -> bool:
        """Process a batch of DIDs.

        Args:
            batch: Batch data containing DIDs to process

        Returns:
            True if the batch was processed successfully, False otherwise
        """
        batch_id = batch["batch_id"]
        dids = batch["dids"]
        
        logger.info(f"Processing batch {batch_id} with {len(dids)} DIDs")
        
        # Initialize results tracking
        successful_dids = 0
        failed_dids = 0
        type_to_record_full_map = {}
        all_user_backfill_metadata = []
        
        # Process each DID
        for did in tqdm(dids, desc=f"Batch {batch_id}"):
            try:
                # Check rate limit before each API call
                self._wait_for_rate_limit()
                
                # Update heartbeat for this batch
                self.redis.hset(
                    f"{WORKER_HEARTBEATS}:{self.job_id}",
                    f"batch:{batch_id}",
                    time.time()
                )
                
                # Process the DID using the existing backfill code
                type_to_count_map, type_to_record_map, user_metadata = do_backfill_for_user(
                    did=did,
                    start_timestamp=self.start_timestamp,
                    end_timestamp=self.end_timestamp,
                )
                
                # Merge records with the full map
                for record_type, records in type_to_record_map.items():
                    if record_type in type_to_record_full_map:
                        type_to_record_full_map[record_type].extend(records)
                    else:
                        type_to_record_full_map[record_type] = records
                
                # Store user metadata
                all_user_backfill_metadata.append(user_metadata)
                
                # Update success count
                successful_dids += 1
                self._update_metrics({"processed_dids": 1, "completed_dids": 1})
                
                # Record API call
                self._update_metrics({"api_calls": 1})
                
            except Exception as e:
                logger.error(f"Error processing DID {did}: {e}")
                failed_dids += 1
                self._update_metrics({"processed_dids": 1, "failed_dids": 1})
        
        # Export records to cache
        if type_to_record_full_map:
            try:
                write_records_to_cache(
                    type_to_record_maps=type_to_record_full_map,
                    batch_size=100,
                )
                logger.info(f"Exported records to cache for batch {batch_id}")
            except Exception as e:
                logger.error(f"Error exporting records: {e}")
                return False
        
        # Log summary
        logger.info(
            f"Completed batch {batch_id}: {successful_dids} successful, "
            f"{failed_dids} failed DIDs"
        )
        
        # Update batch in case it needs to be retried
        batch["processed_at"] = datetime.now().isoformat()
        batch["successful_dids"] = successful_dids
        batch["failed_dids"] = failed_dids
        
        # Return success if all DIDs were processed successfully
        return failed_dids == 0
    
    def run(self, max_batches: Optional[int] = None) -> None:
        """Run the worker.

        Args:
            max_batches: Maximum number of batches to process (None for unlimited)
        """
        logger.info(f"Worker {self.worker_id} starting")
        
        batches_processed = 0
        last_heartbeat = 0
        
        try:
            while True:
                # Send heartbeat if needed
                current_time = time.time()
                if current_time - last_heartbeat > self.heartbeat_interval:
                    self._send_heartbeat()
                    last_heartbeat = current_time
                
                # Get a batch
                batch = self._get_batch()
                
                if not batch:
                    # No more batches available
                    logger.info("No batches available, waiting...")
                    time.sleep(5)
                    continue
                
                # Process the batch
                try:
                    success = self.process_batch(batch)
                except Exception as e:
                    logger.error(f"Error processing batch: {e}")
                    success = False
                
                # Mark the batch as completed or failed
                self._complete_batch(batch, success=success)
                
                # Update processed count
                batches_processed += 1
                
                # Check if we've reached the maximum number of batches
                if max_batches and batches_processed >= max_batches:
                    logger.info(f"Reached maximum of {max_batches} batches")
                    break
                
                # Log memory usage periodically
                if batches_processed % 5 == 0:
                    process = psutil.Process(os.getpid())
                    memory_usage = process.memory_info().rss / 1024 / 1024  # MB
                    logger.info(f"Memory usage: {memory_usage:.1f} MB")
                
        except KeyboardInterrupt:
            logger.info("Worker interrupted")
        except Exception as e:
            logger.error(f"Worker error: {e}")
        finally:
            logger.info(f"Worker {self.worker_id} shutting down")


def main() -> None:
    """Main function."""
    parser = argparse.ArgumentParser(description="Backfill Worker")
    parser.add_argument(
        "--worker-id",
        type=str,
        default=f"worker-{uuid.uuid4().hex[:8]}",
        help="Unique identifier for this worker",
    )
    parser.add_argument(
        "--redis-host",
        type=str,
        default="localhost",
        help="Redis server hostname",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="Redis server port",
    )
    parser.add_argument(
        "--job-id",
        type=str,
        default=None,
        help="Job ID to work on (if None, will use the latest job)",
    )
    parser.add_argument(
        "--start-timestamp",
        type=str,
        default=None,
        help="Start timestamp for backfill (format: YYYY-MM-DD-HH:MM:SS)",
    )
    parser.add_argument(
        "--end-timestamp",
        type=str,
        default=None,
        help="End timestamp for backfill (format: YYYY-MM-DD-HH:MM:SS)",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Maximum number of batches to process",
    )
    
    args = parser.parse_args()
    
    # Initialize and run worker
    worker = BackfillWorker(
        worker_id=args.worker_id,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        job_id=args.job_id,
        start_timestamp=args.start_timestamp,
        end_timestamp=args.end_timestamp,
    )
    
    worker.run(max_batches=args.max_batches)


if __name__ == "__main__":
    main() 