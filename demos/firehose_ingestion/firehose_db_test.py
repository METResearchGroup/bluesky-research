"""Test module for the FirehoseDB implementation.

This script tests the performance of the FirehoseDB class by:
1. Creating a test database
2. Generating 1,000 sample firehose records
3. Inserting them into the database while measuring performance metrics
4. Reporting detailed statistics on throughput and resource usage

Example usage:
    python -m demos.firehose_ingestion.firehose_db_test
"""

import argparse
import json
import random
import string
import time
import uuid
from typing import Dict, List, Tuple
import psutil

from demos.firehose_ingestion.db import FirehoseDB, FirehoseRecord
from lib.log.logger import get_logger

logger = get_logger(__file__)

# Operations that can be performed on records
OPERATIONS = ["create", "update", "delete"]

# Types of records to generate
RECORD_TYPES = ["app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.graph.follow"]

class ResourceMonitor:
    """Monitor system resource usage during testing."""
    
    def __init__(self):
        """Initialize the resource monitor."""
        self.process = psutil.Process()
        self.start_cpu_times = None
        self.start_memory = None
        self.has_io_counters = hasattr(self.process, 'io_counters')
        if self.has_io_counters:
            self.start_io_counters = None
        
    def start_monitoring(self):
        """Start monitoring resource usage."""
        self.start_cpu_times = self.process.cpu_times()
        self.start_memory = self.process.memory_info()
        if self.has_io_counters:
            self.start_io_counters = self.process.io_counters()
        
    def get_metrics(self) -> Dict:
        """Get resource usage metrics.
        
        Returns:
            Dictionary containing resource usage metrics
        """
        current_cpu_times = self.process.cpu_times()
        current_memory = self.process.memory_info()
        
        metrics = {
            "cpu_user_time": current_cpu_times.user - self.start_cpu_times.user,
            "cpu_system_time": current_cpu_times.system - self.start_cpu_times.system,
            "memory_usage": current_memory.rss - self.start_memory.rss
        }
        
        if self.has_io_counters:
            current_io_counters = self.process.io_counters()
            metrics.update({
                "read_bytes": current_io_counters.read_bytes - self.start_io_counters.read_bytes,
                "write_bytes": current_io_counters.write_bytes - self.start_io_counters.write_bytes,
            })
            
        return metrics


def generate_random_string(length: int = 10) -> str:
    """Generate a random string of fixed length.
    
    Args:
        length: Length of the random string
    
    Returns:
        A random string of the specified length
    """
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def generate_test_record() -> Dict:
    """Generate a single test firehose record.
    
    Returns:
        Dictionary containing a synthetic firehose record
    """
    record_type = random.choice(RECORD_TYPES)
    
    # Generate record content based on type
    if record_type == "app.bsky.feed.post":
        record_content = {
            "$type": record_type,
            "text": f"Test post {generate_random_string(50)}",
            "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    elif record_type == "app.bsky.feed.like":
        record_content = {
            "$type": record_type,
            "subject": {
                "uri": f"at://user/{generate_random_string(10)}/app.bsky.feed.post/{generate_random_string(10)}",
                "cid": f"cid://{generate_random_string(30)}"
            },
            "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    else:  # app.bsky.graph.follow
        record_content = {
            "$type": record_type,
            "subject": f"did:plc:{generate_random_string(10)}",
            "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    
    return {
        "cid": f"cid://{uuid.uuid4().hex}",
        "operation": random.choice(OPERATIONS),
        "record": json.dumps(record_content),
        "rev": f"rev-{generate_random_string(8)}",
        "rkey": f"rkey-{generate_random_string(8)}",
    }


def generate_test_records(num_records: int) -> List[Dict]:
    """Generate multiple test firehose records.
    
    Args:
        num_records: Number of records to generate
    
    Returns:
        List of dictionaries, each containing a synthetic firehose record
    """
    return [generate_test_record() for _ in range(num_records)]


def test_single_inserts(db: FirehoseDB, records: List[Dict]) -> Tuple[float, List[float]]:
    """Test inserting records one at a time.
    
    Args:
        db: FirehoseDB instance to use
        records: List of records to insert
    
    Returns:
        Tuple of (total time, list of individual insertion times)
    """
    logger.info("Testing single record insertions...")
    insertion_times = []
    start_time = time.time()
    
    for i, record in enumerate(records):
        if i % 100 == 0:
            logger.info(f"Inserting record {i+1}/{len(records)}...")
        
        elapsed = db.add_record(record)
        insertion_times.append(elapsed)
    
    total_time = time.time() - start_time
    return total_time, insertion_times


def test_batch_inserts(
    db: FirehoseDB, 
    records: List[Dict], 
    batch_size: int = 100,
    batch_write_size: int = 10
) -> float:
    """Test inserting records in batches.
    
    Args:
        db: FirehoseDB instance to use
        records: List of records to insert
        batch_size: Number of records per batch
        batch_write_size: Number of batches to write in one transaction
    
    Returns:
        Total time taken for batch insertion
    """
    logger.info(f"Testing batch insertions with batch size {batch_size}...")
    return db.batch_add_records(records, batch_size, batch_write_size)


def run_firehose_db_test(
    num_records: int = 1000,
    batch_size: int = 100,
    batch_write_size: int = 10,
    test_single: bool = True,
    test_batch: bool = True
) -> None:
    """Run the complete firehose DB test suite.
    
    Args:
        num_records: Number of test records to generate
        batch_size: Number of records per batch for batch testing
        batch_write_size: Number of batches per transaction
        test_single: Whether to test single record insertions
        test_batch: Whether to test batch insertions
    """
    # Create a unique database name for this test run
    db_name = f"firehose_test_{int(time.time())}"
    logger.info(f"Creating test database: {db_name}")
    db = FirehoseDB(db_name, create_new_db=True)
    
    try:
        # Initialize resource monitoring
        monitor = ResourceMonitor()
        
        # Generate test records
        logger.info(f"Generating {num_records} test records...")
        test_records = generate_test_records(num_records)
        
        # Track overall metrics
        overall_start_time = time.time()
        monitor.start_monitoring()
        
        # Test single record insertions if requested
        if test_single and num_records <= 100:  # Limit for performance reasons
            logger.info("Running single insertion test...")
            single_total_time, single_insertion_times = test_single_inserts(db, test_records[:100])
            avg_single_time = sum(single_insertion_times) / len(single_insertion_times)
            logger.info(f"Single insertion results:")
            logger.info(f"  Total time: {single_total_time:.2f} seconds")
            logger.info(f"  Average time per record: {avg_single_time*1000:.2f} ms")
            logger.info(f"  Throughput: {100/single_total_time:.2f} records/second")
        
        # Always run batch test to get to 1000 records efficiently
        if test_batch:
            logger.info("Running batch insertion test...")
            
            records_to_insert = test_records
            if test_single and num_records <= 100:
                # If we already inserted some records in single mode, generate new ones
                records_needed = num_records - 100
                if records_needed > 0:
                    logger.info(f"Generating {records_needed} more records for batch test...")
                    records_to_insert = generate_test_records(records_needed)
                else:
                    records_to_insert = []
            
            if records_to_insert:
                batch_total_time = test_batch_inserts(
                    db, 
                    records_to_insert, 
                    batch_size=batch_size,
                    batch_write_size=batch_write_size
                )
                logger.info(f"Batch insertion results:")
                logger.info(f"  Total time: {batch_total_time:.2f} seconds")
                logger.info(f"  Average time per record: {batch_total_time*1000/len(records_to_insert):.2f} ms")
                logger.info(f"  Throughput: {len(records_to_insert)/batch_total_time:.2f} records/second")
        
        # Get final count and verify we reached our target
        final_count = db.get_record_count()
        overall_time = time.time() - overall_start_time
        metrics = monitor.get_metrics()
        
        # Get database stats
        db_stats = db.get_stats()
        
        # Report final results
        logger.info("\n" + "="*60)
        logger.info("FIREHOSE DB TEST RESULTS")
        logger.info("="*60)
        logger.info(f"Database name: {db_name}")
        logger.info(f"Records inserted: {final_count}")
        logger.info(f"Total time: {overall_time:.2f} seconds")
        logger.info(f"Overall throughput: {final_count/overall_time:.2f} records/second")
        logger.info(f"Average write time: {db_stats.avg_write_time:.2f} ms per record")
        logger.info(f"Total sync time: {db_stats.sync_time:.2f} seconds ({db_stats.sync_time/overall_time*100:.1f}% of total)")
        
        logger.info("\nResource Usage:")
        logger.info(f"  CPU time (user/system): {metrics['cpu_user_time']:.2f}s / {metrics['cpu_system_time']:.2f}s")
        if 'read_bytes' in metrics:
            logger.info(f"  I/O (read/write): {metrics['read_bytes']/1024/1024:.2f}MB / {metrics['write_bytes']/1024/1024:.2f}MB")
        logger.info(f"  Memory usage: {metrics['memory_usage']/1024/1024:.2f}MB")
        
        # Wait for target record count if not reached yet
        if final_count < num_records:
            logger.info(f"Waiting for record count to reach {num_records}...")
            wait_start = time.time()
            while db.get_record_count() < num_records:
                time.sleep(0.5)
                if time.time() - wait_start > 60:  # Timeout after 1 minute
                    logger.warning(f"Timeout waiting for records. Current count: {db.get_record_count()}")
                    break
                
            final_count = db.get_record_count()
            logger.info(f"Final record count: {final_count}")
    
    finally:
        # Clean up (uncomment to delete test database)
        # db.clear_database()
        pass


def main():
    """Main entry point for the test script."""
    parser = argparse.ArgumentParser(
        description='Test FirehoseDB performance',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--num_records', type=int, default=1000,
                       help='Number of records to insert during the test')
    parser.add_argument('--batch_size', type=int, default=100,
                       help='Number of records per batch for batch insertions')
    parser.add_argument('--batch_write_size', type=int, default=10,
                       help='Number of batches per transaction')
    parser.add_argument('--skip_single', action='store_true',
                       help='Skip testing single record insertions')
    
    args = parser.parse_args()
    
    if args.num_records <= 0:
        raise ValueError("Number of records must be positive")
    
    run_firehose_db_test(
        num_records=args.num_records,
        batch_size=args.batch_size,
        batch_write_size=args.batch_write_size,
        test_single=not args.skip_single,
        test_batch=True
    )


if __name__ == "__main__":
    main() 