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
import os
import random
import secrets
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
import psutil

# Add the project root to sys.path
project_root = str(Path(os.path.abspath(__file__)).parent.parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from demos.firehose_ingestion.db import FirehoseDB, FirehoseRecord
from lib.helper import BSKY_DATA_DIR
from lib.log.logger import get_logger

logger = get_logger(__file__)

# Kinds of messages in the firehose
MESSAGE_KINDS = ["commit"]

# Types of collections
COLLECTIONS = ["app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.graph.follow", "app.bsky.feed.repost"]

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


def generate_plc_did() -> str:
    """Generate a random PLC-like DID."""
    random_hex = secrets.token_hex(16)
    return f"did:plc:{random_hex}"


def generate_test_record() -> Dict:
    """Generate a test record that matches the structure of a FirehoseRecord."""
    did = generate_plc_did()
    time_us = str(int(time.time() * 1000000))  # Current time in microseconds
    collection = random.choice(COLLECTIONS)
    
    # Create commit data specific to the collection type
    if collection == "app.bsky.feed.post":
        record = {
            "text": f"This is test post {random.randint(1, 10000)}",
            "createdAt": time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        }
    elif collection == "app.bsky.feed.like":
        record = {
            "subject": {
                "uri": f"at://did:plc:{secrets.token_hex(8)}/app.bsky.feed.post/{secrets.token_hex(4)}",
                "cid": f"bafyrei{secrets.token_hex(32)}",
            },
            "createdAt": time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        }
    elif collection == "app.bsky.graph.follow":
        record = {
            "subject": f"did:plc:{secrets.token_hex(8)}",
            "createdAt": time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        }
    elif collection == "app.bsky.feed.repost":
        record = {
            "subject": {
                "uri": f"at://did:plc:{secrets.token_hex(8)}/app.bsky.feed.post/{secrets.token_hex(4)}",
                "cid": f"bafyrei{secrets.token_hex(32)}",
            },
            "createdAt": time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        }
    
    # Generate a random rkey
    rkey = secrets.token_hex(4)
    
    # Create commit object that contains the record
    commit_data = {
        "repo": did,
        "rev": f"bafyrei{secrets.token_hex(24)}",
        "operation": "create",
        "collection": collection,
        "rkey": rkey,
        "record": record
    }
    
    # Return the full FirehoseRecord compatible structure
    return {
        "did": did,
        "time_us": time_us, 
        "kind": "commit",
        "commit_data": json.dumps(commit_data),
        "collection": collection
    }


def generate_bulk_test_records(num_records: int) -> List[Dict]:
    """Generate a specified number of test records.
    
    Args:
        num_records: Number of records to generate
        
    Returns:
        List of dictionaries with test record data
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
    db_name: str = "test_firehose",
    num_records: int = 1000,
    batch_size: int = 100,
    skip_single: bool = False
) -> Tuple[float, float]:
    """Run performance tests for the FirehoseDB.
    
    Args:
        db_name: Name of the test database
        num_records: Number of test records to generate
        batch_size: Size of batches for batch insertion
        skip_single: If True, skip single record insertion tests
        
    Returns:
        Tuple containing (single_insert_time, batch_insert_time)
    """
    print(f"Creating a new test database: {db_name}")
    db = FirehoseDB(db_name, create_new_db=True)
    
    # Generate test records
    test_records = generate_bulk_test_records(num_records)
    
    # Test single record insertion
    single_times = []
    if not skip_single:
        print(f"Testing single record insertion for {min(100, num_records)} records...")
        for i, record in enumerate(test_records[:min(100, num_records)]):
            if i % 10 == 0:
                print(f"Inserting record {i + 1}/{min(100, num_records)}...")
            try:
                insert_time = db.add_record(record)
                single_times.append(insert_time)
            except Exception as e:
                print(f"Error inserting record: {e}")
                break
        
        # Print single insertion stats
        if single_times:
            avg_time = sum(single_times) / len(single_times)
            print(f"Single insertion - Total time: {sum(single_times):.4f}s, "
                  f"Average: {avg_time * 1000:.2f}ms per record, "
                  f"Rate: {1 / avg_time:.2f} records/second")
        else:
            print("No single insertions were performed!")
    
    # Clear the database for batch insertion test
    db.clear_database()
    
    # Test batch insertion
    print(f"Testing batch insertion for {num_records} records...")
    start_time = time.time()
    
    # Calculate number of batches
    num_batches = (num_records + batch_size - 1) // batch_size
    batch_times = []
    
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, num_records)
        batch = test_records[start_idx:end_idx]
        
        print(f"Inserting batch {i + 1}/{num_batches} ({len(batch)} records)...")
        batch_time = db.batch_add_records(batch, batch_size=min(batch_size, 100))
        batch_times.append(batch_time)
    
    batch_total_time = time.time() - start_time
    
    # Print batch insertion stats
    if batch_times:
        print(f"Batch insertion - Total time: {batch_total_time:.4f}s, "
              f"Average: {batch_total_time * 1000 / num_records:.2f}ms per record, "
              f"Rate: {num_records / batch_total_time:.2f} records/second")
    else:
        print("No batch insertions were performed!")
    
    # Print final database stats
    stats = db.get_stats()
    print(f"Final database stats:")
    print(f"  Total records: {stats.total_records}")
    print(f"  Total write time: {stats.total_write_time:.4f}s")
    print(f"  Average write time: {stats.avg_write_time:.2f}ms per record")
    print(f"  Sync time: {stats.sync_time:.4f}s")
    
    return (sum(single_times) if single_times else 0, batch_total_time)


def main():
    """Main entry point for the test script."""
    parser = argparse.ArgumentParser(
        description='Test FirehoseDB performance',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--db_name', type=str, default="test_firehose",
                       help='Name of the test database')
    parser.add_argument('--num_records', type=int, default=1000,
                       help='Number of records to insert during the test')
    parser.add_argument('--batch_size', type=int, default=100,
                       help='Number of records per batch for batch insertions')
    parser.add_argument('--skip_single', action='store_true',
                       help='Skip testing single record insertions')
    
    args = parser.parse_args()
    
    if args.num_records <= 0:
        raise ValueError("Number of records must be positive")
    
    run_firehose_db_test(
        db_name=args.db_name,
        num_records=args.num_records,
        batch_size=args.batch_size,
        skip_single=args.skip_single
    )


if __name__ == "__main__":
    main() 