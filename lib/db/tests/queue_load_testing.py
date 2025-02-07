"""Load testing utility for the Queue implementation.

This script provides performance testing capabilities for the Queue class, which uses
SQLite as its underlying storage mechanism. It measures the insertion performance
of the queue under various load conditions by:
    1. Creating a temporary test queue
    2. Generating a specified number of test records
    3. Measuring the time taken to insert these records
    4. Reporting performance metrics
    5. Cleaning up test artifacts

The script is particularly useful for:
    - Benchmarking queue performance with different batch sizes
    - Testing the queue's behavior under high load
    - Validating performance optimizations
    - Comparing performance across different system configurations

Example usage:
    python -m lib.db.tests.queue_load_testing --num_records 10000

Performance metrics reported:
    - Total insertion time
    - Average insertion rate (records/second)
    - Final queue size verification

Note:
    The script automatically handles cleanup of all test artifacts, including
    SQLite database files and WAL mode files (-wal, -shm).
"""

import argparse
import random
import string
import time
from datetime import datetime

from lib.db.queue import Queue
from lib.log.logger import get_logger

logger = get_logger(__file__)


def generate_random_string(length: int = 10) -> str:
    """Generate a random string of fixed length.
    
    Args:
        length: The length of the random string to generate.
            Defaults to 10 characters.
    
    Returns:
        A random string containing ASCII letters and digits.
    """
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def generate_test_records(num_records: int) -> list[dict]:
    """Generate dummy records for testing.
    
    Creates a list of dictionaries, each representing a test record with
    a random URI and text content.
    
    Args:
        num_records: The number of test records to generate.
    
    Returns:
        A list of dictionaries, each containing 'uri' and 'text' keys with
        random content.
    
    Example record format:
        {
            'uri': 'at://abc123/post/def456',
            'text': 'Test post content xyz789'
        }
    """
    return [
        {
            "uri": f"at://{generate_random_string(10)}/post/{generate_random_string(15)}",
            "text": f"Test post content {generate_random_string(50)}"
        }
        for _ in range(num_records)
    ]


def get_db_size_mb(file_path: str) -> float:
    """Get the total size of the database files in megabytes.
    
    Includes the main database file and WAL files if they exist.
    
    Args:
        file_path: Path to the main database file.
        
    Returns:
        Total size in megabytes.
    """
    import os
    
    total_size = 0
    # Main DB file
    if os.path.exists(file_path):
        total_size += os.path.getsize(file_path)
    # WAL file
    if os.path.exists(f"{file_path}-wal"):
        total_size += os.path.getsize(f"{file_path}-wal")
    # SHM file
    if os.path.exists(f"{file_path}-shm"):
        total_size += os.path.getsize(f"{file_path}-shm")
    
    return total_size / (1024 * 1024)  # Convert bytes to MB


def run_load_test(num_records: int) -> None:
    """Run a complete load test of the Queue implementation.
    
    This function performs the following steps:
    1. Creates a temporary test queue with a timestamp-based name
    2. Generates the specified number of test records
    3. Times the batch insertion process
    4. Reports performance metrics
    5. Cleans up all test artifacts

    Args:
        num_records: The number of test records to insert during the test.
    
    Raises:
        Any exceptions from Queue operations will be propagated.
        
    Performance metrics logged:
        - Total time taken for insertion
        - Average insertion rate (records/second)
        - Final queue size verification
    
    Note:
        Even if an error occurs during testing, the cleanup process will
        still attempt to remove all test artifacts.
    """
    # Create a test queue with timestamp to avoid conflicts
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    queue_name = f"test_load_{timestamp}"
    
    try:
        # Initialize queue
        logger.info(f"Creating test queue: {queue_name}")
        queue = Queue(queue_name, create_new_queue=True)
        
        # Generate test records
        logger.info(f"Generating {num_records} test records...")
        test_records = generate_test_records(num_records)
        
        # Time the batch insertion
        logger.info("Starting batch insertion...")
        start_time = time.time()
        
        queue.batch_add_items_to_queue(test_records)
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # Report results
        logger.info(f"Insertion completed in {elapsed_time:.2f} seconds")
        logger.info(f"Average insertion rate: {num_records/elapsed_time:.2f} records/second")

        # Verify queue length
        final_count = queue.get_queue_length()
        logger.info(f"Final queue size: {final_count} records")
        
        # Report database size
        db_size = get_db_size_mb(queue.db_path)
        logger.info(f"Final database size: {db_size:.2f} MB")
        
    finally:
        # Cleanup: Remove the test database file
        import os
        db_path = queue.db_path
        if os.path.exists(db_path):
            logger.info(f"Cleaning up test database: {db_path}")
            os.remove(db_path)
            if os.path.exists(f"{db_path}-wal"):
                os.remove(f"{db_path}-wal")
            if os.path.exists(f"{db_path}-shm"):
                os.remove(f"{db_path}-shm")


def main():
    """Main entry point for the load testing script.
    
    Parses command line arguments and initiates the load test.
    
    Command line arguments:
        --num_records: (Required) Integer specifying the number of records
                      to insert during the test.
    
    Raises:
        ValueError: If num_records is not positive.
        
    Example usage:
        python -m lib.db.tests.queue_load_testing --num_records 10000
    """
    parser = argparse.ArgumentParser(
        description='Run load testing for Queue implementation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Example usage:
    python -m lib.db.tests.queue_load_testing --num_records 10000
    
    This will:
    1. Create a test queue with a timestamp-based name
    2. Insert 10000 test records
    3. Report insertion performance metrics
    4. Clean up test artifacts"""
    )
    parser.add_argument('--num_records', type=int, required=True,
                      help='Number of records to insert during the test')
    
    args = parser.parse_args()
    
    if args.num_records <= 0:
        raise ValueError("Number of records must be positive")
    
    run_load_test(args.num_records)


if __name__ == "__main__":
    main() 