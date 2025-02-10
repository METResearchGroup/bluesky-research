"""SQLite throughput testing utility.

This script provides performance testing capabilities for SQLite write operations.
It measures the maximum write throughput under various configurations:
    1. Default settings (baseline)
    2. WAL mode with normal synchronous mode
    3. WAL mode with batch transactions
    4. WAL mode with prepared statements

The script measures:
    - Writes per second
    - Total writes in a time period
    - Memory usage
    - I/O operations
    - CPU usage

Example usage:
    python -m lib.db.tests.throughput_testing_sqlite --num_records 10000 --duration 30

Performance metrics reported:
    - Total insertion time
    - Average insertion rate (records/second)
    - Memory usage
    - I/O statistics
    - CPU utilization
"""

import argparse
import os
import random
import sqlite3
import string
import time
import psutil
from datetime import datetime
from typing import Dict, List, Optional, Tuple

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


def generate_test_records(num_records: int) -> List[Dict]:
    """Generate dummy records for testing.
    
    Creates a list of dictionaries, each representing a test record with
    random content.
    
    Args:
        num_records: The number of test records to generate.
    
    Returns:
        A list of dictionaries, each containing test data fields.
    
    Example record format:
        {
            'id': 1,
            'text': 'Random text content',
            'created_at': '2024-03-14T12:34:56'
        }
    """
    return [
        {
            "id": i,
            "text": f"Test content {generate_random_string(50)}",
            "created_at": datetime.now().isoformat()
        }
        for i in range(num_records)
    ]


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
            Dictionary containing resource usage metrics:
            - CPU user time
            - CPU system time
            - Read bytes (if available)
            - Write bytes (if available)
            - Memory usage
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


class SQLiteThroughputTester:
    """Test SQLite write throughput under different configurations."""
    
    def __init__(self, db_path: str):
        """Initialize the tester.
        
        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = db_path
        self.monitor = ResourceMonitor()
        
    def setup_database(self, optimize: bool = False):
        """Set up the test database.
        
        Args:
            optimize: Whether to enable optimizations (WAL mode, etc.).
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create test table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_records (
                id INTEGER PRIMARY KEY,
                text TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        
        if optimize:
            # Enable WAL mode and optimize settings
            cursor.execute("PRAGMA journal_mode = WAL")
            cursor.execute("PRAGMA synchronous = NORMAL")
            cursor.execute("PRAGMA cache_size = -2000")  # 2MB cache
            cursor.execute("PRAGMA page_size = 4096")
            cursor.execute("PRAGMA temp_store = MEMORY")
            cursor.execute("PRAGMA mmap_size = 30000000000")
            
        conn.commit()
        conn.close()
        
    def run_baseline_test(self, records: List[Dict], duration: int) -> Tuple[int, float, Dict]:
        """Run baseline test with default settings.
        
        Args:
            records: List of test records to insert.
            duration: Maximum test duration in seconds.
            
        Returns:
            Tuple of (records written, time taken, resource metrics).
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        self.monitor.start_monitoring()
        start_time = time.time()
        records_written = 0
        
        try:
            for record in records:
                if time.time() - start_time > duration:
                    break
                    
                cursor.execute(
                    "INSERT INTO test_records (id, text, created_at) VALUES (?, ?, ?)",
                    (record["id"], record["text"], record["created_at"])
                )
                conn.commit()  # Commit each record individually
                records_written += 1
                
        finally:
            conn.close()
            
        end_time = time.time()
        metrics = self.monitor.get_metrics()
        
        return records_written, end_time - start_time, metrics
        
    def run_optimized_test(self, records: List[Dict], duration: int) -> Tuple[int, float, Dict]:
        """Run optimized test with WAL mode and batching.
        
        Args:
            records: List of test records to insert.
            duration: Maximum test duration in seconds.
            
        Returns:
            Tuple of (records written, time taken, resource metrics).
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Cache the SQL statement
        insert_sql = "INSERT INTO test_records (id, text, created_at) VALUES (?, ?, ?)"
        
        self.monitor.start_monitoring()
        start_time = time.time()
        records_written = 0
        
        try:
            conn.execute("BEGIN")
            batch_size = 1000  # Commit every 1000 records
            
            for i, record in enumerate(records):
                if time.time() - start_time > duration:
                    break
                    
                cursor.execute(insert_sql, (record["id"], record["text"], record["created_at"]))
                records_written += 1
                
                if i % batch_size == 0:
                    conn.commit()
                    conn.execute("BEGIN")
                    
            conn.commit()  # Final commit
            
        finally:
            conn.close()
            
        end_time = time.time()
        metrics = self.monitor.get_metrics()
        
        return records_written, end_time - start_time, metrics


def run_throughput_test(num_records: int, duration: int = 30) -> None:
    """Run complete throughput test suite.
    
    Args:
        num_records: Number of test records to generate.
        duration: Test duration in seconds.
    """
    # Generate test data
    logger.info(f"Generating {num_records} test records...")
    records = generate_test_records(num_records)
    
    # Create timestamp for unique test files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    baseline_db = f"test_baseline_{timestamp}.db"
    optimized_db = f"test_optimized_{timestamp}.db"
    
    try:
        # Baseline test
        logger.info("\nRunning baseline test...")
        baseline_tester = SQLiteThroughputTester(baseline_db)
        baseline_tester.setup_database(optimize=False)
        baseline_written, baseline_time, baseline_metrics = baseline_tester.run_baseline_test(
            records, duration
        )
        
        # Optimized test
        logger.info("\nRunning optimized test...")
        optimized_tester = SQLiteThroughputTester(optimized_db)
        optimized_tester.setup_database(optimize=True)
        optimized_written, optimized_time, optimized_metrics = optimized_tester.run_optimized_test(
            records, duration
        )
        
        # Report results
        logger.info("\nTest Results:")
        logger.info("\nBaseline Test (Default Settings):")
        logger.info(f"Records written: {baseline_written}")
        logger.info(f"Time taken: {baseline_time:.2f} seconds")
        logger.info(f"Write rate: {baseline_written/baseline_time:.2f} records/second")
        logger.info(f"CPU time (user/system): {baseline_metrics['cpu_user_time']:.2f}s / {baseline_metrics['cpu_system_time']:.2f}s")
        if 'read_bytes' in baseline_metrics:
            logger.info(f"I/O (read/write): {baseline_metrics['read_bytes']/1024/1024:.2f}MB / {baseline_metrics['write_bytes']/1024/1024:.2f}MB")
        logger.info(f"Memory usage: {baseline_metrics['memory_usage']/1024/1024:.2f}MB")
        
        logger.info("\nOptimized Test (WAL Mode + Batching):")
        logger.info(f"Records written: {optimized_written}")
        logger.info(f"Time taken: {optimized_time:.2f} seconds")
        logger.info(f"Write rate: {optimized_written/optimized_time:.2f} records/second")
        logger.info(f"CPU time (user/system): {optimized_metrics['cpu_user_time']:.2f}s / {optimized_metrics['cpu_system_time']:.2f}s")
        if 'read_bytes' in optimized_metrics:
            logger.info(f"I/O (read/write): {optimized_metrics['read_bytes']/1024/1024:.2f}MB / {optimized_metrics['write_bytes']/1024/1024:.2f}MB")
        logger.info(f"Memory usage: {optimized_metrics['memory_usage']/1024/1024:.2f}MB")
        
    finally:
        # Cleanup
        for db_file in [baseline_db, optimized_db]:
            if os.path.exists(db_file):
                os.remove(db_file)
            if os.path.exists(f"{db_file}-wal"):
                os.remove(f"{db_file}-wal")
            if os.path.exists(f"{db_file}-shm"):
                os.remove(f"{db_file}-shm")


def main():
    """Main entry point for the throughput testing script."""
    parser = argparse.ArgumentParser(
        description='Run SQLite throughput testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Example usage:
    python -m lib.db.tests.throughput_testing_sqlite --num_records 10000 --duration 30
    
    This will:
    1. Create test databases
    2. Run baseline and optimized tests
    3. Report performance metrics
    4. Clean up test artifacts"""
    )
    parser.add_argument('--num_records', type=int, required=True,
                      help='Number of records to insert during the test')
    parser.add_argument('--duration', type=int, default=30,
                      help='Maximum test duration in seconds (default: 30)')
    
    args = parser.parse_args()
    
    if args.num_records <= 0:
        raise ValueError("Number of records must be positive")
    
    run_throughput_test(args.num_records, args.duration)


if __name__ == "__main__":
    main() 