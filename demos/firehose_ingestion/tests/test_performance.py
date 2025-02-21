"""Performance tests for the firehose ingestion system."""

import asyncio
import time
import psutil
import statistics
from typing import Dict, List
from rich.console import Console
from rich.table import Table

from ..utils import FirehoseSubscriber, BatchWriter
from ..config import settings

console = Console()

class PerformanceResults:
    def __init__(self):
        self.results = []
        self.start_time = time.time()
        self.process = psutil.Process()
        self.latencies = []

    def add_result(self, test_name: str, criteria: str, passed: bool, value: str):
        self.results.append({
            'test_name': test_name,
            'criteria': criteria,
            'passed': passed,
            'value': value
        })

    def record_latency(self, latency_ms: float):
        self.latencies.append(latency_ms)

    def get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        return self.process.memory_info().rss / 1024 / 1024

    def get_cpu_usage(self) -> float:
        """Get current CPU usage percentage."""
        return self.process.cpu_percent()

    def display_results(self):
        table = Table(title="Firehose Ingestion Performance Test Results")
        table.add_column("Test Name", style="cyan")
        table.add_column("Criteria", style="magenta")
        table.add_column("Actual Value", style="yellow")
        table.add_column("Passed", style="green")

        for result in self.results:
            table.add_row(
                result['test_name'],
                result['criteria'],
                result['value'],
                "✓" if result['passed'] else "✗"
            )

        console.print(table)

perf_results = PerformanceResults()

async def test_throughput(duration_seconds: int = 60):
    """Test processing throughput for a specified duration."""
    subscriber = FirehoseSubscriber()
    writer = BatchWriter()
    
    record_count = 0
    start_time = time.time()
    
    async for record in subscriber.subscribe():
        # Process record
        before_write = time.time()
        await writer.add_record(record)
        after_write = time.time()
        
        # Record latency
        latency_ms = (after_write - before_write) * 1000
        perf_results.record_latency(latency_ms)
        
        record_count += 1
        
        if time.time() - start_time >= duration_seconds:
            break
    
    # Calculate metrics
    elapsed_time = time.time() - start_time
    records_per_second = record_count / elapsed_time
    avg_latency = statistics.mean(perf_results.latencies)
    p95_latency = statistics.quantiles(perf_results.latencies, n=20)[18]  # 95th percentile
    max_memory = perf_results.get_memory_usage()
    avg_cpu = perf_results.get_cpu_usage()
    
    # Record results
    perf_results.add_result(
        "Processing Rate",
        "Should process >100 records/second",
        records_per_second > 100,
        f"{records_per_second:.2f} records/sec"
    )
    
    perf_results.add_result(
        "Average Latency",
        "Should be <100ms",
        avg_latency < 100,
        f"{avg_latency:.2f}ms"
    )
    
    perf_results.add_result(
        "P95 Latency",
        "Should be <200ms",
        p95_latency < 200,
        f"{p95_latency:.2f}ms"
    )
    
    perf_results.add_result(
        "Memory Usage",
        "Should be <500MB",
        max_memory < 500,
        f"{max_memory:.2f}MB"
    )
    
    perf_results.add_result(
        "CPU Usage",
        "Should be <50%",
        avg_cpu < 50,
        f"{avg_cpu:.2f}%"
    )

async def test_batch_sizes():
    """Test different batch size configurations."""
    test_batch_sizes = [500, 1000, 2000]
    writer = BatchWriter()
    
    for batch_size in test_batch_sizes:
        settings.BATCH_SIZE = batch_size
        start_time = time.time()
        memory_before = perf_results.get_memory_usage()
        
        # Process 5000 records with this batch size
        for i in range(5000):
            record_type = settings.RECORD_TYPES[i % len(settings.RECORD_TYPES)]
            record = {
                'type': record_type,
                'uri': f'test_uri_{i}',
                'cid': f'test_cid_{i}',
                'author': 'test_author',
                'record': {'test': 'data'}
            }
            await writer.add_record(record)
        
        elapsed_time = time.time() - start_time
        memory_after = perf_results.get_memory_usage()
        memory_impact = memory_after - memory_before
        
        perf_results.add_result(
            f"Batch Size {batch_size}",
            f"Should process 5000 records efficiently",
            elapsed_time < 60,  # Should process within 60 seconds
            f"{elapsed_time:.2f}s, {memory_impact:.2f}MB"
        )

async def run_performance_tests():
    """Run all performance tests."""
    console.print("[bold]Starting Performance Tests[/bold]")
    console.print("Running throughput test for 60 seconds...")
    await test_throughput()
    
    console.print("Testing different batch sizes...")
    await test_batch_sizes()
    
    perf_results.display_results()

if __name__ == "__main__":
    asyncio.run(run_performance_tests()) 