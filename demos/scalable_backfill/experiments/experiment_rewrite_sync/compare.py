#!/usr/bin/env python3
"""
Comparison script for benchmarking Python vs Go implementations of the sync backfill system.

This script:
1. Takes a list of DIDs as input
2. Runs the same workload through both implementations
3. Measures and reports performance metrics for both
"""

import argparse
import json
import os
import subprocess
import sys
import time
import tempfile
import resource
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any

import psutil
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Ensure we can import from the parent project
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))))

# Try to import the Python implementation components
try:
    from pipelines.backfill_sync.handler import lambda_handler
except ImportError:
    print("Warning: Could not import Python implementation. Only Go benchmarks will be available.")
    HAS_PYTHON_IMPL = False
else:
    HAS_PYTHON_IMPL = True


class BenchmarkResult:
    """Holds the results of a benchmark run."""

    def __init__(self, 
                 implementation: str,
                 duration_seconds: float,
                 peak_memory_mb: float,
                 processed_count: int,
                 record_count: int = 0,
                 api_calls: int = 0,
                 rate_limited: int = 0,
                 success_rate: float = 1.0,
                 memory_samples: Optional[List[float]] = None):
        """Initialize benchmark result.

        Args:
            implementation: Name of the implementation ('python' or 'go')
            duration_seconds: Total runtime in seconds
            peak_memory_mb: Peak memory usage in MB
            processed_count: Number of DIDs processed
            record_count: Number of records processed
            api_calls: Number of API calls made
            rate_limited: Number of rate-limited calls
            success_rate: Proportion of successful DID processing
            memory_samples: List of memory usage samples over time
        """
        self.implementation = implementation
        self.duration_seconds = duration_seconds
        self.peak_memory_mb = peak_memory_mb
        self.processed_count = processed_count
        self.record_count = record_count
        self.api_calls = api_calls
        self.rate_limited = rate_limited
        self.success_rate = success_rate
        self.memory_samples = memory_samples or []
        
        # Calculate derived metrics
        self.dids_per_second = processed_count / duration_seconds if duration_seconds > 0 else 0
        self.mb_per_did = peak_memory_mb / processed_count if processed_count > 0 else 0
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "implementation": self.implementation,
            "duration_seconds": self.duration_seconds,
            "peak_memory_mb": self.peak_memory_mb,
            "processed_count": self.processed_count,
            "record_count": self.record_count,
            "api_calls": self.api_calls,
            "rate_limited": self.rate_limited,
            "success_rate": self.success_rate,
            "dids_per_second": self.dids_per_second,
            "mb_per_did": self.mb_per_did,
        }


def setup_argument_parser() -> argparse.ArgumentParser:
    """Set up command line argument parser."""
    parser = argparse.ArgumentParser(
        description="Compare Python and Go implementations of the sync backfill system."
    )
    parser.add_argument(
        "--input-file", 
        type=str, 
        required=True,
        help="Path to file containing list of DIDs (one per line)"
    )
    parser.add_argument(
        "--concurrency", 
        type=int, 
        default=4,
        help="Number of concurrent workers/goroutines to use"
    )
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=50,
        help="Batch size for processing DIDs"
    )
    parser.add_argument(
        "--timeout", 
        type=int, 
        default=30,
        help="HTTP request timeout in seconds"
    )
    parser.add_argument(
        "--rate-limit", 
        type=int, 
        default=3000,
        help="Rate limit (requests per window)"
    )
    parser.add_argument(
        "--output-dir", 
        type=str, 
        default="benchmark_results",
        help="Directory to save benchmark results"
    )
    parser.add_argument(
        "--skip-python", 
        action="store_true",
        help="Skip running the Python implementation"
    )
    parser.add_argument(
        "--skip-go", 
        action="store_true",
        help="Skip running the Go implementation"
    )
    parser.add_argument(
        "--sample-count", 
        type=int, 
        default=100,
        help="Number of DIDs to sample for benchmarking (0 for all)"
    )
    return parser


def load_dids_from_file(filename: str, sample_count: int = 0) -> List[str]:
    """Load DIDs from a file.
    
    Args:
        filename: Path to file containing DIDs (one per line)
        sample_count: Number of DIDs to sample (0 for all)
        
    Returns:
        List of DIDs
    """
    with open(filename, 'r') as f:
        dids = [line.strip() for line in f.readlines() if line.strip()]
    
    # Sample if requested
    if sample_count > 0 and sample_count < len(dids):
        np.random.seed(42)  # For reproducibility
        dids = np.random.choice(dids, sample_count, replace=False).tolist()
        
    return dids


def measure_python_implementation(dids: List[str], 
                                  concurrency: int = 4, 
                                  batch_size: int = 50) -> BenchmarkResult:
    """Run and measure the Python implementation.
    
    Args:
        dids: List of DIDs to process
        concurrency: Number of concurrent workers
        batch_size: Batch size for processing
        
    Returns:
        BenchmarkResult with performance metrics
    """
    print(f"Running Python implementation with {len(dids)} DIDs...")
    
    # Create a process to monitor memory usage
    memory_samples = []
    process = psutil.Process(os.getpid())
    stop_monitoring = False
    
    def memory_monitor():
        while not stop_monitoring:
            memory_mb = process.memory_info().rss / 1024 / 1024
            memory_samples.append(memory_mb)
            time.sleep(0.1)
    
    # Start memory monitoring in a separate thread
    import threading
    monitor_thread = threading.Thread(target=memory_monitor)
    monitor_thread.daemon = True
    monitor_thread.start()
    
    # Prepare input for the Python handler
    event = {
        "mode": "backfill",
        "dids": dids,
        "skip_backfill": True,  # Don't actually write to DB for benchmarking
        "concurrency": concurrency,
        "batch_size": batch_size,
    }
    
    # Run the implementation and measure time
    start_time = time.time()
    context = {}  # Mock Lambda context
    
    try:
        # Call the Python implementation
        result = lambda_handler(event, context)
        success = result.get("status") == "success"
    except Exception as e:
        print(f"Error running Python implementation: {e}")
        success = False
    
    # Stop timing and memory monitoring
    end_time = time.time()
    stop_monitoring = True
    monitor_thread.join(timeout=1.0)
    
    # Calculate metrics
    duration_seconds = end_time - start_time
    peak_memory_mb = max(memory_samples) if memory_samples else 0
    
    return BenchmarkResult(
        implementation="python",
        duration_seconds=duration_seconds,
        peak_memory_mb=peak_memory_mb,
        processed_count=len(dids),
        success_rate=1.0 if success else 0.0,
        memory_samples=memory_samples
    )


def measure_go_implementation(dids: List[str], 
                              concurrency: int = 4, 
                              batch_size: int = 50,
                              timeout: int = 30,
                              rate_limit: int = 3000) -> BenchmarkResult:
    """Run and measure the Go implementation.
    
    Args:
        dids: List of DIDs to process
        concurrency: Number of concurrent goroutines
        batch_size: Batch size for processing
        timeout: HTTP request timeout in seconds
        rate_limit: Rate limit (requests per window)
        
    Returns:
        BenchmarkResult with performance metrics
    """
    print(f"Running Go implementation with {len(dids)} DIDs...")
    
    # Create a temporary file to store DIDs
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        temp_dids_file = temp_file.name
        for did in dids:
            temp_file.write(f"{did}\n")
    
    # Create a temporary file for results
    with tempfile.NamedTemporaryFile(delete=False) as temp_results_file:
        temp_results_path = temp_results_file.name
    
    try:
        # Build the path to the Go binary
        script_dir = os.path.dirname(os.path.abspath(__file__))
        go_dir = os.path.join(script_dir, "go_sync")
        
        # Ensure Go binary is built
        build_process = subprocess.run(
            ["go", "build", "-o", "backfill_sync"],
            cwd=go_dir,
            capture_output=True,
            text=True
        )
        
        if build_process.returncode != 0:
            print(f"Failed to build Go binary: {build_process.stderr}")
            return BenchmarkResult(
                implementation="go",
                duration_seconds=0,
                peak_memory_mb=0,
                processed_count=0,
                success_rate=0.0
            )
        
        # Run the Go implementation
        cmd = [
            "./backfill_sync",
            "--input", temp_dids_file,
            "--output", temp_results_path,
            "--concurrency", str(concurrency),
            "--batch-size", str(batch_size),
            "--timeout", str(timeout),
            "--rate-limit", str(rate_limit),
            "--verbose"
        ]
        
        start_time = time.time()
        process = subprocess.run(
            cmd,
            cwd=go_dir,
            capture_output=True,
            text=True
        )
        end_time = time.time()
        
        if process.returncode != 0:
            print(f"Go implementation failed: {process.stderr}")
            return BenchmarkResult(
                implementation="go",
                duration_seconds=end_time - start_time,
                peak_memory_mb=0,
                processed_count=0,
                success_rate=0.0
            )
        
        # Parse results from the output file
        try:
            with open(temp_results_path, 'r') as f:
                results_data = json.load(f)
            
            return BenchmarkResult(
                implementation="go",
                duration_seconds=results_data.get("duration_ms", 0) / 1000,
                peak_memory_mb=results_data.get("memory_mb", 0),
                processed_count=results_data.get("processed_count", 0),
                record_count=results_data.get("record_count", 0),
                api_calls=results_data.get("api_calls", 0),
                rate_limited=results_data.get("rate_limited", 0),
                success_rate=results_data.get("success_rate", 0)
            )
        except Exception as e:
            print(f"Failed to parse Go results: {e}")
            return BenchmarkResult(
                implementation="go",
                duration_seconds=end_time - start_time,
                peak_memory_mb=0,
                processed_count=len(dids),
                success_rate=0.0
            )
    finally:
        # Clean up temporary files
        os.unlink(temp_dids_file)
        if os.path.exists(temp_results_path):
            os.unlink(temp_results_path)


def generate_comparison_report(python_result: Optional[BenchmarkResult], 
                               go_result: Optional[BenchmarkResult],
                               output_dir: str) -> None:
    """Generate a comparison report between Python and Go implementations.
    
    Args:
        python_result: Results from Python implementation, or None if skipped
        go_result: Results from Go implementation, or None if skipped
        output_dir: Directory to save report files
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create a timestamp for the report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_name = f"benchmark_{timestamp}"
    
    # Collect results for the report
    results = []
    if python_result:
        results.append(python_result.to_dict())
    if go_result:
        results.append(go_result.to_dict())
    
    # Save raw results as JSON
    with open(os.path.join(output_dir, f"{report_name}.json"), 'w') as f:
        json.dump(results, f, indent=2)
    
    # Generate comparative visualizations if we have both implementations
    if python_result and go_result:
        # Create a DataFrame for easy comparison
        df = pd.DataFrame([python_result.to_dict(), go_result.to_dict()])
        
        # 1. Speed comparison (DIDs per second)
        plt.figure(figsize=(10, 6))
        speed_comparison = df.plot.bar(
            x='implementation', 
            y='dids_per_second', 
            color=['blue', 'green'],
            legend=False
        )
        plt.title("Processing Speed Comparison")
        plt.ylabel("DIDs per second")
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Add value labels on bars
        for i, v in enumerate(df['dids_per_second']):
            plt.text(i, v + 0.1, f"{v:.2f}", ha='center')
        
        # Add speedup factor
        speedup = go_result.dids_per_second / python_result.dids_per_second if python_result.dids_per_second > 0 else 0
        plt.text(0.5, df['dids_per_second'].max() * 1.1, 
                f"Go is {speedup:.1f}x faster than Python", 
                ha='center', fontsize=12, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"{report_name}_speed.png"))
        
        # 2. Memory efficiency comparison
        plt.figure(figsize=(10, 6))
        memory_comparison = df.plot.bar(
            x='implementation', 
            y='mb_per_did', 
            color=['blue', 'green'],
            legend=False
        )
        plt.title("Memory Efficiency Comparison")
        plt.ylabel("MB per DID")
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Add value labels on bars
        for i, v in enumerate(df['mb_per_did']):
            plt.text(i, v + 0.01, f"{v:.2f}", ha='center')
        
        # Add efficiency factor
        memory_factor = python_result.mb_per_did / go_result.mb_per_did if go_result.mb_per_did > 0 else 0
        plt.text(0.5, df['mb_per_did'].max() * 1.1, 
                f"Go uses {memory_factor:.1f}x less memory per DID", 
                ha='center', fontsize=12, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"{report_name}_memory.png"))
        
        # 3. Generate a text report
        with open(os.path.join(output_dir, f"{report_name}.md"), 'w') as f:
            f.write(f"# Sync Implementation Benchmark: Python vs Go\n\n")
            f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("## Summary\n\n")
            f.write(f"- **Go Speed Improvement**: {speedup:.2f}x faster than Python\n")
            f.write(f"- **Go Memory Efficiency**: {memory_factor:.2f}x less memory per DID\n\n")
            
            f.write("## Detailed Comparison\n\n")
            
            # Create a comparison table
            f.write("| Metric | Python | Go | Improvement |\n")
            f.write("| ------ | ------ | -- | ----------- |\n")
            
            # Processing speed
            f.write(f"| Processing Speed | {python_result.dids_per_second:.2f} DIDs/sec | {go_result.dids_per_second:.2f} DIDs/sec | {speedup:.2f}x |\n")
            
            # Execution time
            f.write(f"| Execution Time | {python_result.duration_seconds:.2f} sec | {go_result.duration_seconds:.2f} sec | {python_result.duration_seconds/go_result.duration_seconds if go_result.duration_seconds > 0 else 0:.2f}x |\n")
            
            # Memory usage
            f.write(f"| Peak Memory | {python_result.peak_memory_mb:.2f} MB | {go_result.peak_memory_mb:.2f} MB | {python_result.peak_memory_mb/go_result.peak_memory_mb if go_result.peak_memory_mb > 0 else 0:.2f}x |\n")
            
            # Memory efficiency
            f.write(f"| Memory per DID | {python_result.mb_per_did:.2f} MB | {go_result.mb_per_did:.2f} MB | {memory_factor:.2f}x |\n")
            
            f.write("\n## Conclusion\n\n")
            
            if speedup > 1.5 and memory_factor > 1.5:
                f.write("The Go implementation shows **significant improvements** in both speed and memory efficiency. This makes it a compelling option for scaling the backfill system to handle hundreds of thousands of DIDs.\n\n")
            elif speedup > 1.5:
                f.write("The Go implementation shows **significant speed improvements**, making it suitable for larger workloads where processing time is the primary concern.\n\n")
            elif memory_factor > 1.5:
                f.write("The Go implementation shows **significant memory efficiency improvements**, making it suitable for processing large numbers of DIDs with limited memory resources.\n\n")
            else:
                f.write("The Go implementation shows some improvements, but they may not justify the additional maintenance overhead of supporting two codebases.\n\n")
            
            f.write("### Next Steps\n\n")
            f.write("1. Test with larger datasets (10,000+ DIDs) to validate scaling characteristics\n")
            f.write("2. Measure actual PDS API interactions (not simulated)\n")
            f.write("3. Implement and test CAR file parsing in Go\n")
            f.write("4. Evaluate integration complexity with the existing Python ecosystem\n")


def main():
    """Run the comparison benchmark."""
    parser = setup_argument_parser()
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Load DIDs from file
    dids = load_dids_from_file(args.input_file, args.sample_count)
    print(f"Loaded {len(dids)} DIDs for benchmarking")
    
    # Run implementations
    python_result = None
    go_result = None
    
    if not args.skip_python and HAS_PYTHON_IMPL:
        python_result = measure_python_implementation(
            dids=dids,
            concurrency=args.concurrency,
            batch_size=args.batch_size
        )
        print(f"Python: {python_result.processed_count} DIDs in {python_result.duration_seconds:.2f}s "
             f"({python_result.dids_per_second:.2f} DIDs/s), {python_result.peak_memory_mb:.2f}MB peak")
    elif not args.skip_python:
        print("Python implementation is not available. Skipping.")
    
    if not args.skip_go:
        go_result = measure_go_implementation(
            dids=dids,
            concurrency=args.concurrency,
            batch_size=args.batch_size,
            timeout=args.timeout,
            rate_limit=args.rate_limit
        )
        print(f"Go: {go_result.processed_count} DIDs in {go_result.duration_seconds:.2f}s "
             f"({go_result.dids_per_second:.2f} DIDs/s), {go_result.peak_memory_mb:.2f}MB peak")
    
    # Generate comparison report
    generate_comparison_report(python_result, go_result, args.output_dir)
    print(f"Saved benchmark results to {args.output_dir}")


if __name__ == "__main__":
    main() 