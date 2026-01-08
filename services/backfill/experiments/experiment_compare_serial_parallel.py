"""Experimental file for comparing the performance of serial vs parallel backfill implementations.

This experiment directly compares the performance of do_backfill_for_users (serial)
and do_backfill_for_users_parallel (parallel) implementations.

The experiment:
1. Replicates a base set of DIDs to reach target count
2. Runs both implementations on the same dataset
3. Measures and compares:
   - Total execution time
   - Memory usage
   - Records processed
   - CPU utilization
   - Average time per DID

Results are saved to results_serial_parallel_{timestamp}.json
"""

import json
import time
import os
import psutil
import multiprocessing
from typing import Dict, List, Any, Callable

from services.backfill.sync.backfill import (
    do_backfill_for_users,
    do_backfill_for_users_parallel,
    default_start_timestamp,
    default_end_timestamp,
)
from lib.log.logger import get_logger
from lib.datetime_utils import generate_current_datetime_str

logger = get_logger(__name__)

# Base DID list - will be replicated to reach target count
BASE_DIDS = [
    "did:plc:w5mjarupsl6ihdrzwgnzdh4y",
    # Uncomment for more unique DIDs
    # "did:plc:e4itbqoxctxwrrfqgs2rauga",
    # "did:plc:gedsnv7yxi45a4g2gts37vyp",
    # "did:plc:fbnm4hjnzu4qwg3nfjfkdhay",
    # "did:plc:dsnypqaat7r5nw6phtfs6ixw",
]


def get_memory_usage() -> float:
    """Get current memory usage of the process in MB."""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    return memory_info.rss / (1024 * 1024)  # Convert to MB


def get_cpu_percent() -> float:
    """Get current CPU utilization percentage."""
    return psutil.cpu_percent(interval=0.1)


def measure_performance(
    func: Callable, dids: List[str], description: str
) -> Dict[str, Any]:
    """
    Measure the performance metrics of a backfill function.

    Args:
        func: The backfill function to measure
        dids: List of DIDs to process
        description: Description of the implementation being measured

    Returns:
        Dictionary containing performance metrics
    """
    logger.info(f"Starting {description} implementation test...")

    start_memory = get_memory_usage()
    start_cpu = get_cpu_percent()
    start_time = time.time()

    # Run the backfill
    result = func(
        dids=dids,
        start_timestamp=default_start_timestamp,
        end_timestamp=default_end_timestamp,
    )

    end_time = time.time()
    end_memory = get_memory_usage()
    end_cpu = get_cpu_percent()

    execution_time = end_time - start_time

    # Calculate metrics
    total_records = sum(
        sum(type_counts.values())
        for did_counts in result[0].values()
        for type_counts in [did_counts]
    )

    metrics = {
        "implementation": description,
        "execution_time_seconds": execution_time,
        "average_time_per_did": execution_time / len(dids),
        "memory_usage": {
            "start_mb": start_memory,
            "end_mb": end_memory,
            "peak_increase_mb": end_memory - start_memory,
        },
        "cpu_utilization": {
            "start_percent": start_cpu,
            "end_percent": end_cpu,
            "average_percent": (start_cpu + end_cpu) / 2,
        },
        "processing_stats": {
            "total_dids": len(dids),
            "total_records_processed": total_records,
            "records_per_second": total_records / execution_time
            if execution_time > 0
            else 0,
            "unique_record_types": list(
                set(
                    record_type
                    for did_counts in result[0].values()
                    for record_type in did_counts.keys()
                )
            ),
        },
    }

    logger.info(f"Completed {description} implementation test")
    logger.info(f"Execution time: {execution_time:.2f} seconds")
    logger.info(f"Records processed: {total_records}")

    return metrics


def replicate_dids(base_dids: List[str], target_count: int) -> List[str]:
    """
    Replicate the base DID list to reach the target count.

    Args:
        base_dids: List of base DIDs to replicate
        target_count: Target number of DIDs

    Returns:
        Replicated list of DIDs
    """
    if not base_dids:
        raise ValueError("Base DIDs list cannot be empty")

    replicated_dids = []
    while len(replicated_dids) < target_count:
        for did in base_dids:
            replicated_dids.append(did)
            if len(replicated_dids) >= target_count:
                break

    return replicated_dids[:target_count]


def run_comparison(settings: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run the comparison between serial and parallel implementations.

    Args:
        settings: Dictionary containing experiment settings

    Returns:
        Dictionary containing comparison results
    """
    results = {"settings": settings, "serial": {}, "parallel": {}, "comparison": {}}

    # Replicate DIDs to reach target count
    dids = replicate_dids(BASE_DIDS, settings["target_did_count"])
    logger.info(f"Running comparison with {len(dids)} DIDs")

    # Run serial implementation
    results["serial"] = measure_performance(do_backfill_for_users, dids, "Serial")

    # Run parallel implementation
    results["parallel"] = measure_performance(
        do_backfill_for_users_parallel, dids, "Parallel"
    )

    # Calculate comparison metrics
    serial_time = results["serial"]["execution_time_seconds"]
    parallel_time = results["parallel"]["execution_time_seconds"]

    results["comparison"] = {
        "speedup_factor": serial_time / parallel_time if parallel_time > 0 else 0,
        "time_reduction_percent": ((serial_time - parallel_time) / serial_time * 100)
        if serial_time > 0
        else 0,
        "memory_overhead_percent": (
            (
                results["parallel"]["memory_usage"]["peak_increase_mb"]
                - results["serial"]["memory_usage"]["peak_increase_mb"]
            )
            / results["serial"]["memory_usage"]["peak_increase_mb"]
            * 100
        )
        if results["serial"]["memory_usage"]["peak_increase_mb"] > 0
        else 0,
        "efficiency_summary": {
            "better_performance": "parallel"
            if parallel_time < serial_time
            else "serial",
            "memory_efficient": "parallel"
            if (
                results["parallel"]["memory_usage"]["peak_increase_mb"]
                < results["serial"]["memory_usage"]["peak_increase_mb"]
            )
            else "serial",
            "cpu_efficient": "parallel"
            if (
                results["parallel"]["cpu_utilization"]["average_percent"]
                < results["serial"]["cpu_utilization"]["average_percent"]
            )
            else "serial",
        },
    }

    return results


def create_experiment_settings() -> Dict[str, Any]:
    """Create settings for the comparison experiment."""
    cpu_count = multiprocessing.cpu_count()

    return {
        "target_did_count": 50,  # Number of DIDs to test with
        "timestamp": generate_current_datetime_str(),
        "system_info": {
            "cpu_count": cpu_count,
            "platform": f"{os.name} {os.uname().sysname} {os.uname().release}",
            "processor": os.uname().machine,
            "memory_gb": psutil.virtual_memory().total / (1024**3),
        },
    }


if __name__ == "__main__":
    # Get timestamp for file naming
    timestamp = generate_current_datetime_str().replace(" ", "_").replace(":", "-")
    results_file = f"results_serial_parallel_{timestamp}.json"

    # Create and run experiment
    settings = create_experiment_settings()
    logger.info("Starting serial vs parallel comparison experiment...")

    results = run_comparison(settings)

    # Save results
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Saved results to {results_file}")

    # Print summary
    print("\n" + "=" * 80)
    print("COMPARISON SUMMARY:")
    print(f"Speedup factor: {results['comparison']['speedup_factor']:.2f}x")
    print(f"Time reduction: {results['comparison']['time_reduction_percent']:.1f}%")
    print(f"Memory overhead: {results['comparison']['memory_overhead_percent']:.1f}%")
    print("\nBest implementation for:")
    print(
        f"- Performance: {results['comparison']['efficiency_summary']['better_performance']}"
    )
    print(
        f"- Memory efficiency: {results['comparison']['efficiency_summary']['memory_efficient']}"
    )
    print(
        f"- CPU efficiency: {results['comparison']['efficiency_summary']['cpu_efficient']}"
    )
    print("=" * 80 + "\n")

    print(f"For detailed results, please check the {results_file} file.")
