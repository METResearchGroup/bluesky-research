"""Experimental file for determining the appropriate way to parallelize the
backfill syncs across DIDs.

Some of the questions this aims to answer include:
1. Is the backfill process I/O-bound or compute-bound?
2. Which parallelization strategy (threading vs multiprocessing) is most effective?
3. What is the optimal number of workers for both strategies?
4. How do both strategies affect memory usage?
5. What is the performance scaling with increasing worker counts?
6. Is there a significant performance benefit to parallelization?

The experiment runs three types of tests:
- Sequential baseline (single thread/process)
- I/O-bound optimization using ThreadPoolExecutor
- Compute-bound optimization using ProcessPoolExecutor

Results are saved to:
- settings.json: Contains experiment parameters
- results.json: Contains performance metrics and analysis
"""

import json
import time
import os
import psutil
import multiprocessing
import concurrent.futures
import gc
from typing import Dict, List, Any, Tuple, Callable

from services.backfill.sync.backfill import (
    do_backfill_for_users,
    do_backfill_for_user,
    default_start_timestamp,
    default_end_timestamp,
)
from lib.log.logger import get_logger
from lib.datetime_utils import generate_current_datetime_str

logger = get_logger(__name__)

# Base DID list from the original code
BASE_DIDS = [
    "did:plc:w5mjarupsl6ihdrzwgnzdh4y",
    # Uncomment these if you want to use more unique DIDs
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


def measure_performance(
    func: Callable, 
    args: Tuple[Any, ...] = (), 
    kwargs: Dict[str, Any] = {}
) -> Dict[str, Any]:
    """
    Measure the performance of a function in terms of execution time and memory usage.
    
    Args:
        func: The function to measure
        args: Positional arguments to pass to the function
        kwargs: Keyword arguments to pass to the function
        
    Returns:
        Dictionary containing performance metrics
    """
    start_memory = get_memory_usage()
    start_time = time.time()
    
    result = func(*args, **kwargs)
    
    end_time = time.time()
    end_memory = get_memory_usage()
    
    # Force garbage collection to get more accurate memory measurements
    gc.collect()
    
    return {
        "execution_time_seconds": end_time - start_time,
        "start_memory_mb": start_memory,
        "end_memory_mb": end_memory,
        "peak_memory_increase_mb": end_memory - start_memory,
        "result_summary": {
            "processed_users": len(result[0]) if isinstance(result, tuple) and len(result) > 0 else "N/A",
            "total_record_types": len(result[1]) if isinstance(result, tuple) and len(result) > 1 else "N/A",
        }
    }


def sequential_backfill(dids: List[str]) -> Tuple[Dict, Dict, List]:
    """Run backfill sequentially for a list of DIDs."""
    for i, did in enumerate(dids):
        logger.info(f"[DID {i + 1}/{len(dids)}] Started processing {did}")
    
    result = do_backfill_for_users(
        dids=dids,
        start_timestamp=default_start_timestamp,
        end_timestamp=default_end_timestamp,
    )
    
    for i, did in enumerate(dids):
        logger.info(f"[DID {i + 1}/{len(dids)}] Finished processing {did}")
    
    return result


def process_batch(batch: List[str], total_dids: int, batch_start_idx: int) -> Tuple[Dict, Dict, List]:
    """Process a batch of DIDs for multiprocessing."""
    for i, did in enumerate(batch):
        current_idx = batch_start_idx + i
        logger.info(f"[DID {current_idx + 1}/{total_dids}] Started processing {did}")
    
    result = do_backfill_for_users(
        dids=batch,
        start_timestamp=default_start_timestamp,
        end_timestamp=default_end_timestamp,
    )
    
    for i, did in enumerate(batch):
        current_idx = batch_start_idx + i
        logger.info(f"[DID {current_idx + 1}/{total_dids}] Finished processing {did}")
    
    return result


def io_bound_parallel_backfill(
    dids: List[str], 
    max_workers: int
) -> Tuple[Dict, Dict, List]:
    """
    Run backfill using threading for I/O bound operations.
    
    This uses ThreadPoolExecutor which is better for I/O bound tasks
    as threads can efficiently wait for I/O operations to complete.
    
    Args:
        dids: List of DIDs to process
        max_workers: Maximum number of threads to use
        
    Returns:
        Combined results from all threads
    """
    # Split DIDs into batches based on worker count
    batch_size = max(1, len(dids) // max_workers)
    batches = [dids[i:i + batch_size] for i in range(0, len(dids), batch_size)]
    
    combined_did_counts = {}
    combined_type_records = {}
    combined_metadata = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create futures with batch start indices for progress tracking
        futures = [
            executor.submit(process_batch, batch, len(dids), i * batch_size) 
            for i, batch in enumerate(batches)
        ]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                did_counts, type_records, metadata = future.result()
                
                # Combine results
                combined_did_counts.update(did_counts)
                
                for record_type, records in type_records.items():
                    if record_type in combined_type_records:
                        combined_type_records[record_type].extend(records)
                    else:
                        combined_type_records[record_type] = records
                
                combined_metadata.extend(metadata)
                
            except Exception as e:
                logger.error(f"Error in thread: {e}")
    
    return combined_did_counts, combined_type_records, combined_metadata


def process_did(did: str, total_dids: int, current_idx: int) -> Tuple[str, Dict, Dict, Dict]:
    """Process a single DID for multiprocessing."""
    logger.info(f"[DID {current_idx + 1}/{total_dids}] Started processing {did}")
    counts, records, metadata = do_backfill_for_user(
        did=did,
        start_timestamp=default_start_timestamp,
        end_timestamp=default_end_timestamp,
    )
    logger.info(f"[DID {current_idx + 1}/{total_dids}] Finished processing {did}")
    return did, counts, records, metadata


def compute_bound_parallel_backfill(
    dids: List[str], 
    max_workers: int
) -> Tuple[Dict, Dict, List]:
    """
    Run backfill using multiprocessing for compute-bound operations.
    
    This uses ProcessPoolExecutor which is better for CPU-bound tasks
    as processes can utilize multiple CPU cores for parallel computation.
    
    Args:
        dids: List of DIDs to process
        max_workers: Maximum number of processes to use
        
    Returns:
        Combined results from all processes
    """
    combined_did_counts = {}
    combined_type_records = {}
    combined_metadata = []
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Create futures with enumerated DIDs for progress tracking
        futures = [executor.submit(process_did, did, len(dids), i) for i, did in enumerate(dids)]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                did, counts, records, metadata = future.result()
                
                # Combine results
                combined_did_counts[did] = counts
                
                for record_type, record_list in records.items():
                    if record_type in combined_type_records:
                        combined_type_records[record_type].extend(record_list)
                    else:
                        combined_type_records[record_type] = record_list
                
                combined_metadata.append(metadata)
                
            except Exception as e:
                logger.error(f"Error in process: {e}")
    
    return combined_did_counts, combined_type_records, combined_metadata


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
        # Add as many DIDs as needed to reach target count
        for did in base_dids:
            replicated_dids.append(did)
            if len(replicated_dids) >= target_count:
                break
    
    return replicated_dids[:target_count]


def run_experiments(settings: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run the parallelization experiments with the given settings.
    
    Args:
        settings: Dictionary of experiment settings
        
    Returns:
        Dictionary of experiment results
    """
    results = {
        "settings": settings,
        "sequential": {},
        "io_bound": {},
        "compute_bound": {},
    }
    
    # Replicate DIDs to reach target count
    dids = replicate_dids(BASE_DIDS, settings["target_did_count"])
    logger.info(f"Running experiments with {len(dids)} DIDs")
    
    # Run sequential experiment
    if settings["run_sequential"]:
        logger.info("Running sequential experiment...")
        results["sequential"] = measure_performance(sequential_backfill, args=(dids,))
    
    # Run I/O bound parallel experiments
    if settings["run_io_bound"]:
        for worker_count in settings["io_bound_worker_counts"]:
            logger.info(f"Running I/O bound experiment with {worker_count} workers...")
            results["io_bound"][f"workers_{worker_count}"] = measure_performance(
                io_bound_parallel_backfill, 
                args=(dids, worker_count)
            )
    
    # Run compute bound parallel experiments
    if settings["run_compute_bound"]:
        for worker_count in settings["compute_bound_worker_counts"]:
            logger.info(f"Running compute bound experiment with {worker_count} workers...")
            results["compute_bound"][f"workers_{worker_count}"] = measure_performance(
                compute_bound_parallel_backfill, 
                args=(dids, worker_count)
            )
    
    return results


def create_default_settings() -> Dict[str, Any]:
    """Create default settings for the experiments."""
    cpu_count = multiprocessing.cpu_count()
    
    return {
        "target_did_count": 50,  # Number of DIDs to use
        "run_sequential": True,  # Whether to run sequential experiment
        "run_io_bound": True,    # Whether to run I/O bound experiments
        "run_compute_bound": True,  # Whether to run compute bound experiments
        "io_bound_worker_counts": [2, 4, 8, 16],  # Number of threads to use for I/O bound
        "compute_bound_worker_counts": [2, 4, min(8, cpu_count), min(cpu_count, 16)],  # Number of processes for compute bound
        "timestamp": generate_current_datetime_str(),
        "cpu_count": cpu_count,
        "system_info": {
            "platform": f"{os.name} {platform.system()} {platform.release()}",
            "processor": platform.processor(),
            "memory_gb": psutil.virtual_memory().total / (1024**3),
        }
    }


def save_json(data: Dict[str, Any], filename: str) -> None:
    """Save data to a JSON file."""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)


def analyze_results(results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Analyze the results to determine if the process is I/O bound or compute bound.
    
    Args:
        results: Dictionary of experiment results
        
    Returns:
        Dictionary of analysis results
    """
    analysis = {
        "summary": {},
        "speedups": {},
        "conclusion": "",
    }
    
    # Skip analysis if sequential wasn't run
    if not results.get("sequential"):
        analysis["conclusion"] = "Cannot determine bound type: sequential baseline missing"
        return analysis
    
    sequential_time = results["sequential"]["execution_time_seconds"]
    analysis["summary"]["sequential_time"] = sequential_time
    
    # Calculate speedups for I/O bound
    if results.get("io_bound"):
        io_speedups = {}
        for worker_key, worker_results in results["io_bound"].items():
            worker_time = worker_results["execution_time_seconds"]
            speedup = sequential_time / worker_time if worker_time > 0 else 0
            io_speedups[worker_key] = speedup
        
        analysis["speedups"]["io_bound"] = io_speedups
        analysis["summary"]["max_io_speedup"] = max(io_speedups.values()) if io_speedups else 0
    
    # Calculate speedups for compute bound
    if results.get("compute_bound"):
        compute_speedups = {}
        for worker_key, worker_results in results["compute_bound"].items():
            worker_time = worker_results["execution_time_seconds"]
            speedup = sequential_time / worker_time if worker_time > 0 else 0
            compute_speedups[worker_key] = speedup
        
        analysis["speedups"]["compute_bound"] = compute_speedups
        analysis["summary"]["max_compute_speedup"] = max(compute_speedups.values()) if compute_speedups else 0
    
    # Determine if I/O bound or compute bound
    if (analysis["summary"].get("max_io_speedup", 0) > 
            analysis["summary"].get("max_compute_speedup", 0)):
        analysis["conclusion"] = "Process appears to be I/O bound"
    elif (analysis["summary"].get("max_compute_speedup", 0) > 
            analysis["summary"].get("max_io_speedup", 0)):
        analysis["conclusion"] = "Process appears to be compute bound"
    else:
        analysis["conclusion"] = "Inconclusive - similar speedups for both approaches"
    
    # Check if speedups are significant
    max_speedup = max(
        analysis["summary"].get("max_io_speedup", 0),
        analysis["summary"].get("max_compute_speedup", 0)
    )
    
    if max_speedup < 1.2:  # Less than 20% improvement
        analysis["conclusion"] += " (minimal parallelization benefit)"
    elif max_speedup > 2:  # More than 2x improvement
        analysis["conclusion"] += " (significant parallelization benefit)"
    
    return analysis


if __name__ == "__main__":
    import platform
    
    # Get timestamp for file naming
    timestamp = generate_current_datetime_str().replace(" ", "_").replace(":", "-")
    settings_file = f"settings_{timestamp}.json"
    results_file = f"results_{timestamp}.json"
    
    # Create and save settings
    settings = create_default_settings()
    save_json(settings, settings_file)
    logger.info(f"Saved settings to {settings_file}: {settings}")
    
    # Run experiments
    logger.info("Starting parallelization experiments...")
    results = run_experiments(settings)
    
    # Analyze results
    analysis = analyze_results(results)
    results["analysis"] = analysis
    
    # Save results
    save_json(results, results_file)
    logger.info(f"Saved results to {results_file}")
    
    # Print conclusion
    print("\n" + "="*80)
    print("EXPERIMENT CONCLUSION:")
    print(analysis["conclusion"])
    print("="*80 + "\n")
    
    if "sequential" in results:
        print(f"Sequential execution time: {results['sequential']['execution_time_seconds']:.2f} seconds")
    
    if "io_bound" in results and results["io_bound"]:
        best_io = min(results["io_bound"].items(), key=lambda x: x[1]["execution_time_seconds"])
        print(f"Best I/O bound configuration ({best_io[0]}): {best_io[1]['execution_time_seconds']:.2f} seconds")
        
    if "compute_bound" in results and results["compute_bound"]:
        best_compute = min(results["compute_bound"].items(), key=lambda x: x[1]["execution_time_seconds"])
        print(f"Best compute bound configuration ({best_compute[0]}): {best_compute[1]['execution_time_seconds']:.2f} seconds")
    
    print(f"\nFor detailed results, please check the {results_file} file.")
