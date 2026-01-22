"""Validation script for track_performance and track_memory_usage decorators.

This script tests the memory tracking functions to ensure they correctly
measure comprehensive memory metrics during function execution:
- Peak memory: Maximum memory used (critical for OOM prevention)
- Average memory: Mean memory usage (useful for capacity planning)
- End memory: Memory at completion (shows retained memory)
- Memory increase: Peak minus baseline (shows function's contribution)
- Growth rate: Memory growth rate in MB/s (useful for leak detection)
"""

import time
from lib.helper import track_memory_usage, track_performance


def allocate_memory(size_mb: int) -> list:
    """Allocate a specific amount of memory in MB."""
    # Each element is approximately 8 bytes (float64)
    # So we need size_mb * 1024 * 1024 / 8 elements
    num_elements = int(size_mb * 1024 * 1024 / 8)
    return [0.0] * num_elements


@track_memory_usage
def test_memory_allocation(size_mb: int):
    """Test function that allocates memory."""
    data = allocate_memory(size_mb)
    time.sleep(0.5)  # Simulate some work
    return len(data)


@track_performance
def test_performance_tracking(size_mb: int):
    """Test function for performance tracking."""
    data = allocate_memory(size_mb)
    time.sleep(0.5)  # Simulate some work
    return len(data)


@track_performance
def test_long_running_with_growth(duration_seconds: int, growth_mb_per_second: float):
    """Test function that simulates memory growth over time (for growth rate testing)."""
    chunks = []
    chunk_size_mb = 5
    
    # Allocate memory gradually to simulate growth
    for i in range(duration_seconds * 2):  # Allocate every 0.5 seconds
        chunks.append(allocate_memory(chunk_size_mb))
        time.sleep(0.5)
    
    return len(chunks)


def main():
    """Run validation tests."""
    print("=" * 60)
    print("Testing track_memory_usage decorator")
    print("=" * 60)
    
    # Test with different memory allocations
    for size_mb in [10, 50, 100]:
        print(f"\nTest: Allocating {size_mb} MB")
        result = test_memory_allocation(size_mb)
        print(f"Result: {result} elements allocated")
    
    print("\n" + "=" * 60)
    print("Testing track_performance decorator")
    print("=" * 60)
    
    # Test with different memory allocations
    for size_mb in [10, 50, 100]:
        print(f"\nTest: Allocating {size_mb} MB")
        result = test_performance_tracking(size_mb)
        print(f"Result: {result} elements allocated")
    
    print("\n" + "=" * 60)
    print("Testing growth rate detection (long-running function)")
    print("=" * 60)
    print("\nTest: Simulating memory growth over 5 seconds")
    result = test_long_running_with_growth(duration_seconds=5, growth_mb_per_second=10.0)
    print(f"Result: {result} chunks allocated")
    
    print("\n" + "=" * 60)
    print("Validation complete!")
    print("=" * 60)
    print("\nExpected behavior:")
    print("- All memory values should be positive")
    print("- Peak memory should increase with allocation size")
    print("- Memory increase should be approximately equal to allocation size")
    print("- Average memory should be between baseline and peak")
    print("- End memory shows what's retained after function completes")
    print("- Growth rate should appear for long-running functions (>10 samples)")
    print("  - Growth rate > 1.0 MB/s indicates potential memory leak (⚠️)")
    print("  - Growth rate <= 1.0 MB/s is normal (✓)")


if __name__ == "__main__":
    main()
