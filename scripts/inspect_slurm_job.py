"""Script for inspecting Slurm jobs using sacct.

This script provides detailed information about Slurm jobs using the sacct command.
It shows resource usage, timing, memory consumption, and I/O statistics for jobs.
"""

import argparse
import subprocess
from typing import List, Optional

from lib.log.logger import get_logger

logger = get_logger(__file__)


def format_memory(memory_str: str) -> str:
    """Format memory string to be human readable.

    Args:
        memory_str: Raw memory string from sacct (e.g., "1234K", "5678M")

    Returns:
        Formatted string with appropriate units
    """
    if not memory_str or memory_str == "0":
        return "0B"

    try:
        # Extract number and unit
        number = float(memory_str[:-1])
        unit = memory_str[-1].upper()

        # Convert to bytes
        units = {"B": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
        bytes_val = number * units.get(unit, 1)

        # Format to most appropriate unit
        for unit in ["B", "K", "M", "G", "T"]:
            if bytes_val < 1024:
                return f"{bytes_val:.1f}{unit}"
            bytes_val /= 1024
        return f"{bytes_val:.1f}T"
    except (ValueError, IndexError):
        return memory_str


def run_sacct(job_id: str, format_fields: Optional[List[str]] = None) -> str:
    """Run sacct command and return output.

    Args:
        job_id: Slurm job ID to inspect
        format_fields: List of fields to display. If None, uses default comprehensive format.

    Returns:
        Command output as string
    """
    if format_fields is None:
        # Comprehensive default format showing key job metrics
        format_fields = [
            "JobID",
            "JobName",
            "State",
            "ExitCode",
            "Submit",
            "Start",
            "End",
            "Elapsed",
            "TimeLimit",
            "ReqMem",
            "MaxRSS",
            "MaxVMSize",
            "NNodes",
            "NCPUs",
            "TotalCPU",
            "UserCPU",
            "SystemCPU",
            "CPUTime",
            "AveCPU",
            "MaxDiskRead",
            "MaxDiskWrite",
            "MaxPages",
            "NTasks",
            "Partition",
            "QOS",
        ]

    # Build sacct command
    cmd = [
        "sacct",
        "-j",
        job_id,
        "--parsable2",  # Use | as delimiter
        "--allocations",  # Only show job allocations, not steps
        f"--format={','.join(format_fields)}",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running sacct: {e}")
        if e.stderr:
            logger.error(f"stderr: {e.stderr}")
        raise


def parse_sacct_output(output: str) -> dict:
    """Parse sacct output into a dictionary.

    Args:
        output: Raw output from sacct command

    Returns:
        Dictionary containing job information
    """
    lines = output.strip().split("\n")
    if len(lines) < 2:
        raise ValueError("Invalid sacct output - no data rows found")

    # Parse header and data
    headers = lines[0].split("|")
    values = lines[1].split("|")

    # Create dict of field:value pairs
    job_info = {}
    for header, value in zip(headers, values):
        # Format memory-related fields
        if any(x in header for x in ["Mem", "RSS", "VMSize"]):
            value = format_memory(value)
        job_info[header] = value

    return job_info


def print_job_info(job_info: dict) -> None:
    """Print job information in a formatted way.

    Args:
        job_info: Dictionary of job information
    """
    # Group fields by category for organized output
    categories = {
        "Job Details": ["JobID", "JobName", "State", "ExitCode", "Partition", "QOS"],
        "Time": ["Submit", "Start", "End", "Elapsed", "TimeLimit"],
        "Resources": ["NNodes", "NCPUs", "NTasks"],
        "CPU Usage": ["TotalCPU", "UserCPU", "SystemCPU", "CPUTime", "AveCPU"],
        "Memory": ["ReqMem", "MaxRSS", "MaxVMSize"],
        "I/O": ["MaxDiskRead", "MaxDiskWrite", "MaxPages"],
    }

    for category, fields in categories.items():
        print(f"\n{category}:")
        print("-" * len(category))
        for field in fields:
            if field in job_info:
                print(f"{field:12} : {job_info[field]}")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Inspect Slurm job details using sacct"
    )
    parser.add_argument("job_id", help="Slurm job ID to inspect")
    parser.add_argument(
        "--format", help="Comma-separated list of sacct format fields", type=str
    )

    args = parser.parse_args()

    try:
        format_fields = args.format.split(",") if args.format else None
        output = run_sacct(args.job_id, format_fields)
        job_info = parse_sacct_output(output)
        print_job_info(job_info)
    except Exception as e:
        logger.error(f"Failed to inspect job {args.job_id}: {e}")
        raise


if __name__ == "__main__":
    main()
