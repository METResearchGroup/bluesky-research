"""Script to verify SLURM run logs.

This script checks the latest log files for different components of the backfill
coordination pipeline. It can display logs for:
- Backfill posts used in feeds
- ML inference IME
- ML inference sociopolitical
- ML inference perspective API

Usage:
    # Show single component log
    python verify_slurm_run.py -b  # Show backfill posts log
    python verify_slurm_run.py -i  # Show IME inference log
    python verify_slurm_run.py -s  # Show sociopolitical inference log
    python verify_slurm_run.py -p  # Show perspective API inference log

    # Show multiple component logs
    python verify_slurm_run.py -b -i  # Show both backfill and IME logs
    python verify_slurm_run.py -i -s -p  # Show all inference logs

For each log file, displays:
    [Filename]: <filename>

    [Head]
    <first 100 lines>

    [Tail]
    <last 100 lines>
"""

import os
import glob
from typing import Optional

import click

from lib.constants import project_home_directory


def get_latest_log_file(prefix: str) -> Optional[str]:
    """Get the most recent log file with given prefix.

    Args:
        prefix: The prefix to search for in filenames

    Returns:
        str: Path to the most recent log file, or None if no files found
    """
    log_dir = os.path.join(
        project_home_directory, "lib/log/backfill_records_coordination"
    )
    pattern = os.path.join(log_dir, f"{prefix}*.log")
    files = glob.glob(pattern)

    if not files:
        return None

    # Sort by modification time, newest first
    return max(files, key=os.path.getmtime)


def display_log_file(filepath: str) -> None:
    """Display the head and tail of a log file.

    Args:
        filepath: Path to the log file to display
    """
    if not filepath:
        print("No log file found")
        return

    filename = os.path.basename(filepath)

    try:
        with open(filepath, "r") as f:
            lines = f.readlines()

        print(f"\n[Filename]: {filename}\n")

        print("[Head]")
        head = lines[:100]
        print("".join(head))

        print("\n[Tail]")
        tail = lines[-100:] if len(lines) > 100 else []
        print("".join(tail))

    except Exception as e:
        print(f"Error reading file {filepath}: {str(e)}")


@click.command()
@click.option("-b", is_flag=True, help="Show backfill posts used in feeds log")
@click.option("-i", is_flag=True, help="Show ML inference IME log")
@click.option("-s", is_flag=True, help="Show ML inference sociopolitical log")
@click.option("-p", is_flag=True, help="Show ML inference perspective API log")
def main(b: bool, i: bool, s: bool, p: bool):
    """Display latest log files for selected components."""
    if not any([b, i, s, p]):
        click.echo("Please specify at least one component to check (-b, -i, -s, or -p)")
        return

    # Map flags to file prefixes
    components = {
        "b": "backfill_posts_used_in_feeds",
        "i": "ml_inference_ime",
        "s": "ml_inference_sociopolitical",
        "p": "ml_inference_perspective_api",
    }

    # Process each selected component
    for flag, prefix in components.items():
        if locals()[flag]:  # Check if the flag was set
            latest_file = get_latest_log_file(prefix)
            if latest_file:
                display_log_file(latest_file)
            else:
                click.echo(f"\nNo log files found for {prefix}")


if __name__ == "__main__":
    main()
