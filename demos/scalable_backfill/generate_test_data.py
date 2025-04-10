#!/usr/bin/env python3
"""
Test data generator for the scalable backfill system.

This script:
1. Generates a file with synthetic DIDs for testing
2. Can create custom distributions to simulate different scenarios
"""

import argparse
import os
import random
import string
from typing import List, Optional

import matplotlib.pyplot as plt
import numpy as np


def generate_random_did() -> str:
    """Generate a random DID.

    Returns:
        A random DID in the format did:plc:xxxxxxxxxxxxxxxxxxxxxx
    """
    return f"did:plc:{''.join(random.choices(string.ascii_lowercase + string.digits, k=24))}"


def generate_dids(count: int, seed: Optional[int] = None) -> List[str]:
    """Generate a list of DIDs.

    Args:
        count: Number of DIDs to generate
        seed: Random seed for reproducibility

    Returns:
        List of DIDs
    """
    if seed is not None:
        random.seed(seed)
        
    return [generate_random_did() for _ in range(count)]


def save_dids_to_file(dids: List[str], filename: str) -> None:
    """Save DIDs to a file.

    Args:
        dids: List of DIDs to save
        filename: Name of the file to save DIDs to
    """
    os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)
    
    with open(filename, "w") as f:
        for did in dids:
            f.write(f"{did}\n")


def visualize_distribution(dids: List[str], output_file: Optional[str] = None) -> None:
    """Visualize the distribution of DIDs.

    Args:
        dids: List of DIDs to visualize
        output_file: Name of the file to save the visualization to
    """
    # For DIDs, we can visualize the character distribution
    first_chars = [did.split(":")[-1][0] for did in dids]
    unique_chars = sorted(set(first_chars))
    char_counts = [first_chars.count(c) for c in unique_chars]
    
    plt.figure(figsize=(10, 6))
    plt.bar(unique_chars, char_counts)
    plt.title(f"Distribution of {len(dids)} DIDs")
    plt.xlabel("First character of DID value")
    plt.ylabel("Count")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    
    if output_file:
        plt.savefig(output_file)
        print(f"Saved visualization to {output_file}")
    else:
        plt.show()


def main() -> None:
    """Main function."""
    parser = argparse.ArgumentParser(description="Generate test DIDs")
    parser.add_argument(
        "--count",
        type=int,
        default=1000,
        help="Number of DIDs to generate",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="test_dids.txt",
        help="Output file name",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility",
    )
    parser.add_argument(
        "--visualize",
        action="store_true",
        help="Visualize the distribution of DIDs",
    )
    parser.add_argument(
        "--viz-output",
        type=str,
        default=None,
        help="Output file for visualization",
    )
    
    args = parser.parse_args()
    
    print(f"Generating {args.count} DIDs...")
    dids = generate_dids(args.count, args.seed)
    save_dids_to_file(dids, args.output)
    print(f"Saved {args.count} DIDs to {args.output}")
    
    if args.visualize:
        visualize_distribution(dids, args.viz_output)
    
    print(f"Sample DIDs:")
    for i in range(min(5, len(dids))):
        print(f"  {dids[i]}")


if __name__ == "__main__":
    main() 