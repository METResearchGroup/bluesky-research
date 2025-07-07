#!/usr/bin/env python3
"""
Script to analyze CSV data for mirror text analysis.

This script:
1. Loads the compressed CSV file
2. Examines available columns
3. Prints first 10 rows of "mirror text" column (if it exists)
4. Calculates 5-number summary of text lengths in "mirror text" column
"""

import pandas as pd
import numpy as np
import gzip
import os
from typing import Optional


def load_compressed_csv(file_path: str) -> pd.DataFrame:
    """Load a compressed CSV file."""
    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            df = pd.read_csv(f)
        return df
    except Exception as e:
        print(f"Error loading compressed CSV: {e}")
        return None


def find_text_columns(df: pd.DataFrame) -> list[str]:
    """Find columns that might contain text data."""
    text_columns = []
    for col in df.columns:
        if 'text' in col.lower() or 'mirror' in col.lower():
            text_columns.append(col)
    return text_columns


def calculate_five_number_summary(lengths: pd.Series) -> dict:
    """Calculate the 5-number summary (min, Q1, median, Q3, max)."""
    return {
        'Min': lengths.min(),
        'Q1': lengths.quantile(0.25),
        'Median': lengths.median(),
        'Q3': lengths.quantile(0.75),
        'Max': lengths.max()
    }


def main():
    # Path to the compressed CSV file
    csv_file_path = "demos/2024-05-30-analyze-pilot-data/pilot_data_2024-05-30-12:11:48.csv.gz"
    
    print("Loading compressed CSV file...")
    df = load_compressed_csv(csv_file_path)
    
    if df is None:
        print("Failed to load CSV file.")
        return
    
    print(f"CSV loaded successfully. Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print()
    
    # Look for "mirror text" column specifically
    mirror_text_col = None
    for col in df.columns:
        if 'mirror text' in col.lower():
            mirror_text_col = col
            break
    
    if mirror_text_col:
        print(f"Found 'mirror text' column: {mirror_text_col}")
    else:
        print("No 'mirror text' column found.")
        
        # Look for other text columns
        text_columns = find_text_columns(df)
        if text_columns:
            print(f"Available text columns: {text_columns}")
            print("Using the first text column found for analysis:")
            mirror_text_col = text_columns[0]
        else:
            print("No text columns found. Showing all available columns:")
            print(df.columns.tolist())
            return
    
    print(f"\nAnalyzing column: '{mirror_text_col}'")
    print("=" * 50)
    
    # Check for null values
    null_count = df[mirror_text_col].isnull().sum()
    print(f"Null values in '{mirror_text_col}': {null_count}")
    
    # Get non-null text data
    text_data = df[mirror_text_col].dropna()
    
    if len(text_data) == 0:
        print(f"No non-null data found in '{mirror_text_col}' column.")
        return
    
    print(f"Non-null entries: {len(text_data)}")
    print()
    
    # 1. Print first 10 rows of "mirror text"
    print("1. First 10 rows of 'mirror text':")
    print("-" * 30)
    first_10 = text_data.head(10)
    for i, text in enumerate(first_10, 1):
        print(f"{i:2d}. {text}")
        if i < len(first_10):
            print()
    
    print("\n" + "=" * 50)
    
    # 2. Calculate text lengths and 5-number summary
    print("2. 5-Number Summary of Text Lengths:")
    print("-" * 35)
    
    text_lengths = text_data.astype(str).str.len()
    five_num_summary = calculate_five_number_summary(text_lengths)
    
    print(f"{'Statistic':<10} {'Value':<10}")
    print("-" * 20)
    for stat, value in five_num_summary.items():
        print(f"{stat:<10} {value:<10}")
    
    print(f"\nMean length: {text_lengths.mean():.2f}")
    print(f"Standard deviation: {text_lengths.std():.2f}")
    print(f"Total characters: {text_lengths.sum():,}")


if __name__ == "__main__":
    main()