"""Utility functions."""

import pandas as pd

MIN_POST_TEXT_LENGTH = 5


def check_if_string_is_empty(string: str) -> bool:
    """Check if a string is empty.

    This function checks for various forms of empty strings:
    - None value
    - Empty string ("")
    - String with only whitespace
    - Pandas NA/null values
    - "null", "none", "nan" strings (case-insensitive)

    Args:
        string: The string to check

    Returns:
        bool: True if the string is considered empty, False otherwise
    """
    if string is None:
        return True

    # Handle pandas null types
    if pd.isna(string) or pd.isnull(string):
        return True

    string = string.lower().strip()

    # Check various empty string cases
    return (
        string == ""
        or string == "null"
        or string == "none"
        or string == "nan"
        or string.isspace()
    )


def filter_posts_df(df: pd.DataFrame, strict: bool = False) -> pd.DataFrame:
    """Filter out posts with invalid text.

    Args:
        df: DataFrame containing posts with 'text' column and 'preprocessing_timestamp' column

    Returns:
        DataFrame with posts removed that have:
        - Missing text (if text column exists)
        - Empty text (if text column exists)
        - Text shorter than MIN_POST_TEXT_LENGTH characters (if text column exists)
        - Missing preprocessing_timestamp timestamp (if preprocessing_timestamp column exists)
    """
    filtered_df = df.copy()

    # Only apply text filters if text column exists
    if "text" in filtered_df.columns:
        filtered_df = filtered_df[
            filtered_df["text"].notna()
            & (filtered_df["text"] != "")
            & (filtered_df["text"].str.len() >= MIN_POST_TEXT_LENGTH)
        ]
        if strict:
            filtered_df = filtered_df[
                filtered_df["text"].apply(check_if_string_is_empty)
            ]
    # Only apply timestamp filter if preprocessing_timestamp column exists
    if "preprocessing_timestamp" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["preprocessing_timestamp"].notna()]

    return filtered_df
