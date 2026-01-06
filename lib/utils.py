"""Utility functions."""

import pandas as pd
import unicodedata

MIN_POST_TEXT_LENGTH = 5


def check_if_string_is_empty(string: str) -> bool:
    """Check if a string is empty.

    This function checks for various forms of empty strings:
    - None value
    - Empty string ("")
    - String with only whitespace (including Unicode whitespace)
    - Pandas NA/null values
    - Strings that are "null", "none", "nan" (case-insensitive) after stripping whitespace
    - Zero-width spaces and other invisible Unicode characters

    Args:
        string: The string to check

    Returns:
        bool: True if the string is considered empty, False otherwise
    """
    if string is None:
        return True

    # Handle pandas null types - use explicit size check to avoid warning
    if pd.api.types.is_scalar(string):
        if pd.isna(string):
            return True

    if not isinstance(string, str):
        raise AttributeError("Input must be a string")

    # Check for empty string cases and whitespace
    if string == "" or string.isspace():
        return True

    # Convert to string and normalize Unicode
    string = unicodedata.normalize("NFKC", string).lower().strip()

    # Check for common null string representations
    null_values = {"null", "none", "nan"}
    if string in null_values:
        return True

    # Check for invisible Unicode characters including zero-width spaces
    invisible_chars = {"\u200b", "\u200c", "\u200d", "\ufeff", "\u2800"}
    if all(
        char in invisible_chars
        or unicodedata.category(char).startswith("Z")
        or char.isspace()
        for char in string
    ):
        return True

    return False


def filter_posts_df(df: pd.DataFrame, strict: bool = False) -> pd.DataFrame:
    """Filter out posts with invalid text.

    Args:
        df: DataFrame containing posts with 'text' column and 'preprocessing_timestamp' column
        strict: If True, use check_if_string_is_empty with strict mode for additional filtering

    Returns:
        DataFrame with posts removed that have:
        - Missing text (if text column exists)
        - Empty text (if text column exists)
        - Text shorter than MIN_POST_TEXT_LENGTH characters after stripping whitespace
        - Missing preprocessing_timestamp timestamp (if preprocessing_timestamp column exists)
        - Text that represents null values ("null", "none", "nan") after stripping whitespace
    """
    filtered_df = df.copy()

    # Only apply text filters if text column exists
    if "text" in filtered_df.columns:
        # Basic filtering - remove None, empty strings, short text, and null-like strings
        base_mask = (
            filtered_df["text"].notna()
            & (filtered_df["text"].astype(str) != "")
            & (filtered_df["text"].str.strip().str.len() >= MIN_POST_TEXT_LENGTH)
            & ~filtered_df["text"]
            .astype(str)
            .str.strip()
            .str.lower()
            .isin({"null", "none", "nan"})
        )

        # When strict, also filter out strings that are "empty" after Unicode normalization,
        # including zero-width spaces and other invisible characters.
        if strict:
            base_mask &= ~filtered_df["text"].apply(check_if_string_is_empty)

        filtered_df = filtered_df[base_mask]

    # Only apply timestamp filter if preprocessing_timestamp column exists
    if "preprocessing_timestamp" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["preprocessing_timestamp"].notna()]

    return filtered_df
