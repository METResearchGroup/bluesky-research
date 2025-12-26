"""Utilities for processing pandas DataFrames and dictionaries."""

import pandas as pd


def remove_timestamp_partition_fields(dicts: list[dict]) -> list[dict]:
    """Remove fields related to timestamp partitions."""
    fields_to_remove = [
        "year",
        "month",
        "day",
        "hour",
        "minute",
    ]  # extra fields from preprocessing partitions.
    return [
        {k: v for k, v in post.items() if k not in fields_to_remove} for post in dicts
    ]


def convert_nan_to_none(dicts: list[dict]) -> list[dict]:
    """Convert NaN values to None."""
    return [{k: (None if pd.isna(v) else v) for k, v in post.items()} for post in dicts]


def parse_converted_pandas_dicts(dicts: list[dict]) -> list[dict]:
    """Parse converted pandas dicts by removing partition fields and converting NaN to None."""
    df_dicts = remove_timestamp_partition_fields(dicts)
    df_dicts = convert_nan_to_none(df_dicts)
    return df_dicts
