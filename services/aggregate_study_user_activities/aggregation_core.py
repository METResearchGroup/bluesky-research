"""Helper functions for assembling study user activity DataFrames (no I/O)."""

from __future__ import annotations

import json
from typing import Any, Sequence

import numpy as np
import pandas as pd


def copy_dtypes_without_partition(dtypes_map: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of dtypes_map without partition_date; never mutates input."""
    out = dict(dtypes_map)
    out.pop("partition_date", None)
    return out


def json_object_per_row(df: pd.DataFrame, columns: Sequence[str]) -> pd.Series:
    """One JSON object per row with stable key order; missing values become JSON null."""

    def row_to_json(row: pd.Series) -> str:
        obj: dict[str, Any] = {}
        for col in columns:
            val = row[col]
            obj[col] = None if pd.isna(val) else val  # type: ignore
        return json.dumps(obj)

    return df.apply(row_to_json, axis=1)  # type: ignore


def finalize_activity_df(
    df: pd.DataFrame,
    author_did: object,
    author_handle: object,
    data_type: object,
    data: object,
    activity_timestamp: object,
    cols: Sequence[str],
) -> pd.DataFrame:
    """Assign canonical activity columns and project to cols in order."""
    result = df.copy()
    result["author_did"] = author_did
    result["author_handle"] = author_handle
    result["data_type"] = data_type
    result["data"] = data
    result["activity_timestamp"] = activity_timestamp
    col_list = list(cols)
    return result.loc[:, col_list]


def author_did_series_for_follows(df: pd.DataFrame) -> pd.Series:
    """study_user who performed the follow action (same logic as prior row.apply)."""
    is_follower = df["relationship_to_study_user"] == "follower"
    return pd.Series(
        np.where(is_follower, df["follow_did"], df["follower_did"]),
        index=df.index,
    )
