"""
Aggregate per-feed constructiveness averages to per-user-per-day averages.

Input (in this directory):
- per_feed_average_constructiveness.csv
  Columns include:
    - bluesky_handle
    - condition
    - feed_generation_timestamp (format: %Y-%m-%d-%H:%M:%S)
    - avg_prob_constructive (per-feed average over posts)

Output (written in this directory):
- per_user_per_day_average_constructiveness.csv
  Columns:
    - handle  (renamed from bluesky_handle)
    - condition
    - date    (partition date derived from feed_generation_timestamp)
    - feed_average_constructiveness (mean of per-feed averages for that user+day)
"""

from __future__ import annotations

import os

import pandas as pd

BASE_DIR = os.path.dirname(__file__)
INPUT_CSV = os.path.join(BASE_DIR, "per_feed_average_constructiveness.csv")
OUTPUT_CSV = os.path.join(BASE_DIR, "per_user_per_day_average_constructiveness.csv")


def main() -> None:
    try:
        # NOTE: these are top-level modules in this repo; you must run with the repo root
        # on PYTHONPATH (or with an editable install) for `lib.*` to import.
        from lib.constants import study_start_date, study_end_date
        from lib.datetime_utils import get_partition_dates
        from lib.helper import get_partition_date_from_timestamp
    except ModuleNotFoundError as e:  # pragma: no cover
        raise ModuleNotFoundError(
            "Could not import `lib.*`. Run from the repo root with one of:\n"
            "  - PYTHONPATH=/path/to/bluesky-research python "
            "services/calculate_analytics/analyses/constructive_moral_outrage_2026_01_09/"
            "calculate_per_user_per_day_average_constructiveness.py\n"
            "  - (from repo root) python -m "
            "services.calculate_analytics.analyses.constructive_moral_outrage_2026_01_09."
            "calculate_per_user_per_day_average_constructiveness\n"
        ) from e

    # Note: pandas' typing for `usecols` can be finicky; we keep it simple here.
    df = pd.read_csv(INPUT_CSV)
    df = df.loc[
        :,
        [
            "bluesky_handle",
            "condition",
            "feed_generation_timestamp",
            "avg_prob_constructive",
        ],
    ].copy()

    required_cols = {
        "bluesky_handle",
        "condition",
        "feed_generation_timestamp",
        "avg_prob_constructive",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {INPUT_CSV}: {sorted(missing)}")

    # Derive partition date from timestamp using the project's canonical helper.
    # (We do this in Python because Athena date parsing formats can drift; this keeps
    # parity with local pipeline utilities.)
    ts = df["feed_generation_timestamp"].astype(str)
    df["date"] = ts.map(get_partition_date_from_timestamp)

    # Filter to study date range (inclusive).
    # study_start_date / study_end_date are YYYY-MM-DD strings.
    df = df[(df["date"] >= study_start_date) & (df["date"] <= study_end_date)].copy()

    # Filter out placeholder handle.
    df = df[df["bluesky_handle"] != "default"].copy()

    per_user_per_day = (
        df.groupby(
            ["bluesky_handle", "condition", "date"], as_index=False, dropna=False
        )
        .agg(feed_average_constructiveness=("avg_prob_constructive", "mean"))
        .rename(columns={"bluesky_handle": "handle"})
    )

    # Ensure every date in the study window is present for each (handle, condition).
    # NOTE: get_partition_dates() excludes some dates by default; pass an empty list
    # so we truly include *all* dates in [study_start_date, study_end_date].
    all_dates = get_partition_dates(
        start_date=study_start_date,
        end_date=study_end_date,
        exclude_partition_dates=[],
    )
    dates_df = pd.DataFrame({"date": all_dates})

    handle_condition_df = per_user_per_day[["handle", "condition"]].drop_duplicates()

    # Cross join (portable across pandas versions).
    handle_condition_df["__key"] = 1
    dates_df["__key"] = 1
    dense_index = handle_condition_df.merge(dates_df, on="__key").drop(columns="__key")

    dense = dense_index.merge(
        per_user_per_day,
        on=["handle", "condition", "date"],
        how="left",
        validate="one_to_one",
    ).sort_values(["handle", "date", "condition"], kind="stable")

    dense.to_csv(OUTPUT_CSV, index=False)
    print(
        f"Wrote {len(dense):,} rows to {OUTPUT_CSV} "
        f"({len(handle_condition_df):,} handle-condition pairs x {len(all_dates)} dates)"
    )


if __name__ == "__main__":
    main()
