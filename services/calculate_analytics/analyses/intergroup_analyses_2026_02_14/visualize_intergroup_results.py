"""
Visualize per-week average feed_proportion_intergroup by condition.

Reads per_user_per_day_average_prop_intergroup.csv, assigns week via
calculate_week_number_for_date, aggregates to per-week mean by condition,
and plots a line chart (x=weeks, y=mean, one line per condition).
"""

from __future__ import annotations

import os

import pandas as pd

from lib.datetime_utils import calculate_week_number_for_date
from services.calculate_analytics.shared.constants import (
    STUDY_END_DATE,
    STUDY_START_DATE,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV = os.path.join(
    BASE_DIR,
    "per_user_per_day_average_prop_intergroup.csv",
)


def main() -> None:
    df = pd.read_csv(INPUT_CSV)
    # Drop rows without a feed_proportion_intergroup (expanded grid may have NaNs)
    df = df.dropna(subset=["feed_proportion_intergroup"])
    # Restrict to study date range so week calculation is valid
    df = df[(df["date"] >= STUDY_START_DATE) & (df["date"] <= STUDY_END_DATE)]

    def week_for_date(date: str) -> int:
        return calculate_week_number_for_date(
            date=date,
            study_start_date=STUDY_START_DATE,
            study_end_date=STUDY_END_DATE,
        )

    df["week"] = df["date"].astype(str).map(week_for_date)

    # Per-week average by condition: rows = weeks, columns = conditions
    per_week = (
        df.groupby(["week", "condition"], as_index=False)["feed_proportion_intergroup"]
        .mean()
        .pivot(index="week", columns="condition", values="feed_proportion_intergroup")
    )
    per_week = per_week.sort_index()

    # Line chart: x = weeks, y = average, color by condition
    ax = per_week.plot(
        kind="line",
        xlabel="Week",
        ylabel="Average feed proportion intergroup",
        title="Per-week average feed proportion intergroup by condition",
        marker="o",
        figsize=(10, 6),
    )
    ax.legend(title="Condition", bbox_to_anchor=(1.02, 1), loc="upper left")
    ax.figure.tight_layout()
    out_path = os.path.join(BASE_DIR, "intergroup_per_week_by_condition.png")
    ax.figure.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved plot to {out_path}")
    print("\nPer-week table (weeks × conditions):")
    print(per_week.to_string())


if __name__ == "__main__":
    main()
