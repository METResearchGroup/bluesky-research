import pandas as pd
from typing import Any, Dict
import matplotlib

matplotlib.use("Agg")


def summarize_router_results(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Summarize the router results by counting posts per partition_date.
    Returns a dict with a summary text and graph specs for line and bar plots.
    Args:
        df: DataFrame with at least a 'partition_date' column.
    Returns:
        Dict with 'text' and 'graph' keys.
    """
    # Group by partition_date and count
    # TODO: later on, I need to make this dynamic and based on how each service
    # is partitioned.
    if "partition_date" not in df.columns:
        df["partition_date"] = pd.to_datetime(
            # could also be preprocessing_timestamp
            # This is a known problem, where I didn't fix the timestamp field
            # on backfills. It's fixed in prod but not in dev.
            df["synctimestamp"]
        ).dt.date
    grouped = df.groupby("partition_date").size().reset_index(name="counts")
    # Build summary text
    lines = ["The total number of posts per day is:"]
    for _, row in grouped.iterrows():
        lines.append(f"{row['partition_date']}: {row['counts']}")
    summary_text = "\n".join(lines)
    # Build graph spec
    graph_spec = [
        {
            "type": "line",
            "kwargs": {
                "transform": {
                    "groupby": True,
                    "groupby_col": "partition_date",
                    "agg_func": "count",
                    "agg_col": "counts",
                },
                "graph": {
                    "col_x": "partition_date",
                    "xlabel": "Date",
                    "col_y": "counts",
                    "ylabel": "Date",
                },
            },
        },
        {
            "type": "bar",
            "kwargs": {
                "transform": {
                    "groupby": True,
                    "groupby_col": "partition_date",
                    "agg_func": "count",
                    "agg_col": "counts",
                },
                "graph": {
                    "col_x": "partition_date",
                    "xlabel": "Date",
                    "col_y": "counts",
                    "ylabel": "Date",
                },
            },
        },
    ]
    return {"text": summary_text, "graph": graph_spec}
