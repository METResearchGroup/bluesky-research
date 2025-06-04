import pandas as pd
from typing import Any, Dict, List
import matplotlib.pyplot as plt
import tempfile


def compose_answer(
    summarizer_output: Dict[str, Any], df: pd.DataFrame
) -> Dict[str, Any]:
    """
    Compose the final answer by generating plots and formatting the output.
    Args:
        summarizer_output: Dict with 'text' and 'graph' keys.
        df: DataFrame of posts.
    Returns:
        Dict with 'text', 'df', and 'visuals' (list of {'type', 'path'}).
    """
    visuals: List[Dict[str, str]] = []
    # Ensure partition_date is datetime for plotting
    # TODO: later on, I need to make this dynamic and based on how each service
    # is partitioned.
    if "partition_date" not in df.columns:
        df["partition_date"] = pd.to_datetime(
            # could also be preprocessing_timestamp
            # This is a known problem, where I didn't fix the timestamp field
            # on backfills. It's fixed in prod but not in dev.
            df["synctimestamp"]
        ).dt.date
    if "partition_date" in df.columns and df["partition_date"].dtype == "object":
        df["partition_date"] = pd.to_datetime(df["partition_date"])
    # Prepare grouped data for plotting
    grouped_df = df.groupby("partition_date").size().reset_index(name="counts")
    for graph in summarizer_output.get("graph", []):
        plot_type = graph.get("type")
        try:
            plt.figure(figsize=(10, 5))
            if plot_type == "line":
                plt.plot(grouped_df["partition_date"], grouped_df["counts"])
            elif plot_type == "bar":
                plt.bar(grouped_df["partition_date"], grouped_df["counts"])
            plt.xlabel("Date")
            plt.ylabel("Date")
            plt.title(f"{plot_type.capitalize()} Plot: Count of Rows per Date")
            plt.xticks(rotation=45)
            plt.tight_layout()
            # Save to temp file
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=".png", prefix=f"{plot_type}_", dir=None
            ) as tmpfile:
                plt.savefig(tmpfile.name)
                visuals.append({"type": plot_type, "path": tmpfile.name})
            plt.close()
        except Exception as e:
            visuals.append({"type": plot_type, "path": f"error: {e}"})
    # Convert all datetime columns to string for JSON serialization
    df_serializable = df.copy()
    for col in df_serializable.columns:
        if pd.api.types.is_datetime64_any_dtype(df_serializable[col]):
            df_serializable[col] = df_serializable[col].dt.strftime("%Y-%m-%d")
    return {
        "text": summarizer_output.get("text", ""),
        "df": df_serializable.head(10).to_dict(orient="records"),
        "visuals": visuals,
    }
