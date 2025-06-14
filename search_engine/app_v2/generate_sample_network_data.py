import pandas as pd
import random
from typing import Tuple
import os


def generate_sample_network_files() -> Tuple[str, str]:
    """
    Generate sample node and edge CSVs for the SNA demo.
    Returns:
        Tuple[str, str]: Paths to node and edge CSV files.
    """
    num_days = 14
    nodes_per_day = 100
    start_date = pd.Timestamp("2024-06-01")
    valences = ["positive", "neutral", "negative"]
    slants = ["left", "center", "right", "unclear"]
    node_rows = []
    edge_rows = []
    node_id = 1
    for day in range(num_days):
        date = (start_date + pd.Timedelta(days=day)).strftime("%Y-%m-%d")
        for i in range(nodes_per_day):
            valence = random.choices(valences, weights=[0.4, 0.3, 0.3])[0]
            toxic = (
                False
                if valence == "positive"
                else (
                    True if valence == "negative" and random.random() < 0.7 else False
                )
            )
            political = random.choice([True, False])
            slant = random.choice(slants) if political else "unclear"
            node_rows.append(
                {
                    "id": node_id,
                    "date": date,
                    "valence": valence,
                    "toxic": toxic,
                    "political": political,
                    "slant": slant,
                }
            )
            node_id += 1
    node_df = pd.DataFrame(node_rows)
    # Engagement bias: positive/low-toxicity get more edges
    for idx, row in node_df.iterrows():
        if row["valence"] == "positive" and not row["toxic"]:
            num_edges = random.randint(5, 10)
        elif row["valence"] == "negative" and row["toxic"]:
            num_edges = random.randint(0, 2)
        else:
            num_edges = random.randint(1, 4)
        for _ in range(num_edges):
            # Randomly connect to another node from the same day
            same_day_nodes = node_df[node_df["date"] == row["date"]]["id"].tolist()
            target = row["id"]
            while target == row["id"]:
                target = random.choice(same_day_nodes)
            edge_type = random.choice(["retweet", "reply", "like"])
            edge_rows.append(
                {
                    "source": row["id"],
                    "target": target,
                    "type": edge_type,
                    "date": row["date"],
                }
            )
    edge_df = pd.DataFrame(edge_rows)
    # Ensure correct types
    node_df = node_df.astype(
        {
            "id": int,
            "date": str,
            "valence": str,
            "toxic": bool,
            "political": bool,
            "slant": str,
        }
    )
    edge_df = edge_df.astype({"source": int, "target": int, "type": str, "date": str})
    # Write to CSV
    node_csv = os.path.join(os.path.dirname(__file__), "sample_nodes.csv")
    edge_csv = os.path.join(os.path.dirname(__file__), "sample_edges.csv")
    node_df.to_csv(node_csv, index=False)
    edge_df.to_csv(edge_csv, index=False)
    return node_csv, edge_csv


def load_sample_network_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load the generated sample node and edge CSVs as DataFrames for simulation and UI components.
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: (node_df, edge_df)
    """
    node_csv = os.path.join(os.path.dirname(__file__), "sample_nodes.csv")
    edge_csv = os.path.join(os.path.dirname(__file__), "sample_edges.csv")
    node_df = pd.read_csv(node_csv)
    edge_df = pd.read_csv(edge_csv)
    return node_df, edge_df
