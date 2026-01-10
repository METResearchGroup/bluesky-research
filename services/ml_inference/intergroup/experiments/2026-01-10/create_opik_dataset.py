"""Given our eval dataset, create a dataset for Opik."""
import os

import opik
import pandas as pd

from lib.opik import OpikClient

from constants import opik_project_name

dataset_filename = "intergroup_eval_dataset.csv"
dataset_fp = os.path.join(os.path.dirname(__file__), dataset_filename)
opik_dataset_name = "intergroup_eval_dataset_2026-01-10"

def insert_rows_into_opik_dataset(df: pd.DataFrame, dataset: opik.Dataset):
    """Insert rows into Opik dataset.
    
    Opik automatically manages deduplication of rows, so this operation is
    idempotent. See https://www.comet.com/docs/opik/evaluation/manage_datasets
    for more information.
    """
    df_dicts: list[dict] = df.to_dict(orient="records")
    records_to_insert = []
    for record in df_dicts:
        record_to_insert = {
            "input": record["text"],
            "expected_output": record["gold_label"],
        }
        records_to_insert.append(record_to_insert)
    dataset.insert(records_to_insert)

if __name__ == "__main__":
    df = pd.read_csv(dataset_fp)
    opik_client = OpikClient(project_name=opik_project_name)
    dataset = opik_client.get_or_create_dataset(opik_dataset_name)
    insert_rows_into_opik_dataset(df, dataset)
    print(f"Inserted {len(df)} rows into Opik dataset.")
