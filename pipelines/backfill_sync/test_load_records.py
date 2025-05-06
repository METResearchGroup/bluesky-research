import json
from pprint import pprint

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from services.backfill.core.constants import valid_types


def get_total_liked_records(likes: pd.DataFrame) -> int:
    return likes["subject"].apply(json.loads).apply(lambda x: x["uri"]).nunique()


if __name__ == "__main__":
    service = "raw_sync"
    record_to_total_size = {}
    for record_type in valid_types:
        # if record_type != "post":
        #     continue
        custom_args = {"record_type": record_type}
        print(f"Loading {record_type} data...")
        active_df = load_data_from_local_storage(
            service=service,
            directory="active",
            custom_args=custom_args,
        )
        cache_df = load_data_from_local_storage(
            service=service,
            directory="cache",
            custom_args=custom_args,
        )
        df = pd.concat([active_df, cache_df])
        print(
            f"Loaded {len(df)} records for {record_type} ({len(active_df)} active, {len(cache_df)} cache)."
        )
        print(f"Data types: {df.dtypes}")
        record_to_total_size[record_type] = len(df)
        example_records = df.head(5).to_dict(orient="records")
        print(f"Example records for {record_type}:")
        pprint(example_records)
        print("-" * 20)
        if record_type == "like":
            num_liked_records = get_total_liked_records(df)
            record_to_total_size["total_liked_records"] = num_liked_records
    pprint(record_to_total_size)
