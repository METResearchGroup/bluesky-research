import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from services.backfill.sync.constants import valid_types

if __name__ == "__main__":
    service = "study_user_activity"
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
