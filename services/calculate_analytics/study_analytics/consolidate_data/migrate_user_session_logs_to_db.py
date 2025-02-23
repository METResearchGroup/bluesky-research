"""One-off script to migrate current user_session_logs to .parquet + DB format.

Once the user_session_logs are in their correct location + format, we can start
using the new service to fetch the posts used in feeds.

Reads from the consolidated user_session_logs.parquet file and exports to the
generated_user_session_logs service.
"""

import os

import pandas as pd

from lib.db.manage_local_data import export_data_to_local_storage
from services.calculate_analytics.study_analytics.constants import (
    consolidated_data_root_path,
)


# previously consolidated version of the user_session_logs.
root_user_session_logs_path = os.path.join(
    consolidated_data_root_path,
    "user_session_logs",
    "consolidated_user_session_logs.parquet",
)


def main():
    user_session_logs_df: pd.DataFrame = pd.read_parquet(root_user_session_logs_path)
    print(
        f"Loaded {len(user_session_logs_df)} user_session_logs from {root_user_session_logs_path}"
    )

    # export to store. Partition by feed_generation_timestamp.
    user_session_logs_df["partition_date"] = pd.to_datetime(
        user_session_logs_df["timestamp"]
    ).dt.date

    grouped_user_session_logs = user_session_logs_df.groupby("partition_date")

    print("Exporting user_session_logs to generated_user_session_logs store...")
    for partition_date, grouped_df in grouped_user_session_logs:
        print(f"Exporting {len(grouped_df)} user_session_logs to {partition_date}")
        export_data_to_local_storage(
            service="generated_user_session_logs",
            df=grouped_df,
            export_format="parquet",
        )
    print("Finished exporting user_session_logs to generated_user_session_logs store.")


if __name__ == "__main__":
    main()
