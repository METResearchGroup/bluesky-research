"""One-off script to migrate current feeds to .parquet + DB format.

Once the feeds are in their correct location + format, we can start
using the new service to fetch the posts used in feeds.

Reads from the consolidated feeds.parquet file and exports to the
generated_feeds service.
"""

import pandas as pd

from lib.db.manage_local_data import export_data_to_local_storage
from services.fetch_posts_used_in_feeds.constants import root_feeds_path


def main():
    feeds_df: pd.DataFrame = pd.read_parquet(root_feeds_path)
    print(f"Loaded {len(feeds_df)} feeds from {root_feeds_path}")

    # export to store. Partition by feed_generation_timestamp.
    feeds_df["partition_date"] = pd.to_datetime(
        feeds_df["feed_generation_timestamp"]
    ).dt.date

    grouped_feeds = feeds_df.groupby("partition_date")

    print("Exporting feeds to generated_feeds store...")
    for partition_date, grouped_df in grouped_feeds:
        print(f"Exporting {len(grouped_df)} feeds to {partition_date}")
        export_data_to_local_storage(
            service="generated_feeds", df=grouped_df, export_format="parquet"
        )
    print("Finished exporting feeds to generated_feeds store.")


if __name__ == "__main__":
    main()
