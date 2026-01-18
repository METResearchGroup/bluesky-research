"""Inserts consolidated enriched posts into DuckDB, by partition date.

Done as an alternative to writing to DuckDB since jobs keep crashing for some
reason.
"""

import gc

from lib.db.manage_local_data import load_data_from_local_storage
from scripts.analytics.duckdb_helper import insert_df_to_duckdb
from scripts.analytics.helper import get_partition_dates

excluded_partition_dates = ["2024-10-09"]  # server outage


def main():
    total_count = 0
    partition_dates = get_partition_dates(
        start_date="2024-09-26", end_date="2024-12-01"
    )
    for current_partition_date in partition_dates:
        print(f"Processing partition date={current_partition_date}")
        if current_partition_date in excluded_partition_dates:
            continue
        consolidated_enriched_posts_df = load_data_from_local_storage(
            service="consolidated_enriched_post_records",
            storage_tiers=["cache"],
            partition_date=current_partition_date,
        )
        print(
            f"Loaded {len(consolidated_enriched_posts_df)} consolidated enriched posts for partition date={current_partition_date}"
        )
        if "partition_date" not in consolidated_enriched_posts_df.columns:
            consolidated_enriched_posts_df["partition_date"] = current_partition_date
        insert_df_to_duckdb(
            df=consolidated_enriched_posts_df, table_name="consolidated_enriched_posts"
        )
        total_count += len(consolidated_enriched_posts_df)
        del consolidated_enriched_posts_df
        gc.collect()
    print(f"Total count of consolidated posts: {total_count}")


if __name__ == "__main__":
    main()
