"""Normally, we use preprocessing_timestamp as a measure of when a post was
available for the pipeline. However, this only works during the normal production
runs. During backfills, this is no longer valid. We might do the backfill for
posts at a much later time than when they should've been available had they
been available in normal pipeline runs.

So, we need to use the sync timestamp as a measure of when a post was available
for the pipeline. To do so, the easiest way would be to just impute the
synctimestamp in place of the preprocessing timestamp.

We will make a more permanent solution for this (using the synctimestamp in
lieu of the current timestamp when creating the preprocessing timestamp during
backfills) in the future. For now, we need to impute the data in old records.

The steps include:
- Load all posts in `raw_sync`. This is currently where our backfilled sync
records are stored (we haven't enabled firehose again so the data in `raw_sync`
is only the backfilled records).
- Load all the `preprocess_raw_data` records from `2025-05-01` and beyond.
- Load all the `ml_inference_{integration}` records from `2025-05-01` and beyond.
- Start trueing these up:
    - True up the `preprocess_raw_data` df:
        - Create a map of URI to synctimestamp, based on the `raw_sync` records.
        - Loop through all the `preprocess_raw_data` records. Create a new `new_timestamp` column with `df["new_timestamp"] = df["uri"].map(map_uri_to_synctimestamp)`.
        - Re-assign the `preprocessing_timestamp` column: `df["preprocessing_timestamp"] = df["new_timestamp"]`.
"""

import pandas as pd

from lib.db.manage_local_data import (
    load_data_from_local_storage,
    delete_records_for_service,
)

# the synctimestamps of the posts that were synced. We synced posts
# within this date range.
sync_start_date = "2024-09-01"
sync_end_date = "2024-12-01"

# dates when the posts were preprocessed. These are the timestamps we
# ended up assigning, which we want to replace with the synctimestamp.
preprocessing_start_date = "2025-05-01"
preprocessing_end_date = "2025-05-13"


def load_raw_sync_records(record_type: str) -> pd.DataFrame:
    custom_args = {"record_type": record_type}
    service = "raw_sync"
    active_df = load_data_from_local_storage(
        service=service,
        directory="active",
        custom_args=custom_args,
        start_partition_date=sync_start_date,
        end_partition_date=sync_end_date,
    )
    cached_df = load_data_from_local_storage(
        service=service,
        directory="cache",
        custom_args=custom_args,
        start_partition_date=sync_start_date,
        end_partition_date=sync_end_date,
    )
    df = pd.concat([active_df, cached_df])
    df = df.drop_duplicates(subset=["uri"])
    return df


def load_raw_sync_posts():
    record_types = ["post", "reply"]
    dfs = [load_raw_sync_records(record_type) for record_type in record_types]
    df = pd.concat(dfs)
    df = df.drop_duplicates(subset=["uri"])
    return df


def load_preprocessed_posts():
    service = "preprocess_raw_data"
    active_df = load_data_from_local_storage(
        service=service,
        directory="active",
        start_partition_date=preprocessing_start_date,
        end_partition_date=preprocessing_end_date,
    )
    cached_df = load_data_from_local_storage(
        service=service,
        directory="cache",
        start_partition_date=preprocessing_start_date,
        end_partition_date=preprocessing_end_date,
    )
    df = pd.concat([active_df, cached_df])
    df = df.drop_duplicates(subset=["uri"])
    return df


def load_integration(service: str) -> pd.DataFrame:
    active_df = load_data_from_local_storage(
        service=service,
        directory="active",
        start_partition_date=preprocessing_start_date,
        end_partition_date=preprocessing_end_date,
    )
    cached_df = load_data_from_local_storage(
        service=service,
        directory="cache",
        start_partition_date=preprocessing_start_date,
        end_partition_date=preprocessing_end_date,
    )
    df = pd.concat([active_df, cached_df])
    df = df.drop_duplicates(subset=["uri"])
    return df


def load_integrations() -> dict[str, pd.DataFrame]:
    integration_to_df: dict[str, pd.DataFrame] = {}
    for integration in [
        "ml_inference_perspective_api",
        "ml_inference_sociopolitical",
        "ml_inference_ime",
        "ml_inference_valence_classifier",
    ]:
        df = load_integration(integration)
        integration_to_df[integration] = df
    return integration_to_df


def create_uri_to_synctimestamp_map(df: pd.DataFrame) -> dict:
    return dict(zip(df["uri"], df["synctimestamp"]))


def main():
    raw_posts: pd.DataFrame = load_raw_sync_posts()
    preprocessed_posts: pd.DataFrame = load_preprocessed_posts()
    integrations_map: dict[str, pd.DataFrame] = load_integrations()
    uri_to_synctimestamp_map: dict = create_uri_to_synctimestamp_map(raw_posts)

    # replace the preprocessed posts timestamps.
    print("Replacing preprocessed posts timestamps...")
    preprocessed_posts["new_timestamp"] = preprocessed_posts["uri"].map(
        uri_to_synctimestamp_map
    )

    print(
        f"Before replacement, the min timestamp was: {min(preprocessed_posts['preprocessing_timestamp'])}\t max timestamp was: {max(preprocessed_posts['preprocessing_timestamp'])}"
    )

    preprocessed_posts["preprocessing_timestamp"] = preprocessed_posts["new_timestamp"]

    print(
        f"After replacement, the min timestamp is: {min(preprocessed_posts['preprocessing_timestamp'])}\t max timestamp is: {max(preprocessed_posts['preprocessing_timestamp'])}"
    )

    skipped_integrations = []

    # replace the integrations timestamps.
    for integration, df in integrations_map.items():
        if len(df) == 0:
            print(f"No records to replace for {integration}. Skipping...")
            skipped_integrations.append(integration)
            continue
        df["new_timestamp"] = df["uri"].map(uri_to_synctimestamp_map)
        print(
            f"Before replacement, the min timestamp for {integration} was: {min(df['preprocessing_timestamp'])}\t max timestamp was: {max(df['preprocessing_timestamp'])}"
        )
        print(
            f"Number of posts with no synctimestamp to impute, for integration={integration}: {sum(pd.isna(df['new_timestamp']))}"
        )
        # Only update preprocessing_timestamp if new_timestamp is not NaN. We might
        # not have the raw sync record for some reason, in which case we'll just
        # keep the old timestamp.
        df["preprocessing_timestamp"] = df.apply(
            lambda row: row["new_timestamp"]
            if pd.notna(row["new_timestamp"])
            else row["preprocessing_timestamp"],
            axis=1,
        )
        print(
            f"After replacement, the min timestamp for {integration} is: {min(df['preprocessing_timestamp'])}\t max timestamp is: {max(df['preprocessing_timestamp'])}"
        )

    # print("Exporting preprocess_raw_data...")

    # # export records.
    # export_data_to_local_storage(service="preprocess_raw_data", df=preprocessed_posts)

    # print("Exporting integrations...")
    # for integration, df in integrations_map.items():
    #     if integration in skipped_integrations:
    #         print(f"Skipping {integration} because it has no records to replace.")
    #         continue
    #     export_data_to_local_storage(service=integration, df=df)

    print("Finished exporting updated records. Now deleting old records.")

    # delete old records.
    delete_records_for_service(
        service="preprocess_raw_data",
        directory="active",
        start_partition_date=preprocessing_start_date,
        end_partition_date=preprocessing_end_date,
    )
    delete_records_for_service(
        service="preprocess_raw_data",
        directory="cache",
        start_partition_date=preprocessing_start_date,
        end_partition_date=preprocessing_end_date,
    )

    for integration in integrations_map.keys():
        if integration in skipped_integrations:
            print(
                f"Skipping deletion of records from {integration} because it has no records to replace."
            )
            continue
        delete_records_for_service(
            service=integration,
            directory="active",
            start_partition_date=preprocessing_start_date,
            end_partition_date=preprocessing_end_date,
        )
        delete_records_for_service(
            service=integration,
            directory="cache",
            start_partition_date=preprocessing_start_date,
            end_partition_date=preprocessing_end_date,
        )

    print("Finished deleting old records.")
    print("Finished running script.")


if __name__ == "__main__":
    main()
