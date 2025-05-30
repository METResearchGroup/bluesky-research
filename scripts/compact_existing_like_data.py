"""One-off script to compact existing like data.

Useful template though for how to do it for similar data.

Works for both the like and like_on_user_post data.
"""

import json
import os

import pandas as pd
import pyarrow as pa

from lib.db.manage_local_data import load_data_from_local_storage

filedir = "/projects/p32375/bluesky_research_data/raw_sync/create/like/active"
# filedir = "/projects/p32375/bluesky_research_data/raw_sync/create/like_on_user_post/active"
# filedir = "/Users/mark/Documents/work/bluesky_research_data/raw_sync/create/like/active"
# filedir = "/Users/mark/Documents/work/bluesky_research_data/raw_sync/create/like/cache"
# tmp_output_dir = "/projects/p32375/bluesky_research_data/raw_sync/create/like/active"


def delete_empty_directories(directory: str):
    """Deletes empty directories within the specified directory."""
    for root, dirs, files in os.walk(directory, topdown=False):
        for dir in dirs:
            dir_path = os.path.join(root, dir)
            if not os.listdir(dir_path):
                os.rmdir(dir_path)
                print(f"Deleted empty directory: {dir_path}")


def compact_file_chunks(chunk_filepaths: list[str]):
    validated_files = []
    invalidated_files = []
    for filepath in chunk_filepaths:
        try:
            # for some reason, my normal pq validation doesn't work here.
            # problem is small enough to just read each file as pandas df.
            pd.read_parquet(filepath)
            validated_files.append(filepath)
        except Exception as e:
            invalidated_files.append(filepath)
            print(f"Invalidated file: {filepath} (reason: {e})")
    print(f"Validated {len(validated_files)} files...")
    print(f"Invalidated {len(invalidated_files)} files...")
    df = pd.read_parquet(validated_files)
    # df = pd.read_parquet(validated_files[0:-1]) # last file is corrupt
    # invalidated_files.append(validated_files[-1])
    df = df.drop_duplicates(subset=["uri"])
    df["record"] = df["record"].apply(lambda x: json.dumps(x))
    if not os.path.exists(filedir):
        os.makedirs(filedir, exist_ok=True)
    df["partition_date"] = pd.to_datetime(df["synctimestamp"]).dt.date
    df["partition_date"] = df["partition_date"].apply(lambda x: x.strftime("%Y-%m-%d"))
    print(f"Number of files in the DF: {len(df)}")
    print("Dates in group and their counts:")
    print(df["partition_date"].value_counts())
    date_groups = df.groupby("partition_date")
    print(f"Number of date groups: {len(date_groups)}")
    for _, group in date_groups:
        group.to_parquet(filedir, index=False, partition_cols=["partition_date"])
    print(f"Wrote {len(df)} records to {filedir}...")
    for filepath in chunk_filepaths:
        os.remove(filepath)
    print(f"Deleted {len(chunk_filepaths)} files...")


def migrate_incorrectly_partitioned_files():
    filepaths = []
    for root, dirs, files in os.walk(filedir):
        for file in files:
            filepaths.append(os.path.join(root, file))
    filepaths.sort()
    # only grab files that were incorrectly partitioned. We'll move on to
    # all the files later.
    print(f"Number of files in filepaths (before subsetting): {len(filepaths)}")
    subset = [file for file in filepaths if "startTimestamp" in file]
    print(f"Number of files in filepaths (after subsetting): {len(subset)}")
    chunk_size = 100
    chunks = [subset[i : i + chunk_size] for i in range(0, len(subset), chunk_size)]
    for i, chunk in enumerate(chunks):
        try:
            print(f"Compacting file chunks in iteration {i}...")
            compact_file_chunks(chunk)
        except Exception as e:
            print(f"Error compacting file chunks in iteration {i}: {e}")
            continue
    print("Deleted all the files in filepaths.")
    print(f"Number of files in input directory: {len(os.listdir(filedir))}")
    print(f"Number of files in filedir: {len(os.listdir(filedir))}")


def reformat_correctly_partitioned_files():
    """Takes data from the partitions and does reformatting if applicable.

    Data that was migrated in `migrate_incorrectly_partitioned_files` will have
    their records reformatted already to strings, while data that wasn't migrated
    (since they were already in the correct partitions) will not have their records
    reformatted to strings, and so they need to be reformatted.
    """
    for partition_directory in os.listdir(filedir):
        print(f"Reformatting files in partition directory: {partition_directory}...")
        partition_dir = os.path.join(filedir, partition_directory)
        filenames = os.listdir(partition_dir)
        try:
            df = pd.read_parquet(partition_dir)
        except pa.lib.ArrowNotImplementedError:
            # try loading each file individually and then appending them,
            # to get around casting errors.
            filenames = os.listdir(partition_dir)
            dfs = []
            for filename in filenames:
                filepath = os.path.join(partition_dir, filename)
                try:
                    df = pd.read_parquet(filepath)
                    if not isinstance(df["record"].iloc[0], str):
                        df["record"] = df["record"].apply(lambda x: json.dumps(x))
                    dfs.append(df)
                except Exception as e:
                    print(f"Error reading file {filepath}: {e}")
            df = pd.concat(dfs)
        if not isinstance(df["record"][0], str):
            df["record"] = df["record"].apply(lambda x: json.dumps(x))
        if "partition_date" not in df.columns:
            df["partition_date"] = pd.to_datetime(df["synctimestamp"]).dt.date
            df["partition_date"] = df["partition_date"].apply(
                lambda x: x.strftime("%Y-%m-%d")
            )
        dtypes_map = {
            "author": "string",
            "cid": "string",
            "record": "string",
            "uri": "string",
            "synctimestamp": "string",
        }
        df = df.astype(dtypes_map)
        df.reset_index(drop=True, inplace=True)
        date_groups = df.groupby("partition_date")
        print(f"Number of date groups: {len(date_groups)}")
        for _, group in date_groups:
            group.to_parquet(filedir, index=False, partition_cols=["partition_date"])
        print(
            f"Finished compacting files in partition directory: {partition_directory}."
        )
        print(f"Now deleting the old files in {partition_dir}...")
        for filename in filenames:
            os.remove(os.path.join(partition_dir, filename))
        print("Deleted the old files.")


def main():
    # print("Migrating incorrectly partitioned files...")
    # migrate_incorrectly_partitioned_files()
    print("Reformatting correctly partitioned files...")
    reformat_correctly_partitioned_files()
    print(
        "Now that migration is done, we need to load from local storage to verify that it works."
    )
    df = load_data_from_local_storage(service="study_user_likes")
    print(f"Number of records in the DF: {len(df)}")
    df["partition_date"] = pd.to_datetime(df["synctimestamp"]).dt.date
    df["partition_date"] = df["partition_date"].apply(lambda x: x.strftime("%Y-%m-%d"))
    print(f"Number of like records per day: {df['partition_date'].value_counts()}")
    print("Deleting empty directories...")
    delete_empty_directories(filedir)
    print("Done.")


if __name__ == "__main__":
    main()
