"""Generic helpers for loading local data."""
import gzip
import json
import os
from typing import Optional


def load_jsonl_data(filepath: str) -> list[dict]:
    """Load JSONL data from a file, supporting gzipped files."""
    if filepath.endswith('.gz'):
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            data = [json.loads(line) for line in f]
    else:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = [json.loads(line) for line in f]
    return data


def write_jsons_to_local_store(
    source_directory: Optional[str] = None,
    records: Optional[list[dict]] = None,
    export_filepath: str = None,
    compressed: bool = True
):
    """Writes local JSONs to local store. Writes as a .jsonl file.

    Loads in JSONs from a given directory and writes them to the local storage
    using the export filepath.
    """
    dirpath = os.path.dirname(export_filepath)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)

    res: list[dict] = []

    if not source_directory and records:
        res = records
    elif source_directory:
        for file in os.listdir(source_directory):
            if file.endswith(".json"):
                with open(os.path.join(source_directory, file), 'r') as f:
                    res.append(json.load(f))
    elif not source_directory and not records:
        raise ValueError("No source data provided.")

    intermediate_filepath = export_filepath
    if compressed:
        intermediate_filepath += ".gz"

    # Write the JSON lines to a file
    if not compressed:
        with open(export_filepath, 'w') as f:
            for item in res:
                f.write(json.dumps(item) + "\n")
    else:
        with gzip.open(intermediate_filepath, 'wt') as f:
            for item in res:
                f.write(json.dumps(item) + "\n")


def find_files_after_timestamp(base_path: str, target_timestamp_path: str) -> list[str]:
    """Find files after a given timestamp."""
    year, month, day, hour, minute = target_timestamp_path.split("/")
    files_list = []
    for year_dir in os.listdir(base_path):
        if year_dir >= year:
            if year_dir > year:
                # crawl all files, even in subdirectories, and add to list
                # of files
                year_dir_path = os.path.join(base_path, year_dir)
                for root, _, files in os.walk(year_dir_path):
                    for file in files:
                        files_list.append(os.path.join(root, file))
                continue
            else:
                # case where year_dir == year
                months = os.listdir(os.path.join(base_path, year_dir))
                for month_dir in months:
                    # if same year + more recent month, crawl all files.
                    if month_dir > month:
                        # crawl all files, even in subdirectories, and add
                        # to list of files
                        month_dir_path = os.path.join(base_path, year_dir, month_dir)  # noqa
                        for root, _, files in os.walk(month_dir_path):
                            for file in files:
                                files_list.append(os.path.join(root, file))
                    elif month_dir == month:
                        # if same month, check days
                        days = os.listdir(
                            os.path.join(base_path, year_dir, month_dir)
                        )
                        for day_dir in days:
                            # if same year + same month + more recent day,
                            # crawl all files.
                            if day_dir > day:
                                # crawl all files, even in subdirectories, and
                                # add to list of files
                                day_dir_path = os.path.join(
                                    base_path, year_dir, month_dir, day_dir
                                )
                                for root, _, files in os.walk(day_dir_path):
                                    for file in files:
                                        files_list.append(os.path.join(root, file))  # noqa
                            elif day_dir == day:
                                # if same day, check hours
                                hours = os.listdir(
                                    os.path.join(base_path, year_dir, month_dir, day_dir)  # noqa
                                )
                                for hour_dir in hours:
                                    # if same year + same month + same day +
                                    # more recent hour, crawl all files.
                                    if hour_dir > hour:
                                        # crawl all files, even in
                                        # subdirectories, and add to list
                                        # of files
                                        hour_dir_path = os.path.join(
                                            base_path, year_dir, month_dir, day_dir, hour_dir  # noqa
                                        )
                                        for root, _, files in os.walk(hour_dir_path):  # noqa
                                            for file in files:
                                                files_list.append(os.path.join(root, file))  # noqa
                                    elif hour_dir == hour:
                                        # if same hour, check minutes
                                        minutes = os.listdir(
                                            os.path.join(
                                                base_path, year_dir, month_dir, day_dir, hour_dir  # noqa
                                            )
                                        )
                                        for minute_dir in minutes:
                                            # if same year + same month +
                                            # same day + same hour + more
                                            # recent minute, crawl all files.
                                            if minute_dir > minute:
                                                # crawl all files, even in
                                                # subdirectories, and add to
                                                # list of files
                                                minute_dir_path = os.path.join(
                                                    base_path, year_dir, month_dir, day_dir, hour_dir, minute_dir  # noqa
                                                )
                                                for root, _, files in os.walk(minute_dir_path):  # noqa
                                                    for file in files:
                                                        files_list.append(os.path.join(root, file))  # noqa

    return files_list
