import gzip
import json
import os

from lib.path_utils import create_directory_if_not_exists


def load_jsonl_data(filepath: str) -> list[dict]:
    """Load JSONL data from a file, supporting gzipped files."""
    if filepath.endswith(".gz"):
        with gzip.open(filepath, "rt", encoding="utf-8") as f:
            data = [json.loads(line) for line in f]
    else:
        with open(filepath, "r", encoding="utf-8") as f:
            data = [json.loads(line) for line in f]
    return data


def _load_json_data_from_directory(directory: str) -> list[dict]:
    """Load JSON data from a directory."""
    data: list[dict] = []
    for file in os.listdir(directory):
        if file.endswith(".json"):
            with open(os.path.join(directory, file), "r", encoding="utf-8") as f:
                data.extend(json.load(f))
    return data


def _write_json_records_to_local_store(
    records: list[dict],
    export_filepath: str,
    compressed: bool,
) -> None:
    """Takes a list of JSON records and writes them to a local store."""
    if compressed:
        if not export_filepath.endswith(".gz"):
            export_filepath += ".gz"
        with gzip.open(export_filepath, "wt", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
    else:
        with open(export_filepath, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")


def _write_json_dumped_records_to_local_store(
    source_directory: str,
    export_filepath: str,
    compressed: bool,
) -> None:
    """Takes a directory of JSON files and writes them to a local store."""
    res: list[dict] = _load_json_data_from_directory(source_directory)
    _write_json_records_to_local_store(res, export_filepath, compressed)


def write_jsons_to_local_store(
    export_filepath: str,
    source_directory: str | None = None,
    records: list[dict] | None = None,
    compressed: bool = True,
):
    """Writes local JSONs to local store. Writes as a .jsonl file.

    Loads in JSONs from a given directory and writes them to the local storage
    using the export filepath.
    """
    create_directory_if_not_exists(export_filepath)

    if records is not None and len(records) > 0:
        _write_json_records_to_local_store(
            records=records,
            export_filepath=export_filepath,
            compressed=compressed,
        )
    elif source_directory is not None and os.path.exists(source_directory):
        _write_json_dumped_records_to_local_store(
            source_directory=source_directory,
            export_filepath=export_filepath,
            compressed=compressed,
        )
    else:
        raise ValueError("No source data provided.")
