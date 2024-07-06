"""Generic helpers for loading local data."""
import gzip
import json
import os
import shutil
from typing import Optional


def load_jsonl_data(filepath: str) -> list[dict]:
    """Load JSONL data from a file."""
    with open(filepath, "r") as f:
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
    else:
        for file in os.listdir(source_directory):
            if file.endswith(".json"):
                with open(os.path.join(source_directory, file), 'r') as f:
                    res.append(json.load(f))

    if compressed:
        export_filepath += ".gz"

    if compressed:
        with open(export_filepath, 'rb') as f_in:
            with gzip.open(export_filepath + ".gz", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    else:
        with open(export_filepath, 'w') as f:
            for item in res:
                f.write(json.dumps(item) + "\n")
