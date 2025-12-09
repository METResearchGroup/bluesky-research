import os
import json

from services.sync.stream.protocols import DirectoryManagerProtocol


class CacheFileWriter:
    """Manages the writing of cache files."""

    def __init__(self, directory_manager: DirectoryManagerProtocol):
        """Initialize file writer.

        Args:
            directory_manager: Manager for ensuring directories exist
        """
        self.directory_manager = directory_manager

    def write_json(self, path: str, data: dict) -> None:
        """Write JSON data to file at path."""
        parent_dir = os.path.dirname(path)
        self.directory_manager.ensure_exists(parent_dir)
        with open(path, "w") as f:
            json.dump(data, f)

    def write_jsonl(self, path: str, records: list[dict]) -> None:
        """Write JSONL data to file at path."""
        parent_dir = os.path.dirname(path)
        self.directory_manager.ensure_exists(parent_dir)
        with open(path, "w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")


class CacheFileReader:
    """Manages the reading of cache files."""

    def read_json(self, path: str) -> dict:
        """Read JSON file from path."""
        with open(path, "r") as f:
            return json.load(f)

    def list_files(self, directory: str) -> list[str]:
        """List all files in directory."""
        if not os.path.exists(directory):
            return []
        return [
            f
            for f in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, f))
        ]

    def read_all_json_in_directory(
        self, directory: str
    ) -> tuple[list[dict], list[str]]:
        """Read all JSON files in directory, returning (data, filepaths)."""
        records: list[dict] = []
        filepaths: list[str] = []

        if not os.path.exists(directory):
            return records, filepaths

        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath) and filename.endswith(".json"):
                data = self.read_json(filepath)
                records.append(data)
                filepaths.append(filepath)

        return records, filepaths
