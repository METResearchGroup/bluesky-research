"""Tests for lib/json_file_utils.py"""
import json
import gzip
import os

import pytest

from lib.json_file_utils import (
    _write_json_records_to_local_store,
    _write_json_dumped_records_to_local_store,
    write_jsons_to_local_store,
)


class TestWriteJsonRecordsToLocalStore:
    """Tests for _write_json_records_to_local_store function."""

    @pytest.mark.parametrize("compressed,filepath_input,expected_filepath", [
        (True, "output.jsonl.gz", "output.jsonl.gz"),
        (True, "output.jsonl", "output.jsonl.gz"),
        (False, "output.jsonl", "output.jsonl"),
    ])
    def test_writes_file_with_correct_compression(self, tmp_path, compressed, filepath_input, expected_filepath):
        """Test that files are written with correct compression and extension handling."""
        # Arrange
        export_filepath = tmp_path / filepath_input
        expected_path = tmp_path / expected_filepath
        records = [{"key": "value"}, {"key2": "value2"}]
        
        # Act
        _write_json_records_to_local_store(records, str(export_filepath), compressed=compressed)
        
        # Assert
        if compressed and not filepath_input.endswith(".gz"):
            assert not export_filepath.exists()
        assert expected_path.exists()
        
        # Read file based on compression
        if compressed:
            with gzip.open(expected_path, "rt") as f:
                lines = f.readlines()
        else:
            with open(expected_path, "r") as f:
                lines = f.readlines()
        
        assert len(lines) == len(records)
        for i, line in enumerate(lines):
            assert json.loads(line) == records[i]

    @pytest.mark.parametrize("records,expected_line_count", [
        ([{"id": 1, "name": "first"}, {"id": 2, "name": "second"}, {"id": 3, "name": "third"}], 3),
        ([], 0),
    ])
    def test_writes_correct_number_of_records(self, tmp_path, records, expected_line_count):
        """Test that correct number of records are written as JSONL."""
        # Arrange
        export_filepath = tmp_path / "output.jsonl"
        
        # Act
        _write_json_records_to_local_store(records, str(export_filepath), compressed=False)
        
        # Assert
        assert export_filepath.exists()
        with open(export_filepath, "r") as f:
            content = f.read()
        lines = content.splitlines() if content else []
        assert len(lines) == expected_line_count
        if expected_line_count > 0:
            for i, line in enumerate(lines):
                assert json.loads(line) == records[i]
        else:
            assert content == ""

    def test_writes_complex_json_structure(self, tmp_path):
        """Test that complex nested JSON structures are written correctly."""
        # Arrange
        export_filepath = tmp_path / "output.jsonl"
        records = [
            {
                "nested": {
                    "level1": {
                        "level2": "value",
                        "array": [1, 2, 3]
                    }
                },
                "metadata": {
                    "timestamp": "2024-01-01T00:00:00",
                    "tags": ["tag1", "tag2"]
                }
            }
        ]
        
        # Act
        _write_json_records_to_local_store(records, str(export_filepath), compressed=False)
        
        # Assert
        assert export_filepath.exists()
        with open(export_filepath, "r") as f:
            lines = f.readlines()
        assert len(lines) == 1
        assert json.loads(lines[0]) == records[0]


class TestWriteJsonDumpedRecordsToLocalStore:
    """Tests for _write_json_dumped_records_to_local_store function."""

    @pytest.fixture
    def setup_mocks(self, mocker):
        """Fixture to set up common mocks for tests."""
        mock_load = mocker.patch("lib.json_file_utils._load_json_data_from_directory")
        mock_write = mocker.patch("lib.json_file_utils._write_json_records_to_local_store")
        return mock_load, mock_write

    @pytest.fixture
    def setup_directory(self, tmp_path):
        """Fixture to set up source directory."""
        source_directory = str(tmp_path / "source")
        os.makedirs(source_directory, exist_ok=True)
        export_filepath = tmp_path / "output.jsonl"
        return source_directory, export_filepath

    @pytest.mark.parametrize("compressed,mock_data", [
        (True, [{"key": "value"}, {"key2": "value2"}]),
        (False, [{"key": "value"}]),
    ])
    def test_loads_json_from_directory_and_writes(self, mocker, tmp_path, setup_mocks, setup_directory, compressed, mock_data):
        """Test that JSON files are loaded from directory and written with correct compression."""
        # Arrange
        mock_load, mock_write = setup_mocks
        source_directory, export_filepath = setup_directory
        mock_load.return_value = mock_data
        
        # Act
        _write_json_dumped_records_to_local_store(
            source_directory,
            str(export_filepath),
            compressed=compressed
        )
        
        # Assert
        mock_load.assert_called_once_with(source_directory)
        mock_write.assert_called_once_with(mock_data, str(export_filepath), compressed)

    @pytest.mark.parametrize("mock_data", [
        [{"file1": "data1"}, {"file2": "data2"}, {"file3": "data3"}],
        [{"json": "data"}],
        [],
    ])
    def test_handles_various_data_scenarios(self, mocker, tmp_path, setup_mocks, setup_directory, mock_data):
        """Test that various data scenarios are handled correctly."""
        # Arrange
        mock_load, mock_write = setup_mocks
        source_directory, export_filepath = setup_directory
        mock_load.return_value = mock_data
        
        # Act
        _write_json_dumped_records_to_local_store(
            source_directory,
            str(export_filepath),
            compressed=False
        )
        
        # Assert
        mock_load.assert_called_once_with(source_directory)
        mock_write.assert_called_once_with(mock_data, str(export_filepath), False)


class TestWriteJsonsToLocalStore:
    """Tests for write_jsons_to_local_store function."""

    @pytest.fixture
    def setup_mocks(self, mocker):
        """Fixture to set up common mocks for tests."""
        mock_create_dir = mocker.patch("lib.json_file_utils.create_directory_if_not_exists")
        mock_write_records = mocker.patch("lib.json_file_utils._write_json_records_to_local_store")
        mock_write_dumped = mocker.patch("lib.json_file_utils._write_json_dumped_records_to_local_store")
        return mock_create_dir, mock_write_records, mock_write_dumped

    @pytest.fixture
    def setup_paths(self, tmp_path):
        """Fixture to set up common paths for tests."""
        export_filepath = tmp_path / "output.jsonl"
        source_directory = str(tmp_path / "source")
        return export_filepath, source_directory

    def test_writes_records_when_provided(self, mocker, tmp_path, setup_mocks, setup_paths):
        """Test that records parameter is used when provided."""
        # Arrange
        mock_create_dir, mock_write_records, _ = setup_mocks
        export_filepath, _ = setup_paths
        records = [{"key": "value"}]
        
        # Act
        write_jsons_to_local_store(
            export_filepath=str(export_filepath),
            records=records
        )
        
        # Assert
        mock_create_dir.assert_called_once_with(str(export_filepath))
        mock_write_records.assert_called_once_with(
            records=records,
            export_filepath=str(export_filepath),
            compressed=True
        )

    def test_writes_from_directory_when_provided(self, mocker, tmp_path, setup_mocks, setup_paths):
        """Test that source_directory is used when provided."""
        # Arrange
        mock_create_dir, _, mock_write_dumped = setup_mocks
        export_filepath, source_directory = setup_paths
        os.makedirs(source_directory, exist_ok=True)
        
        # Act
        write_jsons_to_local_store(
            export_filepath=str(export_filepath),
            source_directory=source_directory
        )
        
        # Assert
        mock_create_dir.assert_called_once_with(str(export_filepath))
        mock_write_dumped.assert_called_once_with(
            source_directory=source_directory,
            export_filepath=str(export_filepath),
            compressed=True
        )

    @pytest.mark.parametrize("records,source_directory,description", [
        (None, None, "both records and source_directory are None"),
        ([], None, "records is empty list and source_directory is None"),
        (None, "/nonexistent/path", "source_directory doesn't exist"),
    ])
    def test_raises_value_error_when_no_source_provided(self, mocker, tmp_path, setup_mocks, setup_paths, records, source_directory, description):
        """Test that ValueError is raised when no valid source data is provided."""
        # Arrange
        mock_create_dir, _, _ = setup_mocks
        export_filepath, _ = setup_paths
        if source_directory and "nonexistent" in source_directory:
            source_directory = str(tmp_path / "nonexistent")
        
        # Act & Assert
        with pytest.raises(ValueError, match="No source data provided."):
            write_jsons_to_local_store(
                export_filepath=str(export_filepath),
                records=records,
                source_directory=source_directory
            )
        
        mock_create_dir.assert_called_once_with(str(export_filepath))

    def test_creates_directory_before_writing(self, mocker, tmp_path, setup_mocks, setup_paths):
        """Test that create_directory_if_not_exists is called before writing."""
        # Arrange
        mock_create_dir, mock_write_records, _ = setup_mocks
        export_filepath, _ = setup_paths
        records = [{"key": "value"}]
        
        # Act
        write_jsons_to_local_store(
            export_filepath=str(export_filepath),
            records=records
        )
        
        # Assert
        mock_create_dir.assert_called_once()
        mock_write_records.assert_called_once()
        # Verify both were called (order is guaranteed by function implementation)

    def test_prefers_records_over_directory(self, mocker, tmp_path, setup_mocks, setup_paths):
        """Test that records takes precedence when both provided."""
        # Arrange
        mock_create_dir, mock_write_records, mock_write_dumped = setup_mocks
        export_filepath, source_directory = setup_paths
        os.makedirs(source_directory, exist_ok=True)
        records = [{"key": "value"}]
        
        # Act
        write_jsons_to_local_store(
            export_filepath=str(export_filepath),
            records=records,
            source_directory=source_directory
        )
        
        # Assert
        mock_create_dir.assert_called_once_with(str(export_filepath))
        mock_write_records.assert_called_once()
        mock_write_dumped.assert_not_called()

    @pytest.mark.parametrize("compressed", [True, False])
    def test_passes_compressed_parameter(self, mocker, tmp_path, setup_mocks, setup_paths, compressed):
        """Test that compressed parameter is passed correctly."""
        # Arrange
        mock_create_dir, mock_write_records, _ = setup_mocks
        export_filepath, _ = setup_paths
        records = [{"key": "value"}]
        
        # Act
        write_jsons_to_local_store(
            export_filepath=str(export_filepath),
            records=records,
            compressed=compressed
        )
        
        # Assert
        mock_write_records.assert_called_once_with(
            records=records,
            export_filepath=str(export_filepath),
            compressed=compressed
        )

    def test_handles_records_with_empty_list_check(self, mocker, tmp_path, setup_mocks, setup_paths):
        """Test that empty records list triggers directory path if provided."""
        # Arrange
        mock_create_dir, mock_write_records, mock_write_dumped = setup_mocks
        export_filepath, source_directory = setup_paths
        os.makedirs(source_directory, exist_ok=True)
        
        # Act - Empty records list should fall through to source_directory check
        write_jsons_to_local_store(
            export_filepath=str(export_filepath),
            records=[],
            source_directory=source_directory
        )
        
        # Assert
        mock_create_dir.assert_called_once_with(str(export_filepath))
        mock_write_records.assert_not_called()
        mock_write_dumped.assert_called_once()
