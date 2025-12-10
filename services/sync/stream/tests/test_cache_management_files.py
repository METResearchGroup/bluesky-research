"""Tests for cache_management/files.py - FileUtilities class."""

import json
import os
import tempfile

import pytest

from services.sync.stream.cache_management import FileUtilities


class TestFileUtilities:
    """Tests for FileUtilities class."""

    def test_init(self, dir_manager):
        """Test FileUtilities initialization."""
        # Act
        file_utilities = FileUtilities(directory_manager=dir_manager)

        # Assert
        assert file_utilities.directory_manager == dir_manager

    def test_write_json_creates_file(self, file_writer):
        """Test write_json creates JSON file with correct data."""
        # Arrange

        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "test.json")
            test_data = {"key": "value", "number": 42}

            # Act
            file_writer.write_json(test_path, test_data)

            # Assert
            assert os.path.exists(test_path)
            with open(test_path, "r") as f:
                loaded_data = json.load(f)
            assert loaded_data == test_data

    def test_write_jsonl_creates_file(self, file_writer):
        """Test write_jsonl creates JSONL file with correct data."""
        # Arrange

        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "test.jsonl")
            test_records = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]

            # Act
            file_writer.write_jsonl(test_path, test_records)

            # Assert
            assert os.path.exists(test_path)
            with open(test_path, "r") as f:
                lines = f.readlines()
            assert len(lines) == 2
            assert json.loads(lines[0]) == test_records[0]
            assert json.loads(lines[1]) == test_records[1]

    def test_delete_files_removes_files(self, file_writer):
        """Test delete_files removes specified files."""
        # Arrange
        with tempfile.TemporaryDirectory() as tmpdir:
            file1 = os.path.join(tmpdir, "file1.json")
            file2 = os.path.join(tmpdir, "file2.json")
            file3 = os.path.join(tmpdir, "file3.txt")
            
            # Create files
            with open(file1, "w") as f:
                json.dump({"test": 1}, f)
            with open(file2, "w") as f:
                json.dump({"test": 2}, f)
            with open(file3, "w") as f:
                f.write("text file")
            
            # Verify files exist
            assert os.path.exists(file1)
            assert os.path.exists(file2)
            assert os.path.exists(file3)
            
            # Act
            file_writer.delete_files([file1, file2])
            
            # Assert
            assert not os.path.exists(file1)
            assert not os.path.exists(file2)
            assert os.path.exists(file3)  # Should not be deleted

    def test_delete_files_handles_nonexistent_files(self, file_writer):
        """Test delete_files handles nonexistent files gracefully."""
        # Arrange
        nonexistent_file = "/nonexistent/path/file.json"
        
        # Act & Assert (should not raise)
        file_writer.delete_files([nonexistent_file])

    def test_delete_files_handles_directories(self, file_writer):
        """Test delete_files does not delete directories."""
        # Arrange
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = os.path.join(tmpdir, "subdir")
            os.makedirs(subdir)
            
            # Act
            file_writer.delete_files([subdir])
            
            # Assert
            assert os.path.exists(subdir)  # Directory should still exist

    def test_read_json_loads_file(self, file_reader):
        """Test read_json loads JSON file correctly."""
        # Arrange

        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "test.json")
            test_data = {"key": "value", "number": 42}
            with open(test_path, "w") as f:
                json.dump(test_data, f)

            # Act
            result = file_reader.read_json(test_path)

            # Assert
            assert result == test_data

    def test_list_files_returns_files_only(self, file_reader):
        """Test list_files returns only files, not directories."""
        # Arrange

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create files and directories
            file1 = os.path.join(tmpdir, "file1.txt")
            file2 = os.path.join(tmpdir, "file2.txt")
            subdir = os.path.join(tmpdir, "subdir")
            os.makedirs(subdir)

            with open(file1, "w") as f:
                f.write("content1")
            with open(file2, "w") as f:
                f.write("content2")

            # Act
            result = file_reader.list_files(tmpdir)

            # Assert
            assert len(result) == 2
            assert "file1.txt" in result
            assert "file2.txt" in result
            assert "subdir" not in result

    def test_list_files_returns_empty_list_for_nonexistent_directory(self, file_reader):
        """Test list_files returns empty list for non-existent directory."""
        # Arrange
        non_existent = "/nonexistent/directory/path"

        # Act
        result = file_reader.list_files(non_existent)

        # Assert
        assert result == []

    def test_read_all_json_in_directory_reads_all_json_files(self, file_reader):
        """Test read_all_json_in_directory reads all JSON files."""
        # Arrange

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create multiple JSON files
            file1 = os.path.join(tmpdir, "file1.json")
            file2 = os.path.join(tmpdir, "file2.json")
            file3 = os.path.join(tmpdir, "file3.txt")  # Non-JSON file

            data1 = {"id": 1, "name": "test1"}
            data2 = {"id": 2, "name": "test2"}

            with open(file1, "w") as f:
                json.dump(data1, f)
            with open(file2, "w") as f:
                json.dump(data2, f)
            with open(file3, "w") as f:
                f.write("not json")

            # Act
            records, filepaths = file_reader.read_all_json_in_directory(tmpdir)

            # Assert
            assert len(records) == 2
            assert len(filepaths) == 2
            assert data1 in records
            assert data2 in records
            assert file1 in filepaths
            assert file2 in filepaths

    def test_read_all_json_in_directory_returns_empty_for_nonexistent_directory(
        self,
        file_reader,
    ):
        """Test read_all_json_in_directory returns empty for non-existent directory."""
        # Arrange
        non_existent = "/nonexistent/directory/path"

        # Act
        records, filepaths = file_reader.read_all_json_in_directory(non_existent)

        # Assert
        assert records == []
        assert filepaths == []

    def test_read_all_json_in_directory_skips_malformed_json_files(self, file_reader):
        """Test read_all_json_in_directory skips malformed JSON files and continues."""
        # Arrange
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create valid and invalid JSON files
            file1 = os.path.join(tmpdir, "file1.json")
            file2 = os.path.join(tmpdir, "file2.json")
            file3 = os.path.join(tmpdir, "file3.json")  # Malformed JSON

            data1 = {"id": 1, "name": "test1"}
            data2 = {"id": 2, "name": "test2"}

            with open(file1, "w") as f:
                json.dump(data1, f)
            with open(file2, "w") as f:
                json.dump(data2, f)
            with open(file3, "w") as f:
                f.write("not valid json {invalid}")

            # Act
            records, filepaths = file_reader.read_all_json_in_directory(tmpdir)

            # Assert
            # Should only return valid JSON files
            assert len(records) == 2
            assert len(filepaths) == 2
            assert data1 in records
            assert data2 in records
            assert file1 in filepaths
            assert file2 in filepaths
            assert file3 not in filepaths

