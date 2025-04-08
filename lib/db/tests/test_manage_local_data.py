"""Tests for lib/db/manage_local_data.py"""
import os
from tempfile import TemporaryDirectory

import pandas as pd
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from lib.aws.s3 import S3
from lib.db.manage_local_data import (
    find_files_after_timestamp,
    _get_all_filenames,
    _get_all_filenames_deprecated_format,
    _validate_filepaths,
    list_filenames,
    load_data_from_local_storage,
    export_data_to_local_storage,
)

MOCK_SERVICE_METADATA = {
    "test_service": {
        "local_prefix": "/data/test_service",
        "timestamp_field": "preprocessing_timestamp",
        "partition_key": "preprocessing_timestamp",
    },
    "preprocessed_posts": {
        "local_prefix": "/data/preprocessed_posts",
        "timestamp_field": "preprocessing_timestamp",
        "partition_key": "preprocessing_timestamp",
    }
}

@pytest.mark.parametrize("timestamp_str,expected", [
    ("2024-07-06-20:39:30", "year=2024/month=07/day=06/hour=20/minute=39"),
    ("2025-12-31-23:59:59", "year=2025/month=12/day=31/hour=23/minute=59"),
])
def test_create_partition_key_based_on_timestamp(timestamp_str, expected):
    """Test create_partition_key_based_on_timestamp"""
    with patch('boto3.client') as mock_client:
        mock_s3 = Mock()
        mock_client.return_value = mock_s3
        s3 = S3(create_client_flag=False)
        assert (
            s3.create_partition_key_based_on_timestamp(timestamp_str) == expected
        )


# Setup for find_files_after_timestamp tests


@pytest.fixture
def setup_directory_structure():
    """Create a temporary directory with test files."""
    with TemporaryDirectory() as tmpdir:
        # Create sample directory structures and test files
        timestamps = [
            ("2024", "01", "01", "01", "01"),  # shouldn't be included
            ("2024", "07", "01", "01", "01"),  # shouldn't be included
            ("2024", "07", "05", "20", "59"),
            ("2024", "07", "05", "21", "01"),
            ("2024", "07", "06", "01", "01"),
            ("2024", "08", "01", "01", "01"),
        ]
        for year, month, day, hour, minute in timestamps:
            dir_path = os.path.join(tmpdir, f"year={year}/month={month}/day={day}/hour={hour}/minute={minute}")
            os.makedirs(dir_path, exist_ok=True)
            test_file_path = os.path.join(dir_path, "test_file.txt")
            with open(test_file_path, "w") as f:
                f.write(f"This is a test file for {year}/{month}/{day} {hour}:{minute}.")
        yield tmpdir


def test_find_files_after_timestamp(setup_directory_structure):
    """Test find_files_after_timestamp"""
    base_path = setup_directory_structure
    # A minute before the test file's timestamp
    target_timestamp = "year=2024/month=07/day=05/hour=20/minute=58"
    files_found = find_files_after_timestamp(base_path, target_timestamp)
    assert len(files_found) == 4


@pytest.fixture
def mock_parquet_files():
    """Create a mock list of parquet files with different partition dates."""
    return [
        "/data/service/active/partition_date=2024-03-01/file1.parquet",
        "/data/service/active/partition_date=2024-03-02/file2.parquet",
        "/data/service/active/partition_date=2024-03-03/file3.parquet",
        "/data/service/active/partition_date=2024-03-04/file4.parquet",
        "/data/service/cache/partition_date=2024-02-28/file5.parquet",
    ]


class TestGetAllFilenames:
    """Test suite for _get_all_filenames function."""

    @pytest.fixture(autouse=True)
    def setup(self, mocker):
        """Set up common test fixtures."""
        # Mock service metadata
        self.mock_metadata = {
            "test_service": {
                "local_prefix": "/data/test_service"
            }
        }
        mocker.patch("lib.db.manage_local_data.MAP_SERVICE_TO_METADATA", self.mock_metadata)
        
        # Mock get_local_prefix_for_service
        self.mock_get_prefix = mocker.patch("lib.db.manage_local_data.get_local_prefix_for_service")
        self.mock_get_prefix.return_value = "/data/test_service"

    def test_empty_directory(self, mocker):
        """Test when target directory is empty.
        
        Expected behavior:
        - Should return empty list when no files exist
        """
        # Mock os.walk to return empty file list
        mocker.patch("os.walk", return_value=[("/data/test_service/active", [], [])])
        
        result = _get_all_filenames("test_service")
        assert result == []

    def test_no_valid_files(self, mocker):
        """Test when directory contains only invalid parquet files.
        
        Expected behavior:
        - Should return empty list when validate_pq_files=True and no files pass validation
        """
        # Mock file structure with invalid parquet files
        test_files = [
            "/data/test_service/active/partition_date=2024-03-01/invalid1.parquet",
            "/data/test_service/active/partition_date=2024-03-02/invalid2.parquet"
        ]
        
        # Mock os.walk to return test files
        mocker.patch("os.walk", return_value=[("/data/test_service/active", [], test_files)])
        
        # Mock validated_pq_files_within_directory to return empty list (no valid files)
        mock_validate = mocker.patch("lib.db.manage_local_data.validated_pq_files_within_directory")
        mock_validate.return_value = []

        result = _get_all_filenames("test_service", validate_pq_files=True)
        assert result == []

    def test_mixed_valid_invalid_files(self, mocker):
        """Test with mix of valid and invalid parquet files.
        
        Expected behavior:
        - Should return only validated files when validate_pq_files=True
        """
        # Set up test files
        valid_files = [
            "/data/test_service/active/partition_date=2024-03-01/valid1.parquet",
            "/data/test_service/active/partition_date=2024-03-02/valid2.parquet"
        ]
        all_files = valid_files + [
            "/data/test_service/active/partition_date=2024-03-03/invalid.parquet"
        ]
        
        # Mock os.walk
        mocker.patch("os.walk", return_value=[("/data/test_service/active", [], all_files)])
        
        # Mock validation to return only valid files
        mock_validate = mocker.patch("lib.db.manage_local_data.validated_pq_files_within_directory")
        mock_validate.return_value = valid_files

        result = _get_all_filenames("test_service", validate_pq_files=True)
        assert set(result) == set(valid_files)

    def test_incorrect_directory_structure(self, mocker):
        """Test with files in new format structure.
        
        The function should only look in paths that match the expected structure
        (service/directory/partition_date=*/file.parquet), so files in deprecated format
        (service/firehose|most_liked/directory/partition_date=*/file.parquet) should be ignored.
        """
        # Mock os.walk to return different results based on the path being walked
        def mock_walk(path):
            if 'firehose' in path or 'most_liked' in path:
                # Files in deprecated format should be ignored
                return [(path, [], ["file1.parquet", "file2.parquet"])]
            else:
                # No files found in correct format paths
                return [(path, [], [])]
        
        mocker.patch("os.walk", side_effect=mock_walk)
        
        result = _get_all_filenames("test_service")
        assert result == []


class TestGetAllFilenamesDeprecatedFormat:
    """Test suite for _get_all_filenames_deprecated_format function."""

    @pytest.fixture(autouse=True)
    def setup(self, mocker):
        """Set up common test fixtures."""
        # Mock service metadata
        self.mock_metadata = {
            "ml_inference_perspective_api": {
                "local_prefix": "/data/ml_inference_perspective_api"
            }
        }
        mocker.patch("lib.db.manage_local_data.MAP_SERVICE_TO_METADATA", self.mock_metadata)
        
        # Mock get_local_prefix_for_service
        self.mock_get_prefix = mocker.patch("lib.db.manage_local_data.get_local_prefix_for_service")
        self.mock_get_prefix.return_value = "/data/ml_inference_perspective_api"

    def test_empty_directory(self, mocker):
        """Test when target directory is empty.
        
        Expected behavior:
        - Should return empty list when no files exist
        """
        # Mock os.walk to return empty file list
        mocker.patch("os.walk", return_value=[("/data/ml_inference_perspective_api/firehose/active", [], [])])
        
        result = _get_all_filenames_deprecated_format("ml_inference_perspective_api")
        assert result == []

    def test_no_valid_files(self, mocker):
        """Test when directory contains only invalid parquet files.
        
        Expected behavior:
        - Should return empty list when validate_pq_files=True and no files pass validation
        """
        # Mock file structure with invalid parquet files
        test_files = [
            "/data/ml_inference_perspective_api/firehose/active/partition_date=2024-03-01/invalid1.parquet",
            "/data/ml_inference_perspective_api/most_liked/active/partition_date=2024-03-02/invalid2.parquet"
        ]
        
        # Mock os.walk to return test files
        mocker.patch("os.walk", return_value=[("/data/ml_inference_perspective_api/firehose/active", [], test_files)])
        
        # Mock validated_pq_files_within_directory to return empty list (no valid files)
        mock_validate = mocker.patch("lib.db.manage_local_data.validated_pq_files_within_directory")
        mock_validate.return_value = []

        result = _get_all_filenames_deprecated_format("ml_inference_perspective_api", validate_pq_files=True)
        assert result == []

    def test_mixed_valid_invalid_files(self, mocker):
        """Test with mix of valid and invalid parquet files.
        
        Expected behavior:
        - Should return only validated files when validate_pq_files=True
        """
        # Set up test files
        valid_files = [
            "/data/ml_inference_perspective_api/firehose/active/partition_date=2024-03-01/valid1.parquet",
            "/data/ml_inference_perspective_api/firehose/active/partition_date=2024-03-02/valid2.parquet"
        ]
        all_files = valid_files + [
            "/data/ml_inference_perspective_api/firehose/active/partition_date=2024-03-03/invalid.parquet"
        ]
        
        # Mock os.walk
        mocker.patch("os.walk", return_value=[("/data/ml_inference_perspective_api/source_type/active", [], all_files)])
        
        # Mock validation to return only valid files
        mock_validate = mocker.patch("lib.db.manage_local_data.validated_pq_files_within_directory")
        mock_validate.return_value = valid_files

        result = _get_all_filenames_deprecated_format("ml_inference_perspective_api", validate_pq_files=True)
        assert set(result) == set(valid_files)

    def test_incorrect_directory_structure(self, mocker):
        """Test with files in new format structure.
        
        The function should only look in paths that include 'firehose' or 'most_liked',
        so files in the new format (directly under 'active') should be ignored.
        """
        # Mock os.walk to return different results based on the path being walked
        def mock_walk(path):
            if 'firehose' in path or 'most_liked' in path:
                # No files found in deprecated format paths
                return [(path, [], [])]
            else:
                # Files in new format should be ignored
                return [(path, [], ["file1.parquet", "file2.parquet"])]
        
        mocker.patch("os.walk", side_effect=mock_walk)
        
        result = _get_all_filenames_deprecated_format("ml_inference_perspective_api")
        assert result == []


class TestValidateFilepaths:
    """Test suite for _validate_filepaths function."""

    def test_no_filepaths(self):
        """Test validation of empty filepaths list.
        
        Expected behavior:
        - Should return empty list when no filepaths are provided
        - Should not raise any errors
        """
        result = _validate_filepaths("test_service", [])
        assert result == []

    def test_both_partition_and_range_dates(self):
        """Test validation when both partition_date and date range are provided.
        
        Expected behavior:
        - Should raise ValueError with message about not using both together
        - Should fail before attempting to validate any files
        """
        with pytest.raises(ValueError, match="Cannot use partition_date and start_partition_date or end_partition_date together."):
            _validate_filepaths(
                "test_service",
                ["test.parquet"],
                partition_date="2024-03-01",
                start_partition_date="2024-03-01",
                end_partition_date="2024-03-02"
            )

    def test_partition_date_no_matches(self):
        """Test validation when partition_date matches no files.
        
        Expected behavior:
        - Should return empty list when no files match the partition date
        - Should not include files from other partition dates
        - Should not raise any errors
        """
        test_files = [
            "/data/test_service/active/partition_date=2024-03-01/file1.parquet",
            "/data/test_service/active/partition_date=2024-03-02/file2.parquet"
        ]
        result = _validate_filepaths(
            "test_service",
            test_files,
            partition_date="2024-03-03"
        )
        assert result == []

    def test_partition_date_with_matches(self):
        """Test validation when partition_date matches files.
        
        Expected behavior:
        - Should return only files matching the exact partition date
        - Should maintain file paths in their original format
        - Should not include files from other partition dates
        """
        test_files = [
            "/data/test_service/active/partition_date=2024-03-01/file1.parquet",
            "/data/test_service/active/partition_date=2024-03-02/file2.parquet"
        ]
        result = _validate_filepaths(
            "test_service",
            test_files,
            partition_date="2024-03-01"
        )
        assert result == ["/data/test_service/active/partition_date=2024-03-01/file1.parquet"]

    def test_date_range_no_matches(self):
        """Test validation when date range matches no files.
        
        Expected behavior:
        - Should return empty list when no files fall within date range
        - Should not include files outside the date range
        - Should not raise any errors
        """
        test_files = [
            "/data/test_service/active/partition_date=2024-03-01/file1.parquet",
            "/data/test_service/active/partition_date=2024-03-02/file2.parquet"
        ]
        result = _validate_filepaths(
            "test_service",
            test_files,
            start_partition_date="2024-03-03",
            end_partition_date="2024-03-04"
        )
        assert result == []

    def test_date_range_with_matches(self):
        """Test validation when date range matches files.
        
        Expected behavior:
        - Should return all files with partition dates within range (inclusive)
        - Should maintain file paths in their original format
        - Should not include files outside the date range
        - Should handle multiple matching files correctly
        """
        test_files = [
            "/data/test_service/active/partition_date=2024-03-01/file1.parquet",
            "/data/test_service/active/partition_date=2024-03-02/file2.parquet",
            "/data/test_service/active/partition_date=2024-03-03/file3.parquet"
        ]
        result = _validate_filepaths(
            "test_service",
            test_files,
            start_partition_date="2024-03-01",
            end_partition_date="2024-03-02"
        )
        assert set(result) == {
            "/data/test_service/active/partition_date=2024-03-01/file1.parquet",
            "/data/test_service/active/partition_date=2024-03-02/file2.parquet"
        }

    def test_only_start_date(self):
        """Test validation when only start_partition_date is provided.
        
        Expected behavior:
        - Should raise ValueError with message about requiring both dates
        - Should fail before attempting to validate any files
        """
        with pytest.raises(ValueError, match="Both start_partition_date and end_partition_date must be provided together."):
            _validate_filepaths(
                "test_service",
                ["test.parquet"],
                start_partition_date="2024-03-01"
            )

    def test_only_end_date(self):
        """Test validation when only end_partition_date is provided.
        
        Expected behavior:
        - Should raise ValueError with message about requiring both dates
        - Should fail before attempting to validate any files
        """
        with pytest.raises(ValueError, match="Both start_partition_date and end_partition_date must be provided together."):
            _validate_filepaths(
                "test_service",
                ["test.parquet"],
                end_partition_date="2024-03-02"
            )


class TestListFilenames:
    """Test suite for list_filenames function."""
    
    @pytest.fixture(autouse=True)
    def mock_service_metadata(self, mocker):
        """Mock the service metadata for test services."""
        mock_metadata = {
            "preprocessed_posts": {
                "local_prefix": "/data/preprocessed_posts",
                "subpaths": {
                    "firehose": "firehose",
                    "most_liked": "most_liked"
                }
            },
            "posts": {
                "local_prefix": "/data/posts"
            }
        }
        mocker.patch("lib.db.manage_local_data.MAP_SERVICE_TO_METADATA", mock_metadata)
        return mock_metadata

    def test_deprecated_format_partition_date(self, mocker):
        """Test listing files for service that uses both formats with partition_date filter.
        
        Tests that list_filenames correctly filters files for a service using both deprecated
        and current formats (preprocessed_posts) when given a specific partition_date.
        
        Expected behavior:
        - Should call both _get_all_filenames_deprecated_format and _get_all_filenames
        - Should return only files matching partition_date="2024-03-02"
        - Should include files from both formats
        - Should exclude files from other partition dates
        """
        mock_get_deprecated = mocker.patch("lib.db.manage_local_data._get_all_filenames_deprecated_format")
        mock_get_current = mocker.patch("lib.db.manage_local_data._get_all_filenames")
        
        mock_get_deprecated.return_value = [
            "/data/preprocessed_posts/firehose/active/partition_date=2024-03-01/file3.parquet",
            "/data/preprocessed_posts/firehose/active/partition_date=2024-03-02/file1.parquet",
            "/data/preprocessed_posts/most_liked/active/partition_date=2024-03-02/file2.parquet",
            "/data/preprocessed_posts/most_liked/active/partition_date=2024-03-03/file4.parquet"
        ]
        
        mock_get_current.return_value = [
            "/data/preprocessed_posts/active/partition_date=2024-03-01/file5.parquet",
            "/data/preprocessed_posts/active/partition_date=2024-03-02/file6.parquet",
            "/data/preprocessed_posts/active/partition_date=2024-03-03/file7.parquet"
        ]

        result = list_filenames(
            service="preprocessed_posts",
            partition_date="2024-03-02"
        )

        assert len(result) == 3
        assert all("2024-03-02" in f for f in result)
        # Verify files from both formats are included
        assert any("firehose" in f for f in result)
        assert any("most_liked" in f for f in result)
        assert any(f == "/data/preprocessed_posts/active/partition_date=2024-03-02/file6.parquet" for f in result)
        
        mock_get_deprecated.assert_called_once()
        mock_get_current.assert_called_once()

    def test_current_format_partition_date(self, mocker):
        """Test listing files for current format service with partition_date filter.
        
        Tests that list_filenames correctly filters files for a service using only current format
        (posts) when given a specific partition_date.
        
        Expected behavior:
        - Should call only _get_all_filenames
        - Should NOT call _get_all_filenames_deprecated_format
        - Should return only files matching partition_date="2024-03-02"
        - Should include files from both cache and active directories if specified
        - Should exclude files from other partition dates and deprecated format
        """
        mock_get_current = mocker.patch("lib.db.manage_local_data._get_all_filenames")
        mock_get_deprecated = mocker.patch("lib.db.manage_local_data._get_all_filenames_deprecated_format")
        
        mock_get_current.return_value = [
            "/data/posts/active/partition_date=2024-03-01/file3.parquet",
            "/data/posts/active/partition_date=2024-03-02/file1.parquet",
            "/data/posts/cache/partition_date=2024-03-02/file2.parquet",
            "/data/posts/cache/partition_date=2024-03-03/file4.parquet"
        ]

        result = list_filenames(
            service="posts",
            directories=["active", "cache"],
            partition_date="2024-03-02"
        )

        assert len(result) == 2
        assert all("2024-03-02" in f for f in result)
        assert any("/active/" in f for f in result)
        assert any("/cache/" in f for f in result)
        
        mock_get_current.assert_called_once()
        mock_get_deprecated.assert_not_called()

    def test_deprecated_format_date_range(self, mocker):
        """Test listing files for service using both formats with date range filter.
        
        Tests that list_filenames correctly filters files for a service using both deprecated
        and current formats (preprocessed_posts) when given start and end partition dates.
        
        Expected behavior:
        - Should call both _get_all_filenames_deprecated_format and _get_all_filenames
        - Should return files with dates between 2024-03-01 and 2024-03-03 inclusive
        - Should include files from both formats
        - Should exclude files outside date range
        """
        mock_get_deprecated = mocker.patch("lib.db.manage_local_data._get_all_filenames_deprecated_format")
        mock_get_current = mocker.patch("lib.db.manage_local_data._get_all_filenames")
        
        mock_get_deprecated.return_value = [
            "/data/preprocessed_posts/firehose/active/partition_date=2024-02-28/file4.parquet",
            "/data/preprocessed_posts/firehose/active/partition_date=2024-03-01/file1.parquet",
            "/data/preprocessed_posts/firehose/active/partition_date=2024-03-02/file2.parquet",
            "/data/preprocessed_posts/most_liked/active/partition_date=2024-03-03/file3.parquet",
            "/data/preprocessed_posts/most_liked/active/partition_date=2024-03-04/file5.parquet"
        ]
        
        mock_get_current.return_value = [
            "/data/preprocessed_posts/active/partition_date=2024-02-28/file6.parquet",
            "/data/preprocessed_posts/active/partition_date=2024-03-01/file7.parquet",
            "/data/preprocessed_posts/active/partition_date=2024-03-03/file8.parquet",
            "/data/preprocessed_posts/active/partition_date=2024-03-04/file9.parquet"
        ]

        result = list_filenames(
            service="preprocessed_posts",
            start_partition_date="2024-03-01",
            end_partition_date="2024-03-03"
        )

        assert len(result) == 5  # 3 from deprecated + 2 from current format
        assert all("2024-03-0" in f for f in result)
        assert not any("2024-02-28" in f for f in result)
        assert not any("2024-03-04" in f for f in result)
        # Verify files from both formats are included
        assert any("firehose" in f for f in result)
        assert any("most_liked" in f for f in result)
        assert any(f == "/data/preprocessed_posts/active/partition_date=2024-03-01/file7.parquet" for f in result)
        
        mock_get_deprecated.assert_called_once()
        mock_get_current.assert_called_once()

    def test_current_format_date_range(self, mocker):
        """Test listing files for current format service with date range filter.
        
        Tests that list_filenames correctly filters files for a service using only current format
        (posts) when given start and end partition dates.
        
        Expected behavior:
        - Should call only _get_all_filenames
        - Should NOT call _get_all_filenames_deprecated_format
        - Should return files with dates between 2024-03-01 and 2024-03-03 inclusive
        - Should include files from both cache and active directories if specified
        - Should exclude files outside date range and deprecated format
        """
        mock_get_current = mocker.patch("lib.db.manage_local_data._get_all_filenames")
        mock_get_deprecated = mocker.patch("lib.db.manage_local_data._get_all_filenames_deprecated_format")
        
        mock_get_current.return_value = [
            "/data/posts/active/partition_date=2024-02-28/file4.parquet",
            "/data/posts/active/partition_date=2024-03-01/file1.parquet",
            "/data/posts/cache/partition_date=2024-03-02/file2.parquet",
            "/data/posts/active/partition_date=2024-03-03/file3.parquet",
            "/data/posts/cache/partition_date=2024-03-04/file5.parquet"
        ]

        result = list_filenames(
            service="posts",
            directories=["active", "cache"],
            start_partition_date="2024-03-01",
            end_partition_date="2024-03-03"
        )

        assert len(result) == 3
        assert all("2024-03-0" in f for f in result)
        assert not any("2024-02-28" in f for f in result)
        assert not any("2024-03-04" in f for f in result)
        
        mock_get_current.assert_called_once()
        mock_get_deprecated.assert_not_called()

class TestLoadDataFromLocalStorage:
    @pytest.fixture
    def mock_service_metadata(self, mocker):
        """Mock the service metadata for test_service"""
        mock_metadata = {
            "test_service": {
                "dtypes_map": {"col1": "int64", "col2": "string"},
                "local_prefix": "/data/test",
                "timestamp_field": "col1"
            }
        }
        mocker.patch("lib.db.manage_local_data.MAP_SERVICE_TO_METADATA", mock_metadata)

    @pytest.fixture
    def mock_duckdb_import(self, mocker):
        """Mock DuckDB class and methods"""
        # Instead of mocking duckdb directly, mock the DuckDB instance
        mock_duckdb_instance = mocker.MagicMock()
        mock_duckdb_instance.run_query_as_df.return_value = pd.DataFrame()
        mocker.patch("lib.db.manage_local_data.duckDB", mock_duckdb_instance)
        return mock_duckdb_instance

    @pytest.mark.parametrize("test_params", [
        {
            "service": "test_service", 
            "directory": "active",
            "export_format": "parquet",
            "mock_df": pd.DataFrame({
                "col1": [1, 2],
                "col2": ["a", "b"],
                "partition_date": ["2024-03-01", "2024-03-02"]
            })
        }
    ])
    def test_basic_functionality(self, test_params, mocker, mock_service_metadata):
        """Test basic load functionality with parquet files."""
        mock_read_parquet = mocker.patch("pandas.read_parquet")
        mock_list_filenames = mocker.patch("lib.db.manage_local_data.list_filenames")
        
        mock_read_parquet.return_value = test_params["mock_df"]
        mock_list_filenames.return_value = ["file1.parquet"]
        
        result = load_data_from_local_storage(
            service=test_params["service"],
            directory=test_params["directory"],
            export_format=test_params["export_format"]
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(test_params["mock_df"])
        mock_list_filenames.assert_called_once()
        mock_read_parquet.assert_called_once()

    @pytest.mark.parametrize("directory,expected_files", [
        ("active", ["active/file1.parquet"]),
        ("cache", ["cache/file1.parquet"])
    ])
    def test_directory_selection(self, directory, expected_files, mocker, mock_service_metadata):
        """Test loading from different directories."""
        mock_read_parquet = mocker.patch("pandas.read_parquet")
        mock_list_filenames = mocker.patch("lib.db.manage_local_data.list_filenames")
        
        mock_list_filenames.return_value = expected_files
        
        load_data_from_local_storage(
            service="test_service",
            directory=directory
        )
        
        mock_list_filenames.assert_called_with(
            service="test_service",
            directories=[directory],
            validate_pq_files=False,
            partition_date=None,
            start_partition_date=None,
            end_partition_date=None,
            override_local_prefix=None,
            custom_args=None
        )

    @pytest.mark.parametrize("export_format,mock_data", [
        ("parquet", pd.DataFrame({"col1": [1,2]}).astype({"col1": "Int64"})),
        ("jsonl", pd.DataFrame({"col1": [1,2]}).astype({"col1": "Int64"})),
        ("duckdb", pd.DataFrame({"col1": [1,2]}).astype({"col1": "Int64"}))
    ])
    def test_export_formats(self, export_format, mock_data, mocker, mock_service_metadata, mock_duckdb_import):
        """Test different export formats."""
        mock_read_parquet = mocker.patch("pandas.read_parquet")
        mock_read_json = mocker.patch("pandas.read_json")
        mock_list_filenames = mocker.patch("lib.db.manage_local_data.list_filenames")
        
        mock_list_filenames.return_value = ["file1"]
        if export_format == "parquet":
            mock_read_parquet.return_value = mock_data
        elif export_format == "jsonl":
            mock_read_json.return_value = mock_data
        else:
            mock_duckdb_import.run_query_as_df.return_value = mock_data
            
        result = load_data_from_local_storage(
            service="test_service",
            export_format=export_format,
            duckdb_query="SELECT * FROM test" if export_format == "duckdb" else None,
            query_metadata={"tables": [{"name": "test", "columns": ["col1"]}]} if export_format == "duckdb" else None
        )
        
        assert isinstance(result, pd.DataFrame)
        if export_format == "duckdb":
            mock_duckdb_import.run_query_as_df.assert_called_once()

    def test_duckdb_query(self, mocker, mock_service_metadata, mock_duckdb_import):
        """Test DuckDB query execution."""
        mock_list_filenames = mocker.patch("lib.db.manage_local_data.list_filenames")
        
        test_query = "SELECT * FROM test"
        test_metadata = {"tables": [{"name": "test", "columns": ["col1"]}]}
        
        load_data_from_local_storage(
            service="test_service",
            export_format="duckdb",
            duckdb_query=test_query,
            query_metadata=test_metadata
        )
        
        mock_duckdb_import.run_query_as_df.assert_called_with(
            query=test_query,
            mode="parquet",
            filepaths=mock_list_filenames.return_value,
            query_metadata=test_metadata
        )

    @pytest.mark.parametrize("test_params", [
        {
            "latest_timestamp": "2024-03-02T00:00:00",
            "mock_data": pd.DataFrame({
                "col1": ["2024-03-02T12:00:00", "2024-03-03T12:00:00"],
                "col2": ["a", "b"]
            }).astype({"col1": "string", "col2": "string"})
        }
    ])
    def test_timestamp_filter(self, test_params, mocker, mock_service_metadata):
        """Test filtering by latest timestamp."""
        mock_read_parquet = mocker.patch("pandas.read_parquet")
        mock_list_filenames = mocker.patch("lib.db.manage_local_data.list_filenames")

        mock_df = test_params["mock_data"].copy()
        mock_df["partition_date"] = pd.Series(["2024-03-02", "2024-03-03"], dtype="string")
        mock_read_parquet.return_value = mock_df
        
        result = load_data_from_local_storage(
            service="test_service",
            latest_timestamp=test_params["latest_timestamp"]
        )
        
        assert all(pd.to_datetime(result["col1"]) >= pd.to_datetime(test_params["latest_timestamp"]))
        pd.testing.assert_frame_equal(result, test_params["mock_data"])

    @pytest.mark.parametrize("test_params", [
        {
            "partition_date": "2024-03-02",
            "mock_data": pd.DataFrame({
                "col1": [1, 2],
                "col2": ["a", "b"]
            }).astype({"col1": "Int64", "col2": "string"})
        }
    ])
    def test_partition_date_filter(self, test_params, mocker, mock_service_metadata):
        """Test filtering by partition date."""
        mock_list_filenames = mocker.patch("lib.db.manage_local_data.list_filenames")
        mock_read_parquet = mocker.patch("pandas.read_parquet")
        
        mock_df = test_params["mock_data"].copy()
        mock_df["partition_date"] = pd.Series(["2024-03-02", "2024-03-02"], dtype="string")
        mock_read_parquet.return_value = mock_df
        
        result = load_data_from_local_storage(
            service="test_service",
            partition_date=test_params["partition_date"]
        )
        
        pd.testing.assert_frame_equal(result, test_params["mock_data"])
        
        mock_list_filenames.assert_called_with(
            service="test_service",
            directories=["active"],
            validate_pq_files=False,
            partition_date=test_params["partition_date"],
            start_partition_date=None,
            end_partition_date=None,
            override_local_prefix=None,
            custom_args=None
        )

    @pytest.mark.parametrize("test_params", [
        {
            "start_date": "2024-03-01",
            "end_date": "2024-03-02",
            "files": ["2024-02-28.parquet", "2024-03-01.parquet", "2024-03-02.parquet", "2024-03-03.parquet"],
            "expected_files": ["2024-03-01.parquet", "2024-03-02.parquet"]
        }
    ])
    def test_date_range_filter(self, test_params, mocker, mock_service_metadata):
        """Test filtering by date range."""
        mock_list_filenames = mocker.patch("lib.db.manage_local_data.list_filenames")
        mock_read_parquet = mocker.patch("pandas.read_parquet")
        
        load_data_from_local_storage(
            service="test_service",
            start_partition_date=test_params["start_date"],
            end_partition_date=test_params["end_date"]
        )
        
        mock_list_filenames.assert_called_with(
            service="test_service",
            directories=["active"],
            validate_pq_files=False,
            partition_date=None,
            start_partition_date=test_params["start_date"],
            end_partition_date=test_params["end_date"],
            override_local_prefix=None,
            custom_args=None
        )

    def test_use_all_data(self, mocker, mock_service_metadata):
        """Test use_all_data flag."""
        mock_list_filenames = mocker.patch("lib.db.manage_local_data.list_filenames")
        mock_read_parquet = mocker.patch("pandas.read_parquet")
        
        load_data_from_local_storage(
            service="test_service",
            use_all_data=True
        )
        
        mock_list_filenames.assert_called_with(
            service="test_service",
            directories=["cache", "active"],
            validate_pq_files=False,
            partition_date=None,
            start_partition_date=None,
            end_partition_date=None,
            override_local_prefix=None,
            custom_args=None
        )

    def test_validate_pq_files(self, mocker, mock_service_metadata):
        """Test validate_pq_files flag."""
        mock_list_filenames = mocker.patch("lib.db.manage_local_data.list_filenames")
        mock_read_parquet = mocker.patch("pandas.read_parquet")
        
        load_data_from_local_storage(
            service="test_service",
            validate_pq_files=True
        )
        
        mock_list_filenames.assert_called_with(
            service="test_service",
            directories=["active"],
            validate_pq_files=True,
            partition_date=None,
            start_partition_date=None,
            end_partition_date=None,
            override_local_prefix=None,
            custom_args=None
        )

    def test_preprocessed_posts_validation(self, mocker):
        """Test loading preprocessed_posts data with validation.
        
        Tests that load_data_from_local_storage correctly:
        1. Calls both _get_all_filenames_deprecated_format and _get_all_filenames for preprocessed_posts
        2. Validates the combined filepaths using _validate_filepaths
        3. Filters out files not matching the validation criteria
        
        Expected behavior:
        - Should call both filename functions for preprocessed_posts
        - Should validate combined filepaths with _validate_filepaths
        - Should only return files matching validation criteria
        - Should handle both deprecated and current format paths
        """
        # Mock the filename retrieval functions
        mock_get_deprecated = mocker.patch("lib.db.manage_local_data._get_all_filenames_deprecated_format")
        mock_get_current = mocker.patch("lib.db.manage_local_data._get_all_filenames")
        mock_validate = mocker.patch("lib.db.manage_local_data._validate_filepaths")
        mock_read_parquet = mocker.patch("pandas.read_parquet")

        # Set up test files in both formats
        deprecated_files = [
            "/data/preprocessed_posts/firehose/active/partition_date=2024-03-01/file1.parquet",
            "/data/preprocessed_posts/most_liked/active/partition_date=2024-03-02/file2.parquet",
            "/data/preprocessed_posts/firehose/active/partition_date=2024-03-03/file3.parquet"
        ]
        
        current_files = [
            "/data/preprocessed_posts/active/partition_date=2024-03-01/file4.parquet",
            "/data/preprocessed_posts/active/partition_date=2024-03-02/file5.parquet",
            "/data/preprocessed_posts/active/partition_date=2024-03-03/file6.parquet"
        ]

        # Mock return values
        mock_get_deprecated.return_value = deprecated_files
        mock_get_current.return_value = current_files
        
        # Mock validation to filter some files
        validated_files = [
            "/data/preprocessed_posts/firehose/active/partition_date=2024-03-01/file1.parquet",
            "/data/preprocessed_posts/active/partition_date=2024-03-01/file4.parquet",
            "/data/preprocessed_posts/active/partition_date=2024-03-02/file5.parquet"
        ]
        mock_validate.return_value = validated_files
        
        # Call function with validation criteria
        result = load_data_from_local_storage(
            service="preprocessed_posts",
            start_partition_date="2024-03-01",
            end_partition_date="2024-03-02"
        )

        # Verify correct functions were called
        mock_get_deprecated.assert_called_once()
        mock_get_current.assert_called_once()
        mock_validate.assert_called_once_with(
            service="preprocessed_posts",
            filepaths=deprecated_files + current_files,
            partition_date=None,
            start_partition_date="2024-03-01", 
            end_partition_date="2024-03-02"
        )

    def test_standard_service_validation(self, mocker):
        """Test loading data for a standard service with validation.
        
        Tests that load_data_from_local_storage correctly:
        1. Only calls _get_all_filenames for standard services
        2. Validates filepaths using _validate_filepaths
        3. Filters out files not matching validation criteria
        
        Expected behavior:
        - Should only call _get_all_filenames (not deprecated format)
        - Should validate filepaths with _validate_filepaths
        - Should only return files matching validation criteria
        - Should handle standard format paths
        """
        # Mock the filename retrieval functions
        mock_get_deprecated = mocker.patch("lib.db.manage_local_data._get_all_filenames_deprecated_format")
        mock_get_current = mocker.patch("lib.db.manage_local_data._get_all_filenames")
        mock_validate = mocker.patch("lib.db.manage_local_data._validate_filepaths")
        mock_read_parquet = mocker.patch("pandas.read_parquet")

        # Set up test files
        test_files = [
            "/data/fetch_posts_used_in_feeds/active/partition_date=2024-03-01/file1.parquet",
            "/data/fetch_posts_used_in_feeds/active/partition_date=2024-03-02/file2.parquet",
            "/data/fetch_posts_used_in_feeds/active/partition_date=2024-03-03/file3.parquet"
        ]

        # Mock return values
        mock_get_current.return_value = test_files
        
        # Mock validation to filter some files
        validated_files = [
            "/data/fetch_posts_used_in_feeds/active/partition_date=2024-03-01/file1.parquet",
            "/data/fetch_posts_used_in_feeds/active/partition_date=2024-03-02/file2.parquet"
        ]
        mock_validate.return_value = validated_files

        # Call function with validation criteria
        result = load_data_from_local_storage(
            service="fetch_posts_used_in_feeds",
            start_partition_date="2024-03-01",
            end_partition_date="2024-03-02"
        )

        # Verify correct functions were called
        mock_get_deprecated.assert_not_called()
        mock_get_current.assert_called_once()
        mock_validate.assert_called_once_with(
            service="fetch_posts_used_in_feeds",
            filepaths=test_files,
            partition_date=None,
            start_partition_date="2024-03-01",
            end_partition_date="2024-03-02"
        )

    def test_override_local_prefix(self, mocker, mock_service_metadata):
        """Test loading data with overridden local prefix."""
        mock_read_parquet = mocker.patch("pandas.read_parquet")
        mock_list_filenames = mocker.patch("lib.db.manage_local_data.list_filenames")
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        mock_read_parquet.return_value = mock_df
        
        override_path = "/custom/path"
        load_data_from_local_storage(
            service="test_service",
            override_local_prefix=override_path
        )
        
        mock_list_filenames.assert_called_with(
            service="test_service",
            directories=["active"],
            validate_pq_files=False,
            partition_date=None,
            start_partition_date=None,
            end_partition_date=None,
            override_local_prefix=override_path,
            custom_args=None
        )

class TestExportDataToLocalStorage:
    """Tests for export_data_to_local_storage function."""

    @pytest.fixture
    def mock_service_metadata(self, mocker):
        """Mock service metadata for testing."""
        mock_metadata = {
            "test_service": {
                "local_prefix": "/data/test_service",
                "timestamp_field": "preprocessing_timestamp",
                "partition_key": "preprocessing_timestamp",
                "subpaths": {
                    "firehose": "/data/test_service/firehose",
                    "active": "/data/test_service/active",
                }
            },
            "preprocessed_posts": {
                "local_prefix": "/data/preprocessed_posts",
                "timestamp_field": "preprocessing_timestamp",
                "partition_key": "preprocessing_timestamp",
                "subpaths": {
                    "firehose": "/data/preprocessed_posts/firehose",
                    "active": "/data/preprocessed_posts/active",
                }
            },
            "backfill_sync": {
                "local_prefix": "/data/backfill_sync",
                "timestamp_field": "preprocessing_timestamp",
                "partition_key": "preprocessing_timestamp",
                "skip_date_validation": True,
                "subpaths": {
                    "raw_sync": "/data/backfill_sync/raw_sync",
                    "active": "/data/backfill_sync/active",
                }
            }
        }
        mocker.patch("lib.db.manage_local_data.MAP_SERVICE_TO_METADATA", mock_metadata)
        return mock_metadata

    @pytest.fixture
    def mock_df(self):
        """Create a mock DataFrame for testing."""
        return pd.DataFrame({
            "col1": [1, 2],
            "col2": ["a", "b"],
            "preprocessing_timestamp": ["2024-01-01", "2024-01-01"],
            "record_type": ["post", "post"]
        })

    def test_basic_export(self, mocker, mock_service_metadata, mock_df, tmp_path):
        """Test basic export functionality."""
        mocker.patch("lib.db.manage_local_data.partition_data_by_date", return_value=[{
            "start_timestamp": "2024-01-01",
            "end_timestamp": "2024-01-01",
            "data": mock_df
        }])
        mocker.patch("os.makedirs")
        mock_to_parquet = mocker.patch.object(pd.DataFrame, "to_parquet")
        
        export_data_to_local_storage(
            service="test_service",
            df=mock_df
        )
        
        mock_to_parquet.assert_called_once()

    def test_override_local_prefix(self, mocker, mock_service_metadata, mock_df, tmp_path):
        """Test export with overridden local prefix."""
        mocker.patch("lib.db.manage_local_data.partition_data_by_date", return_value=[{
            "start_timestamp": "2024-01-01",
            "end_timestamp": "2024-01-01",
            "data": mock_df
        }])
        mock_makedirs = mocker.patch("os.makedirs")
        mock_to_parquet = mocker.patch.object(pd.DataFrame, "to_parquet")
        
        override_path = "/custom/path"
        export_data_to_local_storage(
            service="test_service",
            df=mock_df,
            override_local_prefix=override_path
        )
        
        # Verify the override path was used
        assert any(call_args[0][0].startswith(override_path) 
                  for call_args in mock_makedirs.call_args_list)
        mock_to_parquet.assert_called_once()

    def test_export_with_custom_args(self, mocker, mock_service_metadata, mock_df):
        """Test export with custom arguments."""
        mocker.patch("lib.db.manage_local_data.partition_data_by_date", return_value=[{
            "start_timestamp": "2024-01-01",
            "end_timestamp": "2024-01-01",
            "data": mock_df
        }])
        mocker.patch("os.makedirs")
        mock_to_parquet = mocker.patch.object(pd.DataFrame, "to_parquet")
        
        custom_args = {"source": "firehose"}
        export_data_to_local_storage(
            service="preprocessed_posts",
            df=mock_df,
            custom_args=custom_args
        )
        
        mock_to_parquet.assert_called_once()

    def test_export_formats(self, mocker, mock_service_metadata, mock_df):
        """Test different export formats."""
        mocker.patch("lib.db.manage_local_data.partition_data_by_date", return_value=[{
            "start_timestamp": "2024-01-01",
            "end_timestamp": "2024-01-01",
            "data": mock_df
        }])
        mocker.patch("os.makedirs")
        mock_to_json = mocker.patch.object(pd.DataFrame, "to_json")
        mock_to_parquet = mocker.patch.object(pd.DataFrame, "to_parquet")
        
        # Test JSONL format
        export_data_to_local_storage(
            service="test_service",
            df=mock_df,
            export_format="jsonl"
        )
        mock_to_json.assert_called_once()
        
        # Test Parquet format
        export_data_to_local_storage(
            service="test_service",
            df=mock_df,
            export_format="parquet"
        )
        mock_to_parquet.assert_called_once()
        
    def test_backfill_sync_service(self, mocker, mock_service_metadata, mock_df):
        """Test export with backfill_sync service."""
        # Mock the partition_data_by_date function
        mock_partition = mocker.patch("lib.db.manage_local_data.partition_data_by_date", return_value=[{
            "start_timestamp": "2024-01-01", 
            "end_timestamp": "2024-01-01",
            "data": mock_df
        }])
        mocker.patch("os.makedirs")
        mock_to_parquet = mocker.patch.object(pd.DataFrame, "to_parquet")
        
        # Export to the raw_sync subdirectory
        custom_args = {"source": "raw_sync"}
        export_data_to_local_storage(
            service="backfill_sync",
            df=mock_df,
            custom_args=custom_args
        )
        
        # Verify the export occurred to the correct path
        mock_to_parquet.assert_called_once()
        file_path = mock_to_parquet.call_args[1]['path']
        assert file_path == '/data/backfill_sync/raw_sync/cache'
        
        # Verify that partition_data_by_date was called
        mock_partition.assert_called_once()
        
    def test_skip_date_validation(self, mocker, mock_service_metadata, mock_df):
        """Test export with skip_date_validation=True from configuration."""
        # Mock the date validation function to track if it's called
        mock_partition = mocker.patch("lib.db.manage_local_data.partition_data_by_date")
        mocker.patch.dict("lib.db.manage_local_data.MAP_SERVICE_TO_METADATA", {"backfill_sync": {"skip_date_validation": True, "timestamp_field": "preprocessing_timestamp", "local_prefix": "/data/backfill_sync"}})
        mocker.patch("os.makedirs")
        mock_to_parquet = mocker.patch.object(pd.DataFrame, "to_parquet")
        
        # Test export with backfill_sync service which has skip_date_validation=True
        export_data_to_local_storage(
            service="backfill_sync",
            df=mock_df
        )
        
        # Verify that partition_data_by_date was NOT called due to skipping validation
        mock_partition.assert_not_called()
        
        # Verify the export still occurred
        mock_to_parquet.assert_called_once()
