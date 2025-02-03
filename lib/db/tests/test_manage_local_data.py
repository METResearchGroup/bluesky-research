"""Tests for lib/db/manage_local_data.py"""
import os
from tempfile import TemporaryDirectory

import pandas as pd
import pytest
from unittest.mock import Mock, patch

from lib.aws.s3 import S3
from lib.db.manage_local_data import (
    find_files_after_timestamp,
    list_filenames,
    load_data_from_local_storage,
)


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

class TestListFilenames:
    """Test suite for list_filenames function."""
    
    @pytest.fixture(autouse=True)
    def mock_service_metadata(self, mocker):
        """Mock the service metadata for test_service."""
        mock_metadata = {
            "test_service": {
                "local_prefix": "/data/service",
                "dtypes_map": {"col1": "int64", "col2": "string"},
                "timestamp_field": "col1"
            }
        }
        mocker.patch("lib.db.manage_local_data.MAP_SERVICE_TO_METADATA", mock_metadata)
        return mock_metadata

    @pytest.fixture
    def mock_file_structure(self):
        """Mock file structure for testing."""
        return [
            "/data/service/active/partition_date=2024-03-01/file1.parquet",
            "/data/service/active/partition_date=2024-03-02/file2.parquet", 
            "/data/service/active/partition_date=2024-03-03/file3.parquet",
            "/data/service/cache/partition_date=2024-02-28/file4.parquet",
            "/data/service/cache/partition_date=2024-03-01/file5.parquet"
        ]

    def test_basic_functionality(self, mock_file_structure):
        """Test basic file listing functionality."""
        with patch('os.walk') as mock_walk:
            # Only return active directory files
            active_files = [f for f in mock_file_structure if "/active/" in f]
            mock_walk.return_value = [
                ("/data/service/active", [], active_files)
            ]
            
            result = list_filenames(service="test_service")
            
            assert len(result) == 3
            assert all("active" in f for f in result)

    def test_nested_directories(self):
        """Test handling of nested directory structures."""
        nested_files = [
            "/data/service/active/2024/03/01/file1.parquet",
            "/data/service/active/2024/03/02/file2.parquet"
        ]
        
        with patch('os.walk') as mock_walk:
            mock_walk.return_value = [
                ("/data/service/active", ["2024"], []),
                ("/data/service/active/2024", ["03"], []),
                ("/data/service/active/2024/03", ["01", "02"], []),
                ("/data/service/active/2024/03/01", [], ["file1.parquet"]),
                ("/data/service/active/2024/03/02", [], ["file2.parquet"])
            ]
            
            result = list_filenames(service="test_service")
            
            assert len(result) == 2
            assert all(f.endswith(".parquet") for f in result)

    @pytest.mark.parametrize("directories,expected_count", [
        (["active"], 3),
        (["cache"], 2),
        (["cache", "active"], 5)
    ])
    def test_directory_filtering(self, mock_file_structure, directories, expected_count):
        """Test filtering by directory type."""
        with patch('os.walk') as mock_walk:
            # Create walk results for requested directories only
            walk_results = []
            for directory in directories:
                dir_files = [f for f in mock_file_structure if f"/{directory}/" in f]
                walk_results.append((
                    f"/data/service/{directory}",
                    ["partition_date=2024-03-01", "partition_date=2024-03-02", "partition_date=2024-03-03"],
                    [os.path.basename(f) for f in dir_files]
                ))
            
            mock_walk.return_value = walk_results
            
            result = list_filenames(service="test_service", directories=directories)
            
            assert len(result) == expected_count
            # Check that each file appears exactly once
            assert len(set(result)) == len(result)
            assert all(any(f"/{d}/" in f for d in directories) for f in result)

    def test_validate_pq_files(self, mock_file_structure):
        """Test parquet file validation."""
        with patch('os.walk') as mock_walk, \
             patch('pyarrow.parquet.ParquetFile') as mock_parquet_file:
            # Only return active directory files
            active_files = [f for f in mock_file_structure if "/active/" in f]
            mock_walk.return_value = [(
                "/data/service/active",
                [],
                [os.path.basename(f) for f in active_files]
            )]
            
            # Make ParquetFile validation succeed
            mock_parquet_file.return_value = True
            
            result = list_filenames(service="test_service", validate_pq_files=True)
            
            assert len(result) == 3
            assert mock_parquet_file.call_count == 3

    def test_partition_date_filter(self, mock_file_structure):
        """Test filtering by specific partition date."""
        with patch('os.walk') as mock_walk:
            # Only return active directory files
            active_files = [f for f in mock_file_structure if "/active/" in f]
            mock_walk.return_value = [("/data/service/active", [], active_files)]
            
            result = list_filenames(
                service="test_service",
                partition_date="2024-03-02"
            )
            
            assert len(result) == 1
            assert "2024-03-02" in result[0]

    def test_date_range_filter(self, mock_file_structure):
        """Test filtering by date range."""
        with patch('os.walk') as mock_walk:
            # Only return active directory files by default
            active_files = [f for f in mock_file_structure if "/active/" in f]
            mock_walk.return_value = [("/data/service/active", [], active_files)]
            
            result = list_filenames(
                service="test_service",
                start_partition_date="2024-03-01",
                end_partition_date="2024-03-02"
            )
            
            assert len(result) == 2
            assert all("2024-03-0" in f for f in result)
            assert all(f in result for f in active_files if "2024-03-01" in f or "2024-03-02" in f)

    def test_invalid_date_params(self):
        """Test error handling for invalid date parameter combinations."""
        with pytest.raises(ValueError) as exc_info:
            list_filenames(
                service="test_service",
                partition_date="2024-03-01",
                start_partition_date="2024-03-01",
                end_partition_date="2024-03-02"
            )
        
        assert "Cannot use partition_date and start_partition_date or end_partition_date together" in str(exc_info.value)

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
            end_partition_date=None
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
            end_partition_date=None
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
            end_partition_date=test_params["end_date"]
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
            end_partition_date=None
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
            end_partition_date=None
        )
