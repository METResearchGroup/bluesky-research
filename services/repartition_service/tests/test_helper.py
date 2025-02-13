"""Tests for helper.py.

This test suite verifies the functionality of repartitioning helper functions:
- verify_dataframes_equal: Tests DataFrame comparison
- get_service_paths: Tests path generation
- repartition_data_for_partition_date: Tests single date processing
- repartition_data_for_partition_dates: Tests multiple date processing
"""

import os
import unittest
from unittest.mock import patch, MagicMock, Mock, call, ANY
import datetime

import pandas as pd
import pytest

from services.repartition_service.helper import (
    verify_dataframes_equal,
    get_service_paths,
    repartition_data_for_partition_date,
    repartition_data_for_partition_dates,
    VerificationError,
    OperationStatus,
    OperationResult,
)

# Mock service metadata for testing
MOCK_SERVICE_METADATA = {
    "test_service": {
        "local_prefix": "/data/test_service",
        "timestamp_field": "preprocessing_timestamp",
    }
}

@pytest.fixture(autouse=True)
def mock_service_metadata():
    """Automatically mock service metadata for all tests."""
    with patch("services.repartition_service.helper.MAP_SERVICE_TO_METADATA", MOCK_SERVICE_METADATA):
        yield

@pytest.fixture
def mock_df():
    """Create a mock DataFrame for testing."""
    return pd.DataFrame({
        "uri": ["post1", "post2"],
        "preprocessing_timestamp": ["2024-01-01", "2024-01-01"],
        "text": ["text1", "text2"]
    })


class TestVerifyDataFramesEqual:
    """Tests for verify_dataframes_equal function."""

    def test_identical_dataframes(self, mock_df):
        """Test comparison of identical DataFrames."""
        assert verify_dataframes_equal(mock_df, mock_df.copy())

    def test_different_length_dataframes(self, mock_df):
        """Test comparison of DataFrames with different lengths."""
        df2 = mock_df.iloc[0:1]
        assert not verify_dataframes_equal(mock_df, df2)

    def test_different_content_dataframes(self, mock_df):
        """Test comparison of DataFrames with different content."""
        df2 = mock_df.copy()
        df2.loc[0, "text"] = "different"
        assert not verify_dataframes_equal(mock_df, df2)


class TestGetServicePaths(unittest.TestCase):
    """Tests for get_service_paths function."""

    @patch("services.repartition_service.helper.MAP_SERVICE_TO_METADATA")
    @patch("os.makedirs")
    def test_path_generation(self, mock_makedirs, mock_metadata):
        """Test generation of service paths."""
        mock_metadata.__getitem__.return_value = {
            "local_prefix": "/data/test_service"
        }
        
        paths = get_service_paths("test_service", "2024-01-01")
        
        assert "original" in paths
        assert "backup" in paths
        assert "temp" in paths
        assert all("partition_date=2024-01-01" in path for path in paths.values())
        assert mock_makedirs.call_count == 3

    @patch("services.repartition_service.helper.MAP_SERVICE_TO_METADATA")
    def test_unknown_service(self, mock_metadata):
        """Test handling of unknown service."""
        mock_metadata.__getitem__.side_effect = KeyError
        
        with pytest.raises(KeyError):
            get_service_paths("unknown_service", "2024-01-01")


class TestRepartitionDataForPartitionDate(unittest.TestCase):
    """Tests for repartition_data_for_partition_date function."""

    def test_empty_data(self):
        """Test handling of empty data during repartitioning."""
        mock_load = Mock(return_value=pd.DataFrame())
        mock_export = Mock()
        mock_rmtree = Mock()
        mock_exists = Mock(return_value=True)
        mock_makedirs = Mock()

        with patch('services.repartition_service.helper.load_data_from_local_storage', mock_load), \
             patch('services.repartition_service.helper.export_data_to_local_storage', mock_export), \
             patch('services.repartition_service.helper.shutil.rmtree', mock_rmtree), \
             patch('services.repartition_service.helper.root_local_data_directory', '/mock/data'), \
             patch('services.repartition_service.helper.os.path.exists', mock_exists), \
             patch('services.repartition_service.helper.os.makedirs', mock_makedirs):

            result = repartition_data_for_partition_date('test_service', '2024-01-01', new_service_partition_key='preprocessing_timestamp')

            mock_load.assert_called_once_with(
                service='test_service',
                partition_date='2024-01-01',
                output_format='df'
            )
            mock_export.assert_not_called()
            mock_rmtree.assert_called_once_with(ANY)
            assert result.status == OperationStatus.SUCCESS

    def test_successful_repartition(self):
        """Test successful data repartitioning."""
        mock_df = pd.DataFrame({'data': [1, 2, 3]})
        mock_load = Mock(side_effect=[mock_df, mock_df, mock_df])
        mock_export = Mock()
        mock_rmtree = Mock()
        mock_exists = Mock(return_value=True)
        mock_makedirs = Mock()

        with patch('services.repartition_service.helper.load_data_from_local_storage', mock_load), \
             patch('services.repartition_service.helper.export_data_to_local_storage', mock_export), \
             patch('services.repartition_service.helper.shutil.rmtree', mock_rmtree), \
             patch('services.repartition_service.helper.root_local_data_directory', '/mock/data'), \
             patch('services.repartition_service.helper.os.path.exists', mock_exists), \
             patch('services.repartition_service.helper.os.makedirs', mock_makedirs):

            result = repartition_data_for_partition_date('test_service', '2024-01-01', new_service_partition_key='preprocessing_timestamp')

            mock_load.assert_has_calls([
                call(service='test_service', partition_date='2024-01-01', output_format='df'),
                call(service='test_service', partition_date='2024-01-01', output_format='df', override_local_prefix='/mock/data/old_test_service'),
                call(service='test_service', partition_date='2024-01-01', output_format='df', override_local_prefix='/mock/data/tmp_test_service')
            ])
            assert mock_export.call_count == 3
            mock_rmtree.assert_has_calls([
                call('/data/test_service/cache/partition_date=2024-01-01'),
                call('/mock/data/tmp_test_service/cache/partition_date=2024-01-01')
            ])
            assert result.status == OperationStatus.SUCCESS

    def test_verification_failure(self):
        """Test handling of verification failure during repartitioning."""
        mock_df1 = pd.DataFrame({'data': [1, 2, 3]})
        mock_df2 = pd.DataFrame({'data': [4, 5, 6]})  # Different data to trigger verification failure
        mock_load = Mock(side_effect=[mock_df1, mock_df2])
        mock_export = Mock()
        mock_rmtree = Mock()
        mock_exists = Mock(return_value=True)
        mock_makedirs = Mock()

        with patch('services.repartition_service.helper.load_data_from_local_storage', mock_load), \
             patch('services.repartition_service.helper.export_data_to_local_storage', mock_export), \
             patch('services.repartition_service.helper.shutil.rmtree', mock_rmtree), \
             patch('services.repartition_service.helper.root_local_data_directory', '/mock/data'), \
             patch('services.repartition_service.helper.os.path.exists', mock_exists), \
             patch('services.repartition_service.helper.os.makedirs', mock_makedirs):

            result = repartition_data_for_partition_date('test_service', '2024-01-01', new_service_partition_key='preprocessing_timestamp')

            mock_load.assert_has_calls([
                call(service='test_service', partition_date='2024-01-01', output_format='df'),
                call(service='test_service', partition_date='2024-01-01', output_format='df', override_local_prefix='/mock/data/old_test_service')
            ])
            assert mock_export.call_count == 1
            mock_rmtree.assert_called_once_with(ANY)
            assert result.status == OperationStatus.FAILED


class TestRepartitionDataForPartitionDates(unittest.TestCase):
    """Tests for repartition_data_for_partition_dates function."""

    @patch("services.repartition_service.helper.get_partition_dates")
    @patch("services.repartition_service.helper.repartition_data_for_partition_date")
    @patch("services.repartition_service.helper.MAP_SERVICE_TO_METADATA")
    def test_multiple_dates(self, mock_metadata, mock_repartition_single, mock_get_dates):
        """Test processing multiple partition dates."""
        mock_get_dates.return_value = ["2024-01-01", "2024-01-02"]
        mock_metadata.__contains__.return_value = True
        
        repartition_data_for_partition_dates(
            start_date="2024-01-01",
            end_date="2024-01-02",
            service="test_service"
        )
        
        assert mock_repartition_single.call_count == 2

    @patch("services.repartition_service.helper.get_partition_dates")
    @patch("services.repartition_service.helper.repartition_data_for_partition_date")
    @patch("services.repartition_service.helper.MAP_SERVICE_TO_METADATA")
    def test_excluded_dates(self, mock_metadata, mock_repartition_single, mock_get_dates):
        """Test date exclusion functionality."""
        mock_get_dates.return_value = ["2024-01-01", "2024-01-03"]
        mock_metadata.__contains__.return_value = True
        
        repartition_data_for_partition_dates(
            start_date="2024-01-01",
            end_date="2024-01-03",
            service="test_service",
            exclude_partition_dates=["2024-01-02"]
        )
        
        mock_get_dates.assert_called_once_with(
            start_date="2024-01-01",
            end_date="2024-01-03",
            exclude_partition_dates=["2024-01-02"]
        )
        assert mock_repartition_single.call_count == 2

    def test_missing_service(self):
        """Test handling of missing service parameter."""
        with pytest.raises(ValueError, match="Service name is required"):
            repartition_data_for_partition_dates()

    @patch("services.repartition_service.helper.MAP_SERVICE_TO_METADATA")
    def test_unknown_service(self, mock_metadata):
        """Test handling of unknown service."""
        mock_metadata.__contains__.return_value = False
        
        with pytest.raises(ValueError, match="Unknown service"):
            repartition_data_for_partition_dates(service="unknown_service")

    @patch("services.repartition_service.helper.get_partition_dates")
    @patch("services.repartition_service.helper.repartition_data_for_partition_date")
    @patch("services.repartition_service.helper.MAP_SERVICE_TO_METADATA")
    def test_error_handling(self, mock_metadata, mock_repartition_single, mock_get_dates):
        """Test handling of errors during processing."""
        mock_get_dates.return_value = ["2024-01-01", "2024-01-02"]
        mock_metadata.__contains__.return_value = True
        mock_repartition_single.side_effect = [
            OperationResult(status=OperationStatus.FAILED, error="Test error"),
            OperationResult(status=OperationStatus.SUCCESS)
        ]
        
        # Should continue processing despite error
        repartition_data_for_partition_dates(
            start_date="2024-01-01",
            end_date="2024-01-02",
            service="test_service"
        )
        
        assert mock_repartition_single.call_count == 2 