"""Unit tests for aggregate_author_to_average_toxicity_across_days functions."""

import pytest
import pandas as pd
import os
from unittest.mock import patch, MagicMock
from datetime import datetime

from aggregate_author_to_average_toxicity_across_days import (
    aggregate_author_toxicity_across_days,
    export_aggregated_data,
)


class TestAggregateAuthorToxicityAcrossDays:
    """Test cases for aggregate_author_toxicity_across_days function."""

    @patch("aggregate_author_to_average_toxicity_across_days.load_data_from_local_storage")
    def test_aggregate_author_toxicity_across_days_success(self, mock_load_data):
        """Test successful aggregation across multiple days."""
        # Arrange - Create daily data for 3 days with overlapping authors
        # Day 1: author1 (2 posts, avg_tox=0.15, avg_out=0.10), author2 (1 post, avg_tox=0.3, avg_out=0.25)
        # Day 2: author1 (1 post, avg_tox=0.2, avg_out=0.15), author3 (2 posts, avg_tox=0.4, avg_out=0.35)
        # Day 3: author2 (1 post, avg_tox=0.25, avg_out=0.2), author3 (1 post, avg_tox=0.5, avg_out=0.45)
        
        daily_data_2024_10_01 = pd.DataFrame({
            "author_did": ["author1", "author2"],
            "partition_date": ["2024-10-01", "2024-10-01"],
            "average_toxicity": [0.15, 0.3],
            "average_outrage": [0.10, 0.25],
            "total_labeled_posts": [2, 1],
            "preprocessing_timestamp": ["2024-10-01T10:00:00Z", "2024-10-01T11:00:00Z"]
        })
        
        daily_data_2024_10_02 = pd.DataFrame({
            "author_did": ["author1", "author3"],
            "partition_date": ["2024-10-02", "2024-10-02"],
            "average_toxicity": [0.2, 0.4],
            "average_outrage": [0.15, 0.35],
            "total_labeled_posts": [1, 2],
            "preprocessing_timestamp": ["2024-10-02T10:00:00Z", "2024-10-02T11:00:00Z"]
        })
        
        daily_data_2024_10_03 = pd.DataFrame({
            "author_did": ["author2", "author3"],
            "partition_date": ["2024-10-03", "2024-10-03"],
            "average_toxicity": [0.25, 0.5],
            "average_outrage": [0.2, 0.45],
            "total_labeled_posts": [1, 1],
            "preprocessing_timestamp": ["2024-10-03T10:00:00Z", "2024-10-03T11:00:00Z"]
        })
        
        # Expected weighted averages:
        # author1: (0.15*2 + 0.2*1) / (2+1) = 0.5/3 = 0.1667, (0.10*2 + 0.15*1) / 3 = 0.35/3 = 0.1167
        # author2: (0.3*1 + 0.25*1) / (1+1) = 0.55/2 = 0.275, (0.25*1 + 0.2*1) / 2 = 0.45/2 = 0.225
        # author3: (0.4*2 + 0.5*1) / (2+1) = 1.3/3 = 0.4333, (0.35*2 + 0.45*1) / 3 = 1.15/3 = 0.3833
        
        expected_result = pd.DataFrame({
            "author_did": ["author3", "author2", "author1"],  # Sorted by total_labeled_posts descending
            "average_toxicity": [0.4333, 0.275, 0.1667],
            "average_outrage": [0.3833, 0.225, 0.1167],
            "total_labeled_posts": [3, 2, 3]  # author3: 2+1=3, author2: 1+1=2, author1: 2+1=3
        })
        
        def mock_load_side_effect(service, **kwargs):
            if service == "author_to_average_toxicity_outrage":
                partition_date = kwargs.get("partition_date")
                if partition_date == "2024-10-01":
                    return daily_data_2024_10_01.copy()
                elif partition_date == "2024-10-02":
                    return daily_data_2024_10_02.copy()
                elif partition_date == "2024-10-03":
                    return daily_data_2024_10_03.copy()
            return pd.DataFrame()
        
        mock_load_data.side_effect = mock_load_side_effect
        
        # Act
        partition_dates = ["2024-10-01", "2024-10-02", "2024-10-03"]
        actual_result = aggregate_author_toxicity_across_days(partition_dates)
        
        # Assert
        assert len(actual_result) == len(expected_result)
        assert list(actual_result.columns) == list(expected_result.columns)
        
        # Verify author3 data (highest post count)
        author3_actual = actual_result[actual_result["author_did"] == "author3"].iloc[0]
        author3_expected = expected_result[expected_result["author_did"] == "author3"].iloc[0]
        assert author3_actual["total_labeled_posts"] == author3_expected["total_labeled_posts"]
        assert author3_actual["average_toxicity"] == pytest.approx(author3_expected["average_toxicity"], rel=1e-3)
        assert author3_actual["average_outrage"] == pytest.approx(author3_expected["average_outrage"], rel=1e-3)
        
        # Verify author2 data
        author2_actual = actual_result[actual_result["author_did"] == "author2"].iloc[0]
        author2_expected = expected_result[expected_result["author_did"] == "author2"].iloc[0]
        assert author2_actual["total_labeled_posts"] == author2_expected["total_labeled_posts"]
        assert author2_actual["average_toxicity"] == pytest.approx(author2_expected["average_toxicity"], rel=1e-3)
        assert author2_actual["average_outrage"] == pytest.approx(author2_expected["average_outrage"], rel=1e-3)
        
        # Verify author1 data
        author1_actual = actual_result[actual_result["author_did"] == "author1"].iloc[0]
        author1_expected = expected_result[expected_result["author_did"] == "author1"].iloc[0]
        assert author1_actual["total_labeled_posts"] == author1_expected["total_labeled_posts"]
        assert author1_actual["average_toxicity"] == pytest.approx(author1_expected["average_toxicity"], rel=1e-3)
        assert author1_actual["average_outrage"] == pytest.approx(author1_expected["average_outrage"], rel=1e-3)
        
        # Verify sorting by total_labeled_posts descending
        assert actual_result["total_labeled_posts"].iloc[0] >= actual_result["total_labeled_posts"].iloc[1]
        assert actual_result["total_labeled_posts"].iloc[1] >= actual_result["total_labeled_posts"].iloc[2]

    @patch("aggregate_author_to_average_toxicity_across_days.load_data_from_local_storage")
    def test_aggregate_author_toxicity_across_days_empty_data(self, mock_load_data):
        """Test handling when no data is available for any partition dates."""
        # Arrange
        def mock_load_side_effect(service, **kwargs):
            return pd.DataFrame()
        
        mock_load_data.side_effect = mock_load_side_effect
        
        # Act
        partition_dates = ["2024-10-01", "2024-10-02", "2024-10-03"]
        actual_result = aggregate_author_toxicity_across_days(partition_dates)
        
        # Assert
        assert actual_result.empty
        # When DataFrame is empty, it may not have columns, so we check if it's empty first
        if not actual_result.empty:
            assert list(actual_result.columns) == ["author_did", "average_toxicity", "average_outrage", "total_labeled_posts"]

    @patch("aggregate_author_to_average_toxicity_across_days.load_data_from_local_storage")
    def test_aggregate_author_toxicity_across_days_partial_data(self, mock_load_data):
        """Test handling when data is available for only some partition dates."""
        # Arrange - Only day 1 and day 3 have data
        daily_data_2024_10_01 = pd.DataFrame({
            "author_did": ["author1"],
            "partition_date": ["2024-10-01"],
            "average_toxicity": [0.2],
            "average_outrage": [0.15],
            "total_labeled_posts": [2],
            "preprocessing_timestamp": ["2024-10-01T10:00:00Z"]
        })
        
        daily_data_2024_10_03 = pd.DataFrame({
            "author_did": ["author1"],
            "partition_date": ["2024-10-03"],
            "average_toxicity": [0.3],
            "average_outrage": [0.25],
            "total_labeled_posts": [1],
            "preprocessing_timestamp": ["2024-10-03T10:00:00Z"]
        })
        
        expected_result = pd.DataFrame({
            "author_did": ["author1"],
            "average_toxicity": [0.2333],  # (0.2*2 + 0.3*1) / (2+1) = 0.7/3
            "average_outrage": [0.1833],   # (0.15*2 + 0.25*1) / (2+1) = 0.55/3
            "total_labeled_posts": [3]     # 2 + 1
        })
        
        def mock_load_side_effect(service, **kwargs):
            if service == "author_to_average_toxicity_outrage":
                partition_date = kwargs.get("partition_date")
                if partition_date == "2024-10-01":
                    return daily_data_2024_10_01.copy()
                elif partition_date == "2024-10-02":
                    return pd.DataFrame()  # No data for this date
                elif partition_date == "2024-10-03":
                    return daily_data_2024_10_03.copy()
            return pd.DataFrame()
        
        mock_load_data.side_effect = mock_load_side_effect
        
        # Act
        partition_dates = ["2024-10-01", "2024-10-02", "2024-10-03"]
        actual_result = aggregate_author_toxicity_across_days(partition_dates)
        
        # Assert
        assert len(actual_result) == len(expected_result)
        assert actual_result["author_did"].iloc[0] == expected_result["author_did"].iloc[0]
        assert actual_result["total_labeled_posts"].iloc[0] == expected_result["total_labeled_posts"].iloc[0]
        assert actual_result["average_toxicity"].iloc[0] == pytest.approx(expected_result["average_toxicity"].iloc[0], rel=1e-3)
        assert actual_result["average_outrage"].iloc[0] == pytest.approx(expected_result["average_outrage"].iloc[0], rel=1e-3)

    @patch("aggregate_author_to_average_toxicity_across_days.load_data_from_local_storage")
    def test_aggregate_author_toxicity_across_days_large_load(self, mock_load_data):
        """Test aggregation with large dataset across multiple days."""
        # Arrange - Create data for 5 days with 10 authors, some appearing across multiple days
        # Authors 1-3: appear on all 5 days
        # Authors 4-6: appear on days 1, 3, 5
        # Authors 7-10: appear on single days only
        
        def create_daily_data(day_num, authors, toxicity_base, outrage_base, posts_per_author):
            return pd.DataFrame({
                "author_did": [f"author{i:02d}" for i in authors],
                "partition_date": [f"2024-10-{day_num:02d}"] * len(authors),
                "average_toxicity": [toxicity_base + (i % 10) * 0.05 for i in authors],
                "average_outrage": [outrage_base + (i % 8) * 0.05 for i in authors],
                "total_labeled_posts": [posts_per_author] * len(authors),
                "preprocessing_timestamp": [f"2024-10-{day_num:02d}T{i:02d}:00:00Z" for i in authors]
            })
        
        # Day 1: Authors 1-6 (6 authors)
        daily_data_1 = create_daily_data(1, range(1, 7), 0.1, 0.05, 2)
        # Day 2: Authors 1-3, 7 (4 authors)
        daily_data_2 = create_daily_data(2, [1, 2, 3, 7], 0.15, 0.08, 1)
        # Day 3: Authors 1-6, 8 (7 authors)
        daily_data_3 = create_daily_data(3, list(range(1, 7)) + [8], 0.2, 0.1, 3)
        # Day 4: Authors 1-3, 9 (4 authors)
        daily_data_4 = create_daily_data(4, [1, 2, 3, 9], 0.12, 0.06, 2)
        # Day 5: Authors 1-6, 10 (7 authors)
        daily_data_5 = create_daily_data(5, list(range(1, 7)) + [10], 0.18, 0.09, 1)
        
        def mock_load_side_effect(service, **kwargs):
            if service == "author_to_average_toxicity_outrage":
                partition_date = kwargs.get("partition_date")
                if partition_date == "2024-10-01":
                    return daily_data_1.copy()
                elif partition_date == "2024-10-02":
                    return daily_data_2.copy()
                elif partition_date == "2024-10-03":
                    return daily_data_3.copy()
                elif partition_date == "2024-10-04":
                    return daily_data_4.copy()
                elif partition_date == "2024-10-05":
                    return daily_data_5.copy()
            return pd.DataFrame()
        
        mock_load_data.side_effect = mock_load_side_effect
        
        # Act
        partition_dates = ["2024-10-01", "2024-10-02", "2024-10-03", "2024-10-04", "2024-10-05"]
        actual_result = aggregate_author_toxicity_across_days(partition_dates)
        
        # Assert
        expected_author_count = 10  # Authors 1-10
        assert len(actual_result) == expected_author_count
        
        # Verify all expected columns exist
        expected_columns = ["author_did", "average_toxicity", "average_outrage", "total_labeled_posts"]
        assert list(actual_result.columns) == expected_columns
        
        # Verify data types
        assert actual_result["author_did"].dtype == "object"
        assert actual_result["average_toxicity"].dtype in ["float64", "float32"]
        assert actual_result["average_outrage"].dtype in ["float64", "float32"]
        assert actual_result["total_labeled_posts"].dtype in ["int64", "int32"]
        
        # Verify toxicity and outrage values are in valid range [0, 1]
        assert (actual_result["average_toxicity"] >= 0).all()
        assert (actual_result["average_toxicity"] <= 1).all()
        assert (actual_result["average_outrage"] >= 0).all()
        assert (actual_result["average_outrage"] <= 1).all()
        
        # Verify post counts are positive
        assert (actual_result["total_labeled_posts"] > 0).all()
        
        # Verify all authors are unique
        assert actual_result["author_did"].nunique() == expected_author_count
        
        # Verify sorting by total_labeled_posts descending
        assert actual_result["total_labeled_posts"].is_monotonic_decreasing
        
        # Verify expected post counts for specific authors
        # Authors 1-3: appear on all 5 days (2+1+3+2+1 = 9 posts each)
        authors_1_3 = actual_result[actual_result["author_did"].isin(["author01", "author02", "author03"])]
        assert (authors_1_3["total_labeled_posts"] == 9).all()
        
        # Authors 4-6: appear on days 1, 3, 5 (2+3+1 = 6 posts each)
        authors_4_6 = actual_result[actual_result["author_did"].isin(["author04", "author05", "author06"])]
        assert (authors_4_6["total_labeled_posts"] == 6).all()
        
        # Authors 7-10: appear on single days only (1, 3, 2, 1 posts respectively)
        author7 = actual_result[actual_result["author_did"] == "author07"]["total_labeled_posts"].iloc[0]
        author8 = actual_result[actual_result["author_did"] == "author08"]["total_labeled_posts"].iloc[0]
        author9 = actual_result[actual_result["author_did"] == "author09"]["total_labeled_posts"].iloc[0]
        author10 = actual_result[actual_result["author_did"] == "author10"]["total_labeled_posts"].iloc[0]
        
        assert author7 == 1  # Day 2 only
        assert author8 == 3  # Day 3 only
        assert author9 == 2  # Day 4 only
        assert author10 == 1  # Day 5 only


class TestExportAggregatedData:
    """Test cases for export_aggregated_data function."""

    @patch("aggregate_author_to_average_toxicity_across_days.pd.DataFrame.to_parquet")
    @patch("aggregate_author_to_average_toxicity_across_days.os.makedirs")
    @patch("aggregate_author_to_average_toxicity_across_days.os.path.getsize")
    def test_export_aggregated_data_success(self, mock_getsize, mock_makedirs, mock_to_parquet):
        """Test successful export of aggregated data."""
        # Arrange
        test_df = pd.DataFrame({
            "author_did": ["author1", "author2"],
            "average_toxicity": [0.2, 0.3],
            "average_outrage": [0.15, 0.25],
            "total_labeled_posts": [5, 3]
        })
        
        mock_getsize.return_value = 1024 * 1024  # 1 MB
        
        # Act
        output_dir = "/tmp/test_output"
        result_path = export_aggregated_data(test_df, output_dir)
        
        # Assert
        mock_makedirs.assert_called_once()
        mock_to_parquet.assert_called_once()
        # Check that to_parquet was called with index=False
        call_args = mock_to_parquet.call_args
        assert call_args[1]["index"] == False
        assert result_path.endswith("aggregated_author_toxicity_outrage.parquet")
        assert "results" in result_path
        assert output_dir in result_path
        mock_getsize.assert_called_once()

    @patch("aggregate_author_to_average_toxicity_across_days.pd.DataFrame.to_parquet")
    @patch("aggregate_author_to_average_toxicity_across_days.os.makedirs")
    @patch("aggregate_author_to_average_toxicity_across_days.os.path.getsize")
    def test_export_aggregated_data_empty_dataframe(self, mock_getsize, mock_makedirs, mock_to_parquet):
        """Test export of empty DataFrame."""
        # Arrange
        empty_df = pd.DataFrame(columns=["author_did", "average_toxicity", "average_outrage", "total_labeled_posts"])
        mock_getsize.return_value = 0
        
        # Act
        output_dir = "/tmp/test_output"
        result_path = export_aggregated_data(empty_df, output_dir)
        
        # Assert
        mock_makedirs.assert_called_once()
        mock_to_parquet.assert_called_once()
        # Check that to_parquet was called with index=False
        call_args = mock_to_parquet.call_args
        assert call_args[1]["index"] == False
        assert result_path.endswith("aggregated_author_toxicity_outrage.parquet")
        mock_getsize.assert_called_once()

    @patch("aggregate_author_to_average_toxicity_across_days.pd.DataFrame.to_parquet")
    @patch("aggregate_author_to_average_toxicity_across_days.os.makedirs")
    @patch("aggregate_author_to_average_toxicity_across_days.os.path.getsize")
    def test_export_aggregated_data_large_dataset(self, mock_getsize, mock_makedirs, mock_to_parquet):
        """Test export of large dataset."""
        # Arrange
        large_df = pd.DataFrame({
            "author_did": [f"author{i:04d}" for i in range(1, 1001)],  # 1000 authors
            "average_toxicity": [0.1 + (i % 10) * 0.05 for i in range(1, 1001)],
            "average_outrage": [0.05 + (i % 8) * 0.05 for i in range(1, 1001)],
            "total_labeled_posts": [1 + (i % 10) for i in range(1, 1001)]
        })
        
        mock_getsize.return_value = 5 * 1024 * 1024  # 5 MB
        
        # Act
        output_dir = "/tmp/test_output"
        result_path = export_aggregated_data(large_df, output_dir)
        
        # Assert
        mock_makedirs.assert_called_once()
        mock_to_parquet.assert_called_once()
        # Check that to_parquet was called with index=False
        call_args = mock_to_parquet.call_args
        assert call_args[1]["index"] == False
        assert result_path.endswith("aggregated_author_toxicity_outrage.parquet")
        assert "results" in result_path
        mock_getsize.assert_called_once()


class TestDataValidation:
    """Test cases for data validation and edge cases."""

    @patch("aggregate_author_to_average_toxicity_across_days.load_data_from_local_storage")
    def test_handles_null_values_in_daily_data(self, mock_load_data):
        """Test handling of null values in daily data."""
        # Arrange - Create data with null values
        daily_data_with_nulls = pd.DataFrame({
            "author_did": ["author1", "author2"],
            "partition_date": ["2024-10-01", "2024-10-01"],
            "average_toxicity": [0.2, None],
            "average_outrage": [None, 0.25],
            "total_labeled_posts": [2, 1],
            "preprocessing_timestamp": ["2024-10-01T10:00:00Z", "2024-10-01T11:00:00Z"]
        })
        
        def mock_load_side_effect(service, **kwargs):
            if service == "author_to_average_toxicity_outrage":
                return daily_data_with_nulls.copy()
            return pd.DataFrame()
        
        mock_load_data.side_effect = mock_load_side_effect
        
        # Act
        partition_dates = ["2024-10-01"]
        actual_result = aggregate_author_toxicity_across_days(partition_dates)
        
        # Assert
        assert len(actual_result) == 2
        # Verify that null values are handled appropriately
        # author1 should have valid toxicity but null outrage
        # author2 should have null toxicity but valid outrage
        assert actual_result["average_toxicity"].notna().sum() >= 1
        assert actual_result["average_outrage"].notna().sum() >= 1

    @patch("aggregate_author_to_average_toxicity_across_days.load_data_from_local_storage")
    def test_handles_extreme_values(self, mock_load_data):
        """Test handling of extreme values (0.0 and 1.0)."""
        # Arrange - Create data with extreme values
        daily_data_extreme = pd.DataFrame({
            "author_did": ["author1", "author2", "author3"],
            "partition_date": ["2024-10-01", "2024-10-01", "2024-10-01"],
            "average_toxicity": [0.0, 0.5, 1.0],
            "average_outrage": [0.0, 0.5, 1.0],
            "total_labeled_posts": [1, 1, 1],
            "preprocessing_timestamp": ["2024-10-01T10:00:00Z", "2024-10-01T11:00:00Z", "2024-10-01T12:00:00Z"]
        })
        
        def mock_load_side_effect(service, **kwargs):
            if service == "author_to_average_toxicity_outrage":
                return daily_data_extreme.copy()
            return pd.DataFrame()
        
        mock_load_data.side_effect = mock_load_side_effect
        
        # Act
        partition_dates = ["2024-10-01"]
        actual_result = aggregate_author_toxicity_across_days(partition_dates)
        
        # Assert
        assert len(actual_result) == 3
        # Verify extreme values are preserved
        assert (actual_result["average_toxicity"] >= 0.0).all()
        assert (actual_result["average_toxicity"] <= 1.0).all()
        assert (actual_result["average_outrage"] >= 0.0).all()
        assert (actual_result["average_outrage"] <= 1.0).all()
        
        # Verify specific extreme values exist
        assert (actual_result["average_toxicity"] == 0.0).any()
        assert (actual_result["average_toxicity"] == 1.0).any()
        assert (actual_result["average_outrage"] == 0.0).any()
        assert (actual_result["average_outrage"] == 1.0).any()

    @patch("aggregate_author_to_average_toxicity_across_days.load_data_from_local_storage")
    def test_handles_load_data_exception(self, mock_load_data):
        """Test handling of exceptions during data loading."""
        # Arrange
        def mock_load_side_effect(service, **kwargs):
            if service == "author_to_average_toxicity_outrage":
                raise Exception("Database connection failed")
            return pd.DataFrame()
        
        mock_load_data.side_effect = mock_load_side_effect
        
        # Act
        partition_dates = ["2024-10-01", "2024-10-02"]
        actual_result = aggregate_author_toxicity_across_days(partition_dates)
        
        # Assert
        assert actual_result.empty
        # When DataFrame is empty, it may not have columns, so we check if it's empty first
        if not actual_result.empty:
            assert list(actual_result.columns) == ["author_did", "average_toxicity", "average_outrage", "total_labeled_posts"]


if __name__ == "__main__":
    pytest.main([__file__])
