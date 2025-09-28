"""Unit tests for author_to_average_toxicity_outrage helper functions."""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from services.get_author_to_average_toxicity_outrage.helper import (
    get_author_to_average_toxicity_outrage,
    get_and_export_author_to_average_toxicity_outrage_for_partition_date,
    get_and_export_daily_author_to_average_toxicity_outrage,
)


class TestGetAuthorToAverageToxicityOutrage:
    """Test cases for get_author_to_average_toxicity_outrage function."""

    @patch("services.get_author_to_average_toxicity_outrage.helper.load_data_from_local_storage")
    def test_get_author_to_average_toxicity_outrage_success(self, mock_load_data):
        """Test successful processing of author toxicity/outrage data."""
        # Arrange
        perspective_api_data = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3", "uri4"],
            "text": ["text1", "text2", "text3", "text4"],  # Add text column
            "prob_toxic": [0.1, 0.2, 0.3, 0.4],
            "prob_moral_outrage": [0.05, 0.15, 0.25, 0.35],
            "partition_date": ["2024-10-01", "2024-10-01", "2024-10-01", "2024-10-01"]
        })

        preprocessed_posts_data = pd.DataFrame({
            "author_did": ["author1", "author1", "author2", "author2"],
            "text": ["text1", "text2", "text3", "text4"],  # Add text column
            "uri": ["uri1", "uri2", "uri3", "uri4"],
            "partition_date": ["2024-10-01", "2024-10-01", "2024-10-01", "2024-10-01"]
        })

        expected_result = pd.DataFrame({
            "author_did": ["author1", "author2"],
            "partition_date": ["2024-10-01", "2024-10-01"],
            "average_toxicity": [0.15, 0.35],  # (0.1+0.2)/2, (0.3+0.4)/2
            "average_outrage": [0.10, 0.30],   # (0.05+0.15)/2, (0.25+0.35)/2
            "total_labeled_posts": [2, 2]
        })

        def mock_load_side_effect(service, **kwargs):
            if service == "ml_inference_perspective_api":
                return perspective_api_data.copy()
            elif service == "preprocessed_posts":
                return preprocessed_posts_data.copy()
            return pd.DataFrame()

        mock_load_data.side_effect = mock_load_side_effect

        # Act
        actual_result = get_author_to_average_toxicity_outrage("2024-10-01")

        # Assert
        assert len(actual_result) == len(expected_result)
        assert list(actual_result.columns) == list(expected_result.columns)
        
        # Verify author1 data
        author1_actual = actual_result[actual_result["author_did"] == "author1"].iloc[0]
        author1_expected = expected_result[expected_result["author_did"] == "author1"].iloc[0]
        assert author1_actual["total_labeled_posts"] == author1_expected["total_labeled_posts"]
        assert author1_actual["average_toxicity"] == pytest.approx(author1_expected["average_toxicity"])
        assert author1_actual["average_outrage"] == pytest.approx(author1_expected["average_outrage"])

        # Verify author2 data
        author2_actual = actual_result[actual_result["author_did"] == "author2"].iloc[0]
        author2_expected = expected_result[expected_result["author_did"] == "author2"].iloc[0]
        assert author2_actual["total_labeled_posts"] == author2_expected["total_labeled_posts"]
        assert author2_actual["average_toxicity"] == pytest.approx(author2_expected["average_toxicity"])
        assert author2_actual["average_outrage"] == pytest.approx(author2_expected["average_outrage"])

    @patch("services.get_author_to_average_toxicity_outrage.helper.load_data_from_local_storage")
    def test_get_author_to_average_toxicity_outrage_empty_data(self, mock_load_data):
        """Test handling of empty data."""
        # Arrange
        empty_df = pd.DataFrame(columns=["uri", "text", "prob_toxic", "prob_moral_outrage", "partition_date"])
        expected_result = pd.DataFrame(columns=["author_did", "partition_date", "average_toxicity", "average_outrage", "total_labeled_posts"])
        
        # Mock both services to return empty data
        def mock_load_side_effect(service, **kwargs):
            return empty_df.copy()

        mock_load_data.side_effect = mock_load_side_effect

        # Act
        actual_result = get_author_to_average_toxicity_outrage("2024-10-01")

        # Assert
        assert len(actual_result) == len(expected_result)
        assert list(actual_result.columns) == list(expected_result.columns)

    @patch("services.get_author_to_average_toxicity_outrage.helper.load_data_from_local_storage")
    def test_get_author_to_average_toxicity_outrage_no_matching_uris(self, mock_load_data):
        """Test handling when no URIs match between datasets."""
        # Arrange
        perspective_api_data = pd.DataFrame({
            "uri": ["uri1", "uri2"],
            "text": ["text1", "text2"],  # Add text column
            "prob_toxic": [0.1, 0.2],
            "prob_moral_outrage": [0.05, 0.15],
            "partition_date": ["2024-10-01", "2024-10-01"]
        })

        preprocessed_posts_data = pd.DataFrame({
            "author_did": ["author1", "author2"],
            "text": ["text1", "text2"],  # Add text column
            "uri": ["uri3", "uri4"],  # Different URIs
            "partition_date": ["2024-10-01", "2024-10-01"]
        })

        expected_result = pd.DataFrame(columns=["author_did", "partition_date", "average_toxicity", "average_outrage", "total_labeled_posts"])

        def mock_load_side_effect(service, **kwargs):
            if service == "ml_inference_perspective_api":
                return perspective_api_data.copy()
            elif service == "preprocessed_posts":
                return preprocessed_posts_data.copy()
            return pd.DataFrame()

        mock_load_data.side_effect = mock_load_side_effect

        # Act
        actual_result = get_author_to_average_toxicity_outrage("2024-10-01")

        # Assert
        assert len(actual_result) == len(expected_result)
        assert list(actual_result.columns) == list(expected_result.columns)

    @patch("services.get_author_to_average_toxicity_outrage.helper.load_data_from_local_storage")
    def test_get_author_to_average_toxicity_outrage_single_author_multiple_posts(self, mock_load_data):
        """Test processing multiple posts for a single author."""
        # Arrange
        perspective_api_data = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "text": ["text1", "text2", "text3"],  # Add text column
            "prob_toxic": [0.1, 0.2, 0.3],
            "prob_moral_outrage": [0.05, 0.15, 0.25],
            "partition_date": ["2024-10-01", "2024-10-01", "2024-10-01"]
        })

        preprocessed_posts_data = pd.DataFrame({
            "author_did": ["author1", "author1", "author1"],
            "text": ["text1", "text2", "text3"],  # Add text column
            "uri": ["uri1", "uri2", "uri3"],
            "partition_date": ["2024-10-01", "2024-10-01", "2024-10-01"]
        })

        expected_result = pd.DataFrame({
            "author_did": ["author1"],
            "partition_date": ["2024-10-01"],
            "average_toxicity": [0.2],  # (0.1 + 0.2 + 0.3) / 3
            "average_outrage": [0.15],  # (0.05 + 0.15 + 0.25) / 3
            "total_labeled_posts": [3]
        })

        def mock_load_side_effect(service, **kwargs):
            if service == "ml_inference_perspective_api":
                return perspective_api_data.copy()
            elif service == "preprocessed_posts":
                return preprocessed_posts_data.copy()
            return pd.DataFrame()

        mock_load_data.side_effect = mock_load_side_effect

        # Act
        actual_result = get_author_to_average_toxicity_outrage("2024-10-01")

        # Assert
        assert len(actual_result) == len(expected_result)
        assert actual_result["author_did"].iloc[0] == expected_result["author_did"].iloc[0]
        assert actual_result["total_labeled_posts"].iloc[0] == expected_result["total_labeled_posts"].iloc[0]
        assert actual_result["average_toxicity"].iloc[0] == pytest.approx(expected_result["average_toxicity"].iloc[0])
        assert actual_result["average_outrage"].iloc[0] == pytest.approx(expected_result["average_outrage"].iloc[0])


class TestGetAndExportAuthorToAverageToxicityOutrageForPartitionDate:
    """Test cases for get_and_export_author_to_average_toxicity_outrage_for_partition_date function."""

    @patch("services.get_author_to_average_toxicity_outrage.helper.export_data_to_local_storage")
    @patch("services.get_author_to_average_toxicity_outrage.helper.get_author_to_average_toxicity_outrage")
    def test_get_and_export_success(self, mock_get_data, mock_export_data):
        """Test successful data processing and export."""
        # Arrange
        mock_data = pd.DataFrame({
            "author_did": ["author1", "author2"],
            "partition_date": ["2024-10-01", "2024-10-01"],
            "total_labeled_posts": [2, 1],
            "average_toxicity": [0.15, 0.2],
            "average_outrage": [0.10, 0.15]
        })
        mock_get_data.return_value = mock_data

        # Act
        get_and_export_author_to_average_toxicity_outrage_for_partition_date("2024-10-01")

        # Assert
        mock_get_data.assert_called_once_with("2024-10-01")
        mock_export_data.assert_called_once_with(
            service="author_to_average_toxicity_outrage",
            df=mock_data,
            export_format="parquet"
        )

    @patch("services.get_author_to_average_toxicity_outrage.helper.export_data_to_local_storage")
    @patch("services.get_author_to_average_toxicity_outrage.helper.get_author_to_average_toxicity_outrage")
    def test_get_and_export_empty_data(self, mock_get_data, mock_export_data):
        """Test handling of empty data."""
        # Arrange
        mock_get_data.return_value = pd.DataFrame()

        # Act
        get_and_export_author_to_average_toxicity_outrage_for_partition_date("2024-10-01")

        # Assert
        mock_get_data.assert_called_once_with("2024-10-01")
        mock_export_data.assert_called_once()


class TestGetAndExportDailyAuthorToAverageToxicityOutrage:
    """Test cases for get_and_export_daily_author_to_average_toxicity_outrage function."""

    @patch("services.get_author_to_average_toxicity_outrage.helper.get_and_export_author_to_average_toxicity_outrage_for_partition_date")
    def test_get_and_export_daily_success(self, mock_export_function):
        """Test successful daily processing."""
        # Arrange
        partition_dates = ["2024-10-01", "2024-10-02", "2024-10-03"]
        expected_call_count = 3

        # Act
        get_and_export_daily_author_to_average_toxicity_outrage(partition_dates)

        # Assert
        assert mock_export_function.call_count == expected_call_count
        call_args = [call[0][0] for call in mock_export_function.call_args_list]
        assert set(call_args) == set(partition_dates)

    @patch("services.get_author_to_average_toxicity_outrage.helper.get_and_export_author_to_average_toxicity_outrage_for_partition_date")
    def test_get_and_export_daily_empty_dates(self, mock_export_function):
        """Test handling of empty partition dates list."""
        # Arrange
        partition_dates = []
        expected_call_count = 0

        # Act
        get_and_export_daily_author_to_average_toxicity_outrage(partition_dates)

        # Assert
        assert mock_export_function.call_count == expected_call_count

    @patch("services.get_author_to_average_toxicity_outrage.helper.get_and_export_author_to_average_toxicity_outrage_for_partition_date")
    def test_get_and_export_daily_single_date(self, mock_export_function):
        """Test processing single partition date."""
        # Arrange
        partition_dates = ["2024-10-01"]
        expected_call_count = 1

        # Act
        get_and_export_daily_author_to_average_toxicity_outrage(partition_dates)

        # Assert
        assert mock_export_function.call_count == expected_call_count
        mock_export_function.assert_called_once_with("2024-10-01")


class TestDataValidation:
    """Test cases for data validation and edge cases."""

    @patch("services.get_author_to_average_toxicity_outrage.helper.load_data_from_local_storage")
    def test_handles_null_values(self, mock_load_data):
        """Test handling of null values in data."""
        # Arrange
        perspective_api_data = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "text": ["text1", "text2", "text3"],  # Add text column
            "prob_toxic": [0.1, None, 0.3],
            "prob_moral_outrage": [0.05, 0.15, None],
            "partition_date": ["2024-10-01", "2024-10-01", "2024-10-01"]
        })

        preprocessed_posts_data = pd.DataFrame({
            "author_did": ["author1", "author2", "author3"],
            "text": ["text1", "text2", "text3"],  # Add text column
            "uri": ["uri1", "uri2", "uri3"],
            "partition_date": ["2024-10-01", "2024-10-01", "2024-10-01"]
        })

        def mock_load_side_effect(service, **kwargs):
            if service == "ml_inference_perspective_api":
                return perspective_api_data.copy()
            elif service == "preprocessed_posts":
                return preprocessed_posts_data.copy()
            return pd.DataFrame()

        mock_load_data.side_effect = mock_load_side_effect

        # Act
        actual_result = get_author_to_average_toxicity_outrage("2024-10-01")

        # Assert
        assert len(actual_result) == 3
        # Check that null values are handled appropriately
        # When all values for an author are null, the mean will be NaN
        # When some values are null, pandas mean() ignores nulls
        assert actual_result["average_toxicity"].notna().sum() >= 1  # At least one non-null value
        assert actual_result["average_outrage"].notna().sum() >= 1  # At least one non-null value

    @patch("services.get_author_to_average_toxicity_outrage.helper.load_data_from_local_storage")
    def test_handles_duplicate_uris(self, mock_load_data):
        """Test handling of duplicate URIs in the data."""
        # Arrange
        perspective_api_data = pd.DataFrame({
            "uri": ["uri1", "uri1", "uri2"],  # Duplicate uri1
            "text": ["text1", "text2", "text3"],  # Add text column
            "prob_toxic": [0.1, 0.2, 0.3],
            "prob_moral_outrage": [0.05, 0.15, 0.25],
            "partition_date": ["2024-10-01", "2024-10-01", "2024-10-01"]
        })

        preprocessed_posts_data = pd.DataFrame({
            "author_did": ["author1", "author1", "author2"],
            "text": ["text1", "text2", "text3"],  # Add text column
            "uri": ["uri1", "uri1", "uri2"],  # Duplicate uri1
            "partition_date": ["2024-10-01", "2024-10-01", "2024-10-01"]
        })

        expected_unique_authors = 2

        def mock_load_side_effect(service, **kwargs):
            if service == "ml_inference_perspective_api":
                return perspective_api_data.copy()
            elif service == "preprocessed_posts":
                return preprocessed_posts_data.copy()
            return pd.DataFrame()

        mock_load_data.side_effect = mock_load_side_effect

        # Act
        actual_result = get_author_to_average_toxicity_outrage("2024-10-01")

        # Assert
        assert len(actual_result) == expected_unique_authors
        # The function should deduplicate based on author_did and partition_date
        assert actual_result["author_did"].nunique() == expected_unique_authors
