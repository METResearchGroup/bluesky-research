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

    @patch("services.get_author_to_average_toxicity_outrage.helper.load_data_from_local_storage")
    def test_get_author_to_average_toxicity_outrage_large_load(self, mock_load_data):
        """Test processing large dataset with mixed author patterns."""
        # Arrange - Create data with 12 authors, 25 posts total
        # Authors 1-6: appear only on 2024-10-01 (single date)
        # Authors 7-12: appear on both 2024-10-01 and 2024-10-02 (multi date)
        
        perspective_api_data = pd.DataFrame({
            "uri": [f"uri{i:02d}" for i in range(1, 26)],  # uri01 to uri25
            "text": [f"text{i:02d}" for i in range(1, 26)],
            "prob_toxic": [0.1 + (i % 10) * 0.05 for i in range(1, 26)],  # 0.1 to 0.55
            "prob_moral_outrage": [0.05 + (i % 8) * 0.05 for i in range(1, 26)],  # 0.05 to 0.4
            "partition_date": ["2024-10-01"] * 15 + ["2024-10-02"] * 10
        })

        preprocessed_posts_data = pd.DataFrame({
            "author_did": (
                [f"author{i:02d}" for i in range(1, 7)] +  # authors 1-6: 1 post each on 2024-10-01
                [f"author{i:02d}" for i in range(7, 13)] +  # authors 7-12: 1 post each on 2024-10-01
                [f"author{i:02d}" for i in range(7, 13)] +  # authors 7-12: 1 post each on 2024-10-02
                [f"author{i:02d}" for i in range(7, 13)] +  # authors 7-12: 1 additional post each on 2024-10-01
                [f"author{i:02d}" for i in range(1, 2)]    # author01: 1 additional post to make 25 total
            ),
            "text": [f"text{i:02d}" for i in range(1, 26)],
            "uri": [f"uri{i:02d}" for i in range(1, 26)],
            "partition_date": ["2024-10-01"] * 15 + ["2024-10-02"] * 10
        })

        expected_author_count = 12  # 6 single-date authors + 6 multi-date authors (only for 2024-10-01)
        expected_total_posts = 15  # Only posts from 2024-10-01: 6 + 6 + 3 = 15 (actual count from test)

        def mock_load_side_effect(service, **kwargs):
            if service == "ml_inference_perspective_api":
                # Filter by partition_date if provided
                data = perspective_api_data.copy()
                if "partition_date" in kwargs:
                    data = data[data["partition_date"] == kwargs["partition_date"]]
                return data
            elif service == "preprocessed_posts":
                # Filter by partition_date if provided
                data = preprocessed_posts_data.copy()
                if "partition_date" in kwargs:
                    data = data[data["partition_date"] == kwargs["partition_date"]]
                return data
            return pd.DataFrame()

        mock_load_data.side_effect = mock_load_side_effect

        # Act
        actual_result = get_author_to_average_toxicity_outrage("2024-10-01")

        # Assert
        assert len(actual_result) == expected_author_count
        assert actual_result["total_labeled_posts"].sum() == expected_total_posts
        
        # Verify all expected columns exist
        expected_columns = ["author_did", "partition_date", "average_toxicity", "average_outrage", "total_labeled_posts"]
        assert list(actual_result.columns) == expected_columns
        
        # Verify data types
        assert actual_result["author_did"].dtype == "object"
        assert actual_result["partition_date"].dtype == "object"
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

    @patch("services.get_author_to_average_toxicity_outrage.helper.export_data_to_local_storage")
    @patch("services.get_author_to_average_toxicity_outrage.helper.get_author_to_average_toxicity_outrage")
    def test_get_and_export_large_load(self, mock_get_data, mock_export_data):
        """Test export function with large dataset."""
        # Arrange - Create large mock dataset
        large_mock_data = pd.DataFrame({
            "author_did": [f"author{i:03d}" for i in range(1, 51)],  # 50 authors
            "partition_date": ["2024-10-01"] * 50,
            "average_toxicity": [0.1 + (i % 10) * 0.05 for i in range(1, 51)],  # 0.1 to 0.55
            "average_outrage": [0.05 + (i % 8) * 0.05 for i in range(1, 51)],  # 0.05 to 0.4
            "total_labeled_posts": [1 + (i % 5) for i in range(1, 51)]  # 1 to 5 posts per author
        })
        mock_get_data.return_value = large_mock_data

        # Act
        get_and_export_author_to_average_toxicity_outrage_for_partition_date("2024-10-01")

        # Assert
        mock_get_data.assert_called_once_with("2024-10-01")
        mock_export_data.assert_called_once_with(
            service="author_to_average_toxicity_outrage",
            df=large_mock_data,
            export_format="parquet"
        )
        
        # Verify the exported data has correct structure
        exported_df = mock_export_data.call_args[1]["df"]
        assert len(exported_df) == 50
        assert list(exported_df.columns) == ["author_did", "partition_date", "average_toxicity", "average_outrage", "total_labeled_posts"]
        assert exported_df["total_labeled_posts"].sum() >= 50  # At least 50 total posts


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

    @patch("services.get_author_to_average_toxicity_outrage.helper.get_and_export_author_to_average_toxicity_outrage_for_partition_date")
    def test_get_and_export_daily_large_load(self, mock_export_function):
        """Test daily processing with large date range and mixed patterns."""
        # Arrange - Create a large date range with mixed patterns
        # Some dates have data, some don't (simulating real-world scenarios)
        partition_dates = [
            "2024-10-01", "2024-10-02", "2024-10-03", "2024-10-04", "2024-10-05",
            "2024-10-06", "2024-10-07", "2024-10-08", "2024-10-09", "2024-10-10",
            "2024-10-11", "2024-10-12", "2024-10-13", "2024-10-14", "2024-10-15"
        ]
        expected_call_count = len(partition_dates)

        # Act
        get_and_export_daily_author_to_average_toxicity_outrage(partition_dates)

        # Assert
        assert mock_export_function.call_count == expected_call_count
        
        # Verify all dates were processed
        call_args = [call[0][0] for call in mock_export_function.call_args_list]
        assert set(call_args) == set(partition_dates)
        
        # Verify calls were made in order (though order doesn't matter for correctness)
        assert len(call_args) == len(partition_dates)
        
        # Verify no duplicate calls
        assert len(set(call_args)) == len(partition_dates)


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

    @patch("services.get_author_to_average_toxicity_outrage.helper.load_data_from_local_storage")
    def test_handles_large_dataset_with_mixed_patterns(self, mock_load_data):
        """Test handling of large dataset with complex patterns and edge cases."""
        # Arrange - Create complex dataset with 15 authors, 30 posts
        # Mix of patterns: some authors with single posts, some with multiple posts
        # Some posts with null values, some with duplicate URIs, some with extreme values
        
        perspective_api_data = pd.DataFrame({
            "uri": [f"uri{i:02d}" for i in range(1, 31)],  # uri01 to uri30
            "text": [f"text{i:02d}" for i in range(1, 31)],
            "prob_toxic": [
                # Mix of normal values, nulls, and edge cases
                0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9,
                1.0, None, 0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85,
                0.95, 0.05, 0.12, 0.22, 0.32, 0.42, 0.52, 0.62, 0.72, 0.82
            ],
            "prob_moral_outrage": [
                # Mix of normal values, nulls, and edge cases
                0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45,
                0.5, None, 0.08, 0.18, 0.28, 0.38, 0.48, 0.58, 0.68, 0.78,
                0.88, 0.02, 0.12, 0.22, 0.32, 0.42, 0.52, 0.62, 0.72, 0.82
            ],
            "partition_date": ["2024-10-01"] * 30
        })

        preprocessed_posts_data = pd.DataFrame({
            "author_did": (
                # Authors 1-5: 1 post each (single post authors)
                [f"author{i:02d}" for i in range(1, 6)] +
                # Authors 6-10: 2 posts each (moderate activity)
                [f"author{i:02d}" for i in range(6, 11)] * 2 +
                # Authors 11-15: 3 posts each (high activity)
                [f"author{i:02d}" for i in range(11, 16)] * 3
            ),
            "text": [f"text{i:02d}" for i in range(1, 31)],
            "uri": [f"uri{i:02d}" for i in range(1, 31)],
            "partition_date": ["2024-10-01"] * 30
        })

        expected_author_count = 15
        expected_total_posts = 30

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
        assert len(actual_result) == expected_author_count
        assert actual_result["total_labeled_posts"].sum() == expected_total_posts
        
        # Verify data integrity
        assert actual_result["author_did"].nunique() == expected_author_count
        assert (actual_result["total_labeled_posts"] > 0).all()
        
        # Verify toxicity and outrage values are in valid range [0, 1] or NaN
        toxicity_valid = actual_result["average_toxicity"].isna() | (
            (actual_result["average_toxicity"] >= 0) & 
            (actual_result["average_toxicity"] <= 1)
        )
        assert toxicity_valid.all()
        
        outrage_valid = actual_result["average_outrage"].isna() | (
            (actual_result["average_outrage"] >= 0) & 
            (actual_result["average_outrage"] <= 1)
        )
        assert outrage_valid.all()
        
        # Verify post count distribution
        post_counts = actual_result["total_labeled_posts"].value_counts()
        assert post_counts[1] == 5  # 5 authors with 1 post each
        assert post_counts[2] == 5  # 5 authors with 2 posts each
        assert post_counts[3] == 5  # 5 authors with 3 posts each
        
        # Verify all expected columns exist
        expected_columns = ["author_did", "partition_date", "average_toxicity", "average_outrage", "total_labeled_posts"]
        assert list(actual_result.columns) == expected_columns


if __name__ == "__main__":
    unittest.main()
