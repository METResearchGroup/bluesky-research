"""Tests for labels.py.

This test suite verifies the functionality of label data loading functions:
- Perspective API label loading and filtering
- Label transformation for different integrations
- Comprehensive label aggregation across multiple integrations
- Error handling and edge cases

The tests use mocks to isolate the data loading logic from external dependencies.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock

from services.calculate_analytics.shared.data_loading.labels import (
    get_perspective_api_labels,
    get_perspective_api_labels_for_posts,
    get_labels_for_partition_date,
    transform_labels_dict,
    get_all_labels_for_posts,
)

# Import mock data
from services.calculate_analytics.shared.data_loading.tests.mock_labels import (
    mock_perspective_data,
    mock_sociopolitical_data,
    mock_ime_data,
    mock_valence_data,
    perspective_api_labels,
    sociopolitical_labels,
    ime_labels,
    valence_labels,
)


class TestGetPerspectiveApiLabels:
    """Tests for get_perspective_api_labels function."""

    def test_loads_perspective_api_labels_successfully(self):
        """Test successful loading of Perspective API labels.

        This test verifies that:
        1. The function calls load_data_from_local_storage correctly
        2. The correct service name is used
        3. The lookback dates are properly passed through
        4. The function returns the expected DataFrame
        """
        # Arrange
        lookback_start_date = "2024-01-01"
        lookback_end_date = "2024-01-07"
        mock_df = pd.DataFrame({
            "uri": ["post1", "post2", "post3"],
            "prob_toxic": [0.8, 0.3, 0.7],
            "prob_constructive": [0.6, 0.9, 0.4]
        })
        
        expected_service = "ml_inference_perspective_api"
        expected_directory = "cache"

        # Act
        with patch("services.calculate_analytics.shared.data_loading.labels.load_data_from_local_storage", return_value=mock_df) as mock_load:
            result = get_perspective_api_labels(lookback_start_date, lookback_end_date)

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert list(result.columns) == ["uri", "prob_toxic", "prob_constructive"]
        
        # Verify the dependency was called correctly
        mock_load.assert_called_once_with(
            service=expected_service,
            storage_tiers=[expected_directory],
            start_partition_date=lookback_start_date,
            end_partition_date=lookback_end_date,
            duckdb_query=None,
            query_metadata=None,
            export_format="parquet"
        )

    def test_passes_optional_parameters_correctly(self):
        """Test that optional parameters are passed through correctly.

        This test verifies that:
        1. DuckDB query is passed when provided
        2. Query metadata is passed when provided
        3. Export format is passed when provided
        4. Default values are used when not provided
        """
        # Arrange
        lookback_start_date = "2024-01-01"
        lookback_end_date = "2024-01-07"
        duckdb_query = "SELECT * FROM labels WHERE prob_toxic > 0.5"
        query_metadata = {"filter": "high_toxicity"}
        export_format = "jsonl"
        
        mock_df = pd.DataFrame({"uri": ["post1"], "prob_toxic": [0.8]})

        # Act
        with patch("services.calculate_analytics.shared.data_loading.labels.load_data_from_local_storage", return_value=mock_df) as mock_load:
            result = get_perspective_api_labels(
                lookback_start_date, 
                lookback_end_date,
                duckdb_query=duckdb_query,
                query_metadata=query_metadata,
                export_format=export_format
            )

        # Assert
        mock_load.assert_called_once_with(
            service="ml_inference_perspective_api",
            storage_tiers=["cache"],
            start_partition_date=lookback_start_date,
            end_partition_date=lookback_end_date,
            duckdb_query=duckdb_query,
            query_metadata=query_metadata,
            export_format=export_format
        )

    def test_raises_exception_on_load_data_failure(self):
        """Test that exceptions from load_data_from_local_storage are properly propagated.

        This test verifies that:
        1. Exceptions from the dependency are caught and logged
        2. The original exception is re-raised
        3. Error handling works correctly
        """
        # Arrange
        lookback_start_date = "2024-01-01"
        lookback_end_date = "2024-01-07"
        expected_error = "Failed to load data from local storage"
        mock_load_data = Mock(side_effect=Exception(expected_error))

        # Act & Assert
        with patch("services.calculate_analytics.shared.data_loading.labels.load_data_from_local_storage", side_effect=mock_load_data):
            with pytest.raises(Exception, match=expected_error):
                get_perspective_api_labels(lookback_start_date, lookback_end_date)


class TestGetPerspectiveApiLabelsForPosts:
    """Tests for get_perspective_api_labels_for_posts function."""

    def test_filters_labels_to_specific_posts(self):
        """Test filtering of labels to specific posts.

        This test verifies that:
        1. The function calls get_perspective_api_labels correctly
        2. The result is filtered to only include the specified posts
        3. The filtering logic works correctly
        4. The function returns the expected filtered DataFrame
        """
        # Arrange
        posts = pd.DataFrame({
            "uri": ["post1", "post3"],
            "other_column": ["value1", "value3"]
        })
        lookback_start_date = "2024-01-01"
        lookback_end_date = "2024-01-07"
        
        mock_all_labels = pd.DataFrame({
            "uri": ["post1", "post2", "post3", "post4"],
            "prob_toxic": [0.8, 0.5, 0.7, 0.9],
            "prob_constructive": [0.6, 0.4, 0.4, 0.2]
        })
        
        expected_filtered_labels = pd.DataFrame({
            "uri": ["post1", "post3"],
            "prob_toxic": [0.8, 0.7],
            "prob_constructive": [0.6, 0.4]
        })

        # Act
        with patch("services.calculate_analytics.shared.data_loading.labels.get_perspective_api_labels", return_value=mock_all_labels):
            result = get_perspective_api_labels_for_posts(
                posts, lookback_start_date, lookback_end_date
            )

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert result["uri"].tolist() == ["post1", "post3"]
        assert result["prob_toxic"].tolist() == [0.8, 0.7]
        assert result["prob_constructive"].tolist() == [0.6, 0.4]

    def test_handles_empty_posts_dataframe(self):
        """Test handling of empty posts DataFrame.

        This test verifies that:
        1. Empty posts DataFrame is handled gracefully
        2. Function returns empty DataFrame with correct structure
        3. Edge case doesn't cause crashes
        """
        # Arrange
        posts = pd.DataFrame(columns=["uri", "other_column"])
        lookback_start_date = "2024-01-01"
        lookback_end_date = "2024-01-07"
        
        mock_all_labels = pd.DataFrame({
            "uri": ["post1", "post2"],
            "prob_toxic": [0.8, 0.5]
        })

        # Act
        with patch("services.calculate_analytics.shared.data_loading.labels.get_perspective_api_labels", return_value=mock_all_labels):
            result = get_perspective_api_labels_for_posts(
                posts, lookback_start_date, lookback_end_date
            )

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert list(result.columns) == ["uri", "prob_toxic"]

    def test_handles_no_matching_posts(self):
        """Test handling when no posts match the filter.

        This test verifies that:
        1. When no posts match, empty DataFrame is returned
        2. The filtering logic works correctly for edge cases
        3. Function doesn't crash on unexpected input
        """
        # Arrange
        posts = pd.DataFrame({
            "uri": ["post1", "post2"],
            "other_column": ["value1", "value2"]
        })
        lookback_start_date = "2024-01-01"
        lookback_end_date = "2024-01-07"
        
        mock_all_labels = pd.DataFrame({
            "uri": ["post3", "post4"],
            "prob_toxic": [0.8, 0.5]
        })

        # Act
        with patch("services.calculate_analytics.shared.data_loading.labels.get_perspective_api_labels", return_value=mock_all_labels):
            result = get_perspective_api_labels_for_posts(
                posts, lookback_start_date, lookback_end_date
            )

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_passes_optional_parameters_to_get_perspective_api_labels(self):
        """Test that optional parameters are passed through to get_perspective_api_labels.

        This test verifies that:
        1. DuckDB query is passed through correctly
        2. Query metadata is passed through correctly
        3. Export format is passed through correctly
        """
        # Arrange
        posts = pd.DataFrame({"uri": ["post1"]})
        lookback_start_date = "2024-01-01"
        lookback_end_date = "2024-01-07"
        duckdb_query = "SELECT * FROM labels"
        query_metadata = {"filter": "test"}
        export_format = "jsonl"
        
        mock_all_labels = pd.DataFrame({"uri": ["post1"], "prob_toxic": [0.8]})

        # Act
        with patch("services.calculate_analytics.shared.data_loading.labels.get_perspective_api_labels", return_value=mock_all_labels) as mock_get_labels:
            result = get_perspective_api_labels_for_posts(
                posts, 
                lookback_start_date, 
                lookback_end_date,
                duckdb_query=duckdb_query,
                query_metadata=query_metadata,
                export_format=export_format
            )

        # Assert
        mock_get_labels.assert_called_once_with(
            lookback_start_date=lookback_start_date,
            lookback_end_date=lookback_end_date,
            duckdb_query=duckdb_query,
            query_metadata=query_metadata,
            export_format=export_format
        )


class TestGetLabelsForPartitionDate:
    """Tests for get_labels_for_partition_date function."""

    def test_loads_labels_for_specific_partition_date(self):
        """Test loading of labels for a specific partition date.

        This test verifies that:
        1. The function calls load_data_from_local_storage correctly
        2. The correct service name is constructed
        3. The partition date is properly passed through
        4. Duplicates are removed correctly
        """
        # Arrange
        integration = "perspective_api"
        partition_date = "2024-01-01"
        
        mock_df = pd.DataFrame({
            "uri": ["post1", "post1", "post2", "post3", "post3"],
            "prob_toxic": [0.8, 0.8, 0.3, 0.7, 0.7],
            "prob_constructive": [0.6, 0.6, 0.9, 0.4, 0.4]
        })
        
        expected_service = "ml_inference_perspective_api"
        expected_count_after_dedup = 3

        # Act
        with patch("services.calculate_analytics.shared.data_loading.labels.load_data_from_local_storage", return_value=mock_df) as mock_load:
            result = get_labels_for_partition_date(integration, partition_date)

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == expected_count_after_dedup
        assert result["uri"].tolist() == ["post1", "post2", "post3"]
        
        # Verify the dependency was called correctly
        mock_load.assert_called_once_with(
            service=expected_service,
            storage_tiers=["cache"],
            partition_date=partition_date
        )

    def test_handles_empty_labels_dataframe(self):
        """Test handling of empty labels DataFrame.

        This test verifies that:
        1. Empty DataFrame is handled gracefully
        2. Function returns empty DataFrame with correct structure
        3. Edge case doesn't cause crashes
        """
        # Arrange
        integration = "ime"
        partition_date = "2024-01-01"
        
        mock_df = pd.DataFrame(columns=["uri", "prob_intergroup"])

        # Act
        with patch("services.calculate_analytics.shared.data_loading.labels.load_data_from_local_storage", return_value=mock_df):
            result = get_labels_for_partition_date(integration, partition_date)

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_raises_exception_on_load_data_failure(self):
        """Test that exceptions from load_data_from_local_storage are properly propagated.

        This test verifies that:
        1. Exceptions from the dependency are caught and logged
        2. The original exception is re-raised
        3. Error handling works correctly
        """
        # Arrange
        integration = "valence_classifier"
        partition_date = "2024-01-01"
        expected_error = "Failed to load data from local storage"
        mock_load_data = Mock(side_effect=Exception(expected_error))

        # Act & Assert
        with patch("services.calculate_analytics.shared.data_loading.labels.load_data_from_local_storage", side_effect=mock_load_data):
            with pytest.raises(Exception, match=expected_error):
                get_labels_for_partition_date(integration, partition_date)


class TestTransformLabelsDict:
    """Tests for transform_labels_dict function."""

    def test_transforms_perspective_api_labels_correctly(self):
        """Test transformation of Perspective API labels.

        This test verifies that:
        1. All Perspective API label fields are correctly mapped
        2. The transformation preserves the original values
        3. The result contains the expected label structure
        """
        # Arrange
        integration = "perspective_api"
        labels_dict = {
            "uri": "post1",
            "prob_toxic": 0.8,
            "prob_constructive": 0.6,
            "prob_severe_toxic": 0.9,
            "prob_identity_attack": 0.2,
            "prob_insult": 0.7,
            "prob_profanity": 0.1,
            "prob_threat": 0.3,
            "prob_affinity": 0.8,
            "prob_compassion": 0.9,
            "prob_curiosity": 0.7,
            "prob_nuance": 0.6,
            "prob_personal_story": 0.8,
            "prob_reasoning": 0.7,
            "prob_respect": 0.9,
            "prob_alienation": 0.2,
            "prob_fearmongering": 0.1,
            "prob_generalization": 0.4,
            "prob_moral_outrage": 0.3,
            "prob_scapegoating": 0.1,
            "prob_sexually_explicit": 0.0,
            "prob_flirtation": 0.2,
            "prob_spam": 0.1
        }
        
        expected_fields = [
            "prob_toxic", "prob_constructive", "prob_severe_toxic",
            "prob_identity_attack", "prob_insult", "prob_profanity",
            "prob_threat", "prob_affinity", "prob_compassion",
            "prob_curiosity", "prob_nuance", "prob_personal_story",
            "prob_reasoning", "prob_respect", "prob_alienation",
            "prob_fearmongering", "prob_generalization", "prob_moral_outrage",
            "prob_scapegoating", "prob_sexually_explicit", "prob_flirtation", "prob_spam"
        ]

        # Act
        result = transform_labels_dict(integration, labels_dict)

        # Assert
        assert isinstance(result, dict)
        assert len(result) == len(expected_fields)
        for field in expected_fields:
            assert field in result
            assert result[field] == labels_dict[field]

    def test_transforms_sociopolitical_labels_correctly(self):
        """Test transformation of sociopolitical labels.

        This test verifies that:
        1. Sociopolitical label fields are correctly mapped
        2. The is_not_sociopolitical field is correctly negated
        3. Political ideology labels are correctly converted to boolean
        4. The result contains the expected label structure
        """
        # Arrange
        integration = "sociopolitical"
        labels_dict = {
            "uri": "post1",
            "is_sociopolitical": True,
            "political_ideology_label": "left"
        }
        
        expected = {
            "is_sociopolitical": True,
            "is_not_sociopolitical": False,
            "is_political_left": True,
            "is_political_right": False,
            "is_political_moderate": False,
            "is_political_unclear": False
        }

        # Act
        result = transform_labels_dict(integration, labels_dict)

        # Assert
        assert result == expected

    def test_transforms_ime_labels_correctly(self):
        """Test transformation of IME labels.

        This test verifies that:
        1. IME label fields are correctly mapped
        2. The transformation preserves the original values
        3. The result contains the expected label structure
        """
        # Arrange
        integration = "ime"
        labels_dict = {
            "uri": "post1",
            "prob_intergroup": 0.7,
            "prob_moral": 0.8,
            "prob_emotion": 0.6,
            "prob_other": 0.3
        }
        
        expected = {
            "prob_intergroup": 0.7,
            "prob_moral": 0.8,
            "prob_emotion": 0.6,
            "prob_other": 0.3
        }

        # Act
        result = transform_labels_dict(integration, labels_dict)

        # Assert
        assert result == expected

    def test_transforms_valence_classifier_labels_correctly(self):
        """Test transformation of valence classifier labels.

        This test verifies that:
        1. Valence classifier label fields are correctly mapped
        2. The compound score is mapped to valence_clf_score
        3. Valence labels are correctly converted to boolean
        4. The result contains the expected label structure
        """
        # Arrange
        integration = "valence_classifier"
        labels_dict = {
            "uri": "post1",
            "compound": 0.7,
            "valence_label": "positive"
        }
        
        expected = {
            "valence_clf_score": 0.7,
            "is_valence_positive": True,
            "is_valence_negative": False,
            "is_valence_neutral": False
        }

        # Act
        result = transform_labels_dict(integration, labels_dict)

        # Assert
        assert result == expected

    def test_raises_error_for_invalid_integration(self):
        """Test that invalid integration raises ValueError.

        This test verifies that:
        1. Invalid integration types are properly validated
        2. The function raises a descriptive error message
        3. The error handling prevents invalid transformations
        """
        # Arrange
        integration = "invalid_integration"
        labels_dict = {"uri": "post1", "prob_toxic": 0.8}
        expected_error = "Invalid integration for labeling: invalid_integration"

        # Act & Assert
        with pytest.raises(ValueError, match=expected_error):
            transform_labels_dict(integration, labels_dict)

    def test_handles_different_political_ideology_values(self):
        """Test handling of different political ideology values.

        This test verifies that:
        1. All political ideology values are handled correctly
        2. The boolean conversion works for all expected values
        3. Edge cases are handled properly
        """
        # Arrange
        integration = "sociopolitical"
        
        test_cases = [
            ("left", {"is_political_left": True, "is_political_right": False, "is_political_moderate": False, "is_political_unclear": False}),
            ("right", {"is_political_left": False, "is_political_right": True, "is_political_moderate": False, "is_political_unclear": False}),
            ("moderate", {"is_political_left": False, "is_political_right": False, "is_political_moderate": True, "is_political_unclear": False}),
            ("unclear", {"is_political_left": False, "is_political_right": False, "is_political_moderate": False, "is_political_unclear": True})
        ]

        # Act & Assert
        for ideology, expected_booleans in test_cases:
            labels_dict = {
                "uri": "post1",
                "is_sociopolitical": True,
                "political_ideology_label": ideology
            }
            
            result = transform_labels_dict(integration, labels_dict)
            
            for field, expected_value in expected_booleans.items():
                assert result[field] == expected_value, f"Failed for ideology '{ideology}', field '{field}'"

    def test_handles_different_valence_labels(self):
        """Test handling of different valence label values.

        This test verifies that:
        1. All valence label values are handled correctly
        2. The boolean conversion works for all expected values
        3. Edge cases are handled properly
        """
        # Arrange
        integration = "valence_classifier"
        
        test_cases = [
            ("positive", {"is_valence_positive": True, "is_valence_negative": False, "is_valence_neutral": False}),
            ("negative", {"is_valence_positive": False, "is_valence_negative": True, "is_valence_neutral": False}),
            ("neutral", {"is_valence_positive": False, "is_valence_negative": False, "is_valence_neutral": True})
        ]

        # Act & Assert
        for valence, expected_booleans in test_cases:
            labels_dict = {
                "uri": "post1",
                "compound": 0.5,
                "valence_label": valence
            }
            
            result = transform_labels_dict(integration, labels_dict)
            
            for field, expected_value in expected_booleans.items():
                assert result[field] == expected_value, f"Failed for valence '{valence}', field '{field}'"


class TestGetAllLabelsForPosts:
    """Tests for get_all_labels_for_posts function."""

    def test_aggregates_labels_from_all_integrations(self):
        """Test aggregation of labels from all integrations.

        This test verifies that:
        1. Labels from all integrations are loaded and aggregated
        2. The URI to labels mapping is correctly constructed
        3. Labels are properly transformed for each integration
        4. The result contains the expected structure
        """
        # Arrange
        post_uris = {"post1", "post2"}
        partition_dates = ["2024-01-01", "2024-01-02"]
        
        # Mock the integrations list
        mock_integrations = ["perspective_api", "sociopolitical", "ime", "valence_classifier"]
        
        # Mock data for each integration is now imported from mock_labels.py

        # Act
        with patch("services.calculate_analytics.shared.data_loading.labels.integrations_list", mock_integrations), \
             patch("services.calculate_analytics.shared.data_loading.labels.get_labels_for_partition_date") as mock_get_labels:

            # Configure the mock to return the correct data based on integration and partition_date
            def mock_get_labels_side_effect(integration, partition_date):
                if integration == "perspective_api":
                    return mock_perspective_data
                elif integration == "sociopolitical":
                    return mock_sociopolitical_data
                elif integration == "ime":
                    return mock_ime_data
                elif integration == "valence_classifier":
                    return mock_valence_data
                else:
                    raise ValueError(f"Unknown integration: {integration}")

            mock_get_labels.side_effect = mock_get_labels_side_effect
            result = get_all_labels_for_posts(post_uris, partition_dates)

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 2
        assert "post1" in result
        assert "post2" in result
        
        # Check that post1 has all expected labels
        post1_labels = result["post1"]
    
        # Check perspective_api labels (should be 22 fields)
        for perspective_api_label in perspective_api_labels:
            assert perspective_api_label in post1_labels, f"Missing perspective_api label: {perspective_api_label}"
    
        # Check sociopolitical labels (should be 2 fields)
        for sociopolitical_label in sociopolitical_labels:
            assert sociopolitical_label in post1_labels, f"Missing sociopolitical label: {sociopolitical_label}"
    
        # Check IME labels (should be 4 fields)
        for ime_label in ime_labels:
            assert ime_label in post1_labels, f"Missing IME label: {ime_label}"
    
        # Check valence classifier labels (should be 2 fields)
        for valence_label in valence_labels:
            assert valence_label in post1_labels, f"Missing valence label: {valence_label}"
        
        # Check that post2 has all expected labels
        post2_labels = result["post2"]

        for perspective_api_label in perspective_api_labels:
            assert perspective_api_label in post2_labels

        for sociopolitical_label in sociopolitical_labels:
            assert sociopolitical_label in post2_labels

        for ime_label in ime_labels:
            assert ime_label in post2_labels

        for valence_label in valence_labels:
            assert valence_label in post2_labels

    def test_handles_empty_post_uris(self):
        """Test handling of empty post URIs set.

        This test verifies that:
        1. Empty post URIs set is handled gracefully
        2. Function returns empty dictionary
        3. Edge case doesn't cause crashes
        """
        # Arrange
        post_uris = set()
        partition_dates = ["2024-01-01"]
        
        mock_integrations = ["perspective_api"]

        # Act
        with patch("services.calculate_analytics.shared.data_loading.labels.integrations_list", mock_integrations):
            result = get_all_labels_for_posts(post_uris, partition_dates)

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 0

    def test_handles_empty_partition_dates(self):
        """Test handling of empty partition dates list.

        This test verifies that:
        1. Empty partition dates list is handled gracefully
        2. Function returns empty URI to labels mapping
        3. Edge case doesn't cause crashes
        """
        # Arrange
        post_uris = {"post1", "post2"}
        partition_dates = []
        
        mock_integrations = ["perspective_api"]

        # Act
        with patch("services.calculate_analytics.shared.data_loading.labels.integrations_list", mock_integrations):
            result = get_all_labels_for_posts(post_uris, partition_dates)

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 2
        # All URIs should have empty label dictionaries since no partition dates were processed
        for uri in post_uris:
            assert result[uri] == {}

    def test_raises_exception_on_get_labels_failure(self):
        """Test that exceptions from get_labels_for_partition_date are properly propagated.

        This test verifies that:
        1. Exceptions from the dependency are caught and logged
        2. The original exception is re-raised
        3. Error handling works correctly
        """
        # Arrange
        post_uris = {"post1"}
        partition_dates = ["2024-01-01"]
        
        mock_integrations = ["perspective_api"]
        expected_error = "Failed to get labels for partition date"
        mock_get_labels = Mock(side_effect=Exception(expected_error))

        # Act & Assert
        with patch("services.calculate_analytics.shared.data_loading.labels.integrations_list", mock_integrations), \
             patch("services.calculate_analytics.shared.data_loading.labels.get_labels_for_partition_date", side_effect=mock_get_labels):
            with pytest.raises(Exception, match=expected_error):
                get_all_labels_for_posts(post_uris, partition_dates)

    def test_handles_transform_labels_failure_gracefully(self):
        """Test that exceptions from transform_labels_dict are properly caught and logged.

        This test verifies that:
        1. Exceptions from the dependency are caught and logged
        2. The function continues processing other integrations
        3. Error handling works correctly without crashing
        """
        # Arrange
        post_uris = {"post1"}
        partition_dates = ["2024-01-01"]
    
        mock_integrations = ["perspective_api"]
        expected_error = "Failed to transform labels"
        mock_transform = Mock(side_effect=Exception(expected_error))
    
        # Act
        with patch("services.calculate_analytics.shared.data_loading.labels.integrations_list", mock_integrations), \
             patch("services.calculate_analytics.shared.data_loading.labels.get_labels_for_partition_date", return_value=mock_perspective_data), \
             patch("services.calculate_analytics.shared.data_loading.labels.transform_labels_dict", side_effect=mock_transform):
            result = get_all_labels_for_posts(post_uris, partition_dates)
    
        # Assert
        # The function should handle the error gracefully and return empty results
        assert isinstance(result, dict)
        assert len(result) == 1
        assert "post1" in result
        # post1 should have no labels since the transform failed
        assert result["post1"] == {}
