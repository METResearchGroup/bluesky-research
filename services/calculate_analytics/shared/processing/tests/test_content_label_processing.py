"""Tests for content_label_processing.py.

This test suite verifies the functionality of content label processing functions:
- Label collection and aggregation across posts
- Calculation of averages and proportions for different label types
- Metric field name transformation
- Content metrics calculation for feeds and engagement
- Handling of missing values (None, NAType) in calculations
"""

import pytest
import numpy as np
import pandas as pd

from services.calculate_analytics.shared.processing.content_label_processing import (
    collect_labels_for_post_uris,
    _calculate_average_for_probability_label,
    _calculate_average_for_score_label,
    _calculate_average_for_boolean_label,
    calculate_average_for_label,
    calculate_averages_for_content_labels,
    _calculate_proportion_for_probability_label,
    _calculate_proportion_for_score_label,
    _calculate_proportion_for_boolean_label,
    calculate_proportion_for_label,
    calculate_proportions_for_content_labels,
    transform_metric_field_names,
    calculate_metrics_for_content_labels,
    get_metrics_for_user_feeds_from_partition_date,
    get_metrics_for_record_type,
    get_metrics_for_record_types,
    flatten_content_metrics_across_record_types,
)


class TestCollectLabelsForPostUris:
    """Tests for collect_labels_for_post_uris function."""

    def test_collects_labels_from_multiple_posts(self):
        """Test collecting labels from multiple posts with complete data."""
        # Arrange
        post_uris = ["post1", "post2", "post3"]
        labels_for_content = {
            "post1": {
                "prob_toxic": 0.8,
                "prob_constructive": 0.6,
                "is_sociopolitical": True,
            },
            "post2": {
                "prob_toxic": 0.3,
                "prob_constructive": 0.9,
                "is_sociopolitical": False,
            },
            "post3": {
                "prob_toxic": 0.7,
                "prob_constructive": 0.4,
                "is_sociopolitical": True,
            },
        }
        expected_toxic = [0.8, 0.3, 0.7]
        expected_constructive = [0.6, 0.9, 0.4]
        expected_sociopolitical = [True, False, True]

        # Act
        result = collect_labels_for_post_uris(post_uris, labels_for_content)

        # Assert
        assert result["prob_toxic"] == expected_toxic
        assert result["prob_constructive"] == expected_constructive
        assert result["is_sociopolitical"] == expected_sociopolitical

    def test_handles_missing_labels(self):
        """Test handling of posts with missing or None labels.

        This test verifies that:
        1. Posts with missing labels are handled gracefully
        2. None values are filtered out from the results
        3. Only valid label values are included in the aggregated lists
        """
        # Arrange
        post_uris = ["post1", "post2"]
        labels_for_content = {
            "post1": {"prob_toxic": 0.8, "prob_constructive": None},
            "post2": {"prob_toxic": 0.3, "prob_constructive": 0.9},
        }
        expected_toxic = [0.8, 0.3]
        expected_constructive = [0.9]  # None values are filtered out

        # Act
        result = collect_labels_for_post_uris(post_uris, labels_for_content)

        # Assert
        assert result["prob_toxic"] == expected_toxic
        assert result["prob_constructive"] == expected_constructive

    def test_handles_empty_post_list(self):
        """Test handling of empty post list.

        This test verifies that:
        1. Empty post list is handled gracefully
        2. Result contains empty lists for all label types
        3. Function doesn't crash on edge case input
        """
        # Arrange
        post_uris = []
        labels_for_content = {}
        expected_empty_lists = True

        # Act
        result = collect_labels_for_post_uris(post_uris, labels_for_content)

        # Assert
        assert all(len(value) == 0 for value in result.values()) == expected_empty_lists

    def test_handles_none_values_correctly(self):
        """Test that None values are properly filtered out from all label types.

        This test verifies that:
        1. None values in probability labels are filtered out
        2. None values in boolean labels are filtered out
        3. The filtering logic works consistently across different label types
        """
        # Arrange
        post_uris = ["post1", "post2", "post3"]
        labels_for_content = {
            "post1": {"prob_toxic": 0.8, "prob_constructive": None},
            "post2": {"prob_toxic": None, "prob_constructive": 0.9},
            "post3": {"prob_toxic": 0.7, "prob_constructive": 0.4},
        }
        expected_toxic = [0.8, 0.7]  # None filtered out
        expected_constructive = [0.9, 0.4]  # None filtered out

        # Act
        result = collect_labels_for_post_uris(post_uris, labels_for_content)

        # Assert
        assert result["prob_toxic"] == expected_toxic
        assert result["prob_constructive"] == expected_constructive


class TestCalculateAverageForProbabilityLabel:
    """Tests for _calculate_average_for_probability_label function."""

    def test_calculates_correct_average(self):
        """Test correct average calculation for probability labels.

        This test verifies that:
        1. The mean is calculated correctly from the input values
        2. The result is rounded to three decimal places
        3. The calculation matches numpy's mean function
        """
        # Arrange
        label_values = [0.8, 0.3, 0.7, 0.9]
        expected = round(np.mean([0.8, 0.3, 0.7, 0.9]), 3)

        # Act
        result = _calculate_average_for_probability_label(label_values)

        # Assert
        assert result == expected

    def test_returns_none_for_empty_list(self):
        """Test returns None for empty list.

        This test verifies that:
        1. Empty input list is handled gracefully
        2. Function returns None instead of crashing
        3. Edge case is properly handled
        """
        # Arrange
        label_values = []
        expected = None

        # Act
        result = _calculate_average_for_probability_label(label_values)

        # Assert
        assert result is expected

    def test_rounds_to_three_decimal_places(self):
        """Test that result is rounded to three decimal places.

        This test verifies that:
        1. The rounding logic works correctly
        2. Results are consistently formatted
        3. Precision is maintained at the specified level
        """
        # Arrange
        label_values = [0.123456, 0.789012]
        expected = round(np.mean([0.123456, 0.789012]), 3)

        # Act
        result = _calculate_average_for_probability_label(label_values)

        # Assert
        assert result == expected

    def test_handles_none_values(self):
        """Test handling of None values in probability labels.

        This test verifies that:
        1. None values are filtered out before calculation
        2. The average is calculated only from valid values
        3. The function doesn't crash on None values
        """
        # Arrange
        label_values = [0.8, None, 0.7, 0.9]
        expected = round(np.mean([0.8, 0.7, 0.9]), 3)

        # Act
        result = _calculate_average_for_probability_label(label_values)

        # Assert
        assert result == expected

    def test_handles_pandas_natype_values(self):
        """Test handling of pandas NAType values in probability labels.

        This test verifies that:
        1. pandas NAType values are filtered out before calculation
        2. The average is calculated only from valid values
        3. The function doesn't crash on NAType values
        """
        # Arrange
        label_values = [0.8, pd.NA, 0.7, 0.9]
        expected = round(np.mean([0.8, 0.7, 0.9]), 3)

        # Act
        result = _calculate_average_for_probability_label(label_values)

        # Assert
        assert result == expected

    def test_handles_mixed_missing_values(self):
        """Test handling of mixed None and NAType values.

        This test verifies that:
        1. Both None and NAType values are filtered out
        2. The average is calculated only from valid values
        3. The function handles mixed missing value types
        """
        # Arrange
        label_values = [0.8, None, pd.NA, 0.7, 0.9]
        expected = round(np.mean([0.8, 0.7, 0.9]), 3)

        # Act
        result = _calculate_average_for_probability_label(label_values)

        # Assert
        assert result == expected

    def test_returns_none_when_all_values_missing(self):
        """Test returns None when all values are missing.

        This test verifies that:
        1. When all values are None or NAType, function returns None
        2. The function doesn't crash on all-missing data
        3. Edge case is handled gracefully
        """
        # Arrange
        label_values = [None, pd.NA, None]
        expected = None

        # Act
        result = _calculate_average_for_probability_label(label_values)

        # Assert
        assert result is expected


class TestCalculateAverageForScoreLabel:
    """Tests for _calculate_average_for_score_label function."""

    def test_calculates_correct_average(self):
        """Test correct average calculation for score labels.

        This test verifies that:
        1. The mean is calculated correctly from the input values
        2. The result is rounded to three decimal places
        3. The calculation matches numpy's mean function
        """
        # Arrange
        label_values = [0.5, 0.8, 0.2, 0.9]
        expected = round(np.mean([0.5, 0.8, 0.2, 0.9]), 3)

        # Act
        result = _calculate_average_for_score_label(label_values)

        # Assert
        assert result == expected

    def test_returns_none_for_empty_list(self):
        """Test returns None for empty list.

        This test verifies that:
        1. Empty input list is handled gracefully
        2. Function returns None instead of crashing
        3. Edge case is properly handled
        """
        # Arrange
        label_values = []
        expected = None

        # Act
        result = _calculate_average_for_score_label(label_values)

        # Assert
        assert result is expected

    def test_rounds_to_three_decimal_places(self):
        """Test that result is rounded to three decimal places.

        This test verifies that:
        1. The rounding logic works correctly
        2. Results are consistently formatted
        3. Precision is maintained at the specified level
        """
        # Arrange
        label_values = [0.123456, 0.789012]
        expected = round(np.mean([0.123456, 0.789012]), 3)

        # Act
        result = _calculate_average_for_score_label(label_values)

        # Assert
        assert result == expected

    def test_handles_none_values(self):
        """Test handling of None values in score labels.

        This test verifies that:
        1. None values are filtered out before calculation
        2. The average is calculated only from valid values
        3. The function doesn't crash on None values
        """
        # Arrange
        label_values = [0.5, None, 0.2, 0.9]
        expected = round(np.mean([0.5, 0.2, 0.9]), 3)

        # Act
        result = _calculate_average_for_score_label(label_values)

        # Assert
        assert result == expected

    def test_handles_pandas_natype_values(self):
        """Test handling of pandas NAType values in score labels.

        This test verifies that:
        1. pandas NAType values are filtered out before calculation
        2. The average is calculated only from valid values
        3. The function doesn't crash on NAType values
        """
        # Arrange
        label_values = [0.5, pd.NA, 0.2, 0.9]
        expected = round(np.mean([0.5, 0.2, 0.9]), 3)

        # Act
        result = _calculate_average_for_score_label(label_values)

        # Assert
        assert result == expected

    def test_returns_none_when_all_values_missing(self):
        """Test returns None when all values are missing.

        This test verifies that:
        1. When all values are None or NAType, function returns None
        2. The function doesn't crash on all-missing data
        3. Edge case is handled gracefully
        """
        # Arrange
        label_values = [None, pd.NA, None]
        expected = None

        # Act
        result = _calculate_average_for_score_label(label_values)

        # Assert
        assert result is expected


class TestCalculateAverageForBooleanLabel:
    """Tests for _calculate_average_for_boolean_label function."""

    def test_calculates_correct_average_for_booleans(self):
        """Test correct average calculation for boolean labels.

        This test verifies that:
        1. True values are treated as 1 and False as 0
        2. The mean is calculated correctly from the converted values
        3. The result is rounded to three decimal places
        """
        # Arrange
        label_values = [True, False, True, True, False]
        expected = 0.6  # True=1, False=0, so average should be 3/5 = 0.6

        # Act
        result = _calculate_average_for_boolean_label(label_values)

        # Assert
        assert result == expected

    def test_returns_none_for_empty_list(self):
        """Test returns None for empty list.

        This test verifies that:
        1. Empty input list is handled gracefully
        2. Function returns None instead of crashing
        3. Edge case is properly handled
        """
        # Arrange
        label_values = []
        expected = None

        # Act
        result = _calculate_average_for_boolean_label(label_values)

        # Assert
        assert result is expected

    def test_handles_mixed_boolean_values(self):
        """Test handling of mixed boolean values.

        This test verifies that:
        1. Mixed True/False values are handled correctly
        2. The proportion calculation works for partial True values
        3. Results are properly rounded
        """
        # Arrange
        label_values = [True, True, False]
        expected = 0.667  # 2 True, 1 False = 2/3 = 0.667

        # Act
        result = _calculate_average_for_boolean_label(label_values)

        # Assert
        assert result == expected

    def test_handles_all_true_values(self):
        """Test handling when all values are True.

        This test verifies that:
        1. All True values result in 1.0 average
        2. Edge case of uniform values is handled correctly
        """
        # Arrange
        label_values = [True, True, True]
        expected = 1.0

        # Act
        result = _calculate_average_for_boolean_label(label_values)

        # Assert
        assert result == expected

    def test_handles_all_false_values(self):
        """Test handling when all values are False.

        This test verifies that:
        1. All False values result in 0.0 average
        2. Edge case of uniform values is handled correctly
        """
        # Arrange
        label_values = [False, False, False]
        expected = 0.0

        # Act
        result = _calculate_average_for_boolean_label(label_values)

        # Assert
        assert result == expected

    def test_handles_none_values(self):
        """Test handling of None values in boolean labels.

        This test verifies that:
        1. None values are filtered out before calculation
        2. The average is calculated only from valid boolean values
        3. The function doesn't crash on None values
        """
        # Arrange
        label_values = [True, None, False, True]
        expected = 0.667  # 2 True, 1 False = 2/3 = 0.667

        # Act
        result = _calculate_average_for_boolean_label(label_values)

        # Assert
        assert result == expected

    def test_handles_pandas_natype_values(self):
        """Test handling of pandas NAType values in boolean labels.

        This test verifies that:
        1. pandas NAType values are filtered out before calculation
        2. The average is calculated only from valid boolean values
        3. The function doesn't crash on NAType values
        """
        # Arrange
        label_values = [True, pd.NA, False, True]
        expected = 0.667  # 2 True, 1 False = 2/3 = 0.667

        # Act
        result = _calculate_average_for_boolean_label(label_values)

        # Assert
        assert result == expected

    def test_returns_none_when_all_values_missing(self):
        """Test returns None when all values are missing.

        This test verifies that:
        1. When all values are None or NAType, function returns None
        2. The function doesn't crash on all-missing data
        3. Edge case is handled gracefully
        """
        # Arrange
        label_values = [None, pd.NA, None]
        expected = None

        # Act
        result = _calculate_average_for_boolean_label(label_values)

        # Assert
        assert result is expected


class TestCalculateAverageForLabel:
    """Tests for calculate_average_for_label function."""

    def test_handles_probability_labels(self):
        """Test handling of probability type labels.

        This test verifies that:
        1. Probability labels are routed to the correct calculation function
        2. The result matches the expected probability average
        3. The label type detection works correctly
        """
        # Arrange
        label = "prob_toxic"
        label_values = [0.8, 0.3, 0.7]
        expected = round(np.mean([0.8, 0.3, 0.7]), 3)

        # Act
        result = calculate_average_for_label(label, label_values)

        # Assert
        assert result == expected

    def test_handles_score_labels(self):
        """Test handling of score type labels.

        This test verifies that:
        1. Score labels are routed to the correct calculation function
        2. The result matches the expected score average
        3. The label type detection works correctly
        """
        # Arrange
        label = "valence_clf_score"
        label_values = [0.5, 0.8, 0.2]
        expected = round(np.mean([0.5, 0.8, 0.2]), 3)

        # Act
        result = calculate_average_for_label(label, label_values)

        # Assert
        assert result == expected

    def test_handles_boolean_labels(self):
        """Test handling of boolean type labels.

        This test verifies that:
        1. Boolean labels are routed to the correct calculation function
        2. The result matches the expected boolean average
        3. The label type detection works correctly
        """
        # Arrange
        label = "is_sociopolitical"
        label_values = [True, False, True]
        expected = 0.667  # 2 True, 1 False = 2/3 = 0.667

        # Act
        result = calculate_average_for_label(label, label_values)

        # Assert
        assert result == expected

    def test_returns_none_for_empty_lists(self):
        """Test returns None for empty label value lists.

        This test verifies that:
        1. Empty input lists are handled gracefully
        2. Function returns None instead of crashing
        3. Edge case is properly handled across all label types
        """
        # Arrange
        label = "prob_toxic"
        label_values = []
        expected = None

        # Act
        result = calculate_average_for_label(label, label_values)

        # Assert
        assert result is expected


class TestCalculateAveragesForContentLabels:
    """Tests for calculate_averages_for_content_labels function."""

    def test_calculates_averages_for_all_labels(self):
        """Test that averages are calculated for all labels.

        This test verifies that:
        1. All labels in the input dictionary are processed
        2. Each label gets its average calculated correctly
        3. The result contains all expected label keys
        4. Values are properly rounded to three decimal places
        """
        # Arrange
        aggregated_label_to_label_values = {
            "prob_toxic": [0.8, 0.3, 0.7],
            "prob_constructive": [0.6, 0.9, 0.4],
            "is_sociopolitical": [True, False, True],
            "valence_clf_score": [0.5, 0.8, 0.2],
        }
        expected_toxic = round(np.mean([0.8, 0.3, 0.7]), 3)
        expected_constructive = round(np.mean([0.6, 0.9, 0.4]), 3)
        expected_sociopolitical = round(np.mean([True, False, True]), 3)
        expected_valence = round(np.mean([0.5, 0.8, 0.2]), 3)

        # Act
        result = calculate_averages_for_content_labels(aggregated_label_to_label_values)

        # Assert
        assert result["prob_toxic"] == expected_toxic
        assert result["prob_constructive"] == expected_constructive
        assert result["is_sociopolitical"] == expected_sociopolitical
        assert result["valence_clf_score"] == expected_valence

    def test_handles_empty_lists(self):
        """Test handling of empty label value lists.

        This test verifies that:
        1. Empty lists result in None values
        2. Non-empty lists are processed normally
        3. The function handles mixed empty/non-empty inputs gracefully
        """
        # Arrange
        aggregated_label_to_label_values = {
            "prob_toxic": [],
            "prob_constructive": [0.6, 0.9],
            "is_sociopolitical": [],
        }
        expected_toxic = None
        expected_constructive = round(np.mean([0.6, 0.9]), 3)
        expected_sociopolitical = None

        # Act
        result = calculate_averages_for_content_labels(aggregated_label_to_label_values)

        # Assert
        assert result["prob_toxic"] is expected_toxic
        assert result["prob_constructive"] == expected_constructive
        assert result["is_sociopolitical"] is expected_sociopolitical


class TestCalculateProportionForProbabilityLabel:
    """Tests for _calculate_proportion_for_probability_label function."""

    def test_calculates_proportion_above_threshold(self):
        """Test proportion calculation for probability labels.

        This test verifies that:
        1. The proportion of values above the threshold is calculated correctly
        2. The threshold comparison works as expected
        3. The result is rounded to three decimal places
        """
        # Arrange
        label_values = [0.8, 0.3, 0.7, 0.9]
        threshold = 0.5
        # Values above 0.5: [0.8, 0.7, 0.9] = 3/4 = 0.75
        expected = round(np.mean(np.array([0.8, 0.3, 0.7, 0.9]) >= 0.5), 3)

        # Act
        result = _calculate_proportion_for_probability_label(label_values, threshold)

        # Assert
        assert result == expected

    def test_returns_none_for_empty_list(self):
        """Test returns None for empty list.

        This test verifies that:
        1. Empty input list is handled gracefully
        2. Function returns None instead of crashing
        3. Edge case is properly handled
        """
        # Arrange
        label_values = []
        threshold = 0.5
        expected = None

        # Act
        result = _calculate_proportion_for_probability_label(label_values, threshold)

        # Assert
        assert result is expected

    def test_handles_none_values(self):
        """Test handling of None values in probability labels.

        This test verifies that:
        1. None values are filtered out before calculation
        2. The proportion is calculated only from valid values
        3. The function doesn't crash on None values
        """
        # Arrange
        label_values = [0.8, None, 0.3, 0.7]
        threshold = 0.5
        # Values above 0.5: [0.8, 0.7] = 2/3 = 0.667
        expected = round(np.mean(np.array([0.8, 0.3, 0.7]) >= 0.5), 3)

        # Act
        result = _calculate_proportion_for_probability_label(label_values, threshold)

        # Assert
        assert result == expected

    def test_handles_pandas_natype_values(self):
        """Test handling of pandas NAType values in probability labels.

        This test verifies that:
        1. pandas NAType values are filtered out before calculation
        2. The proportion is calculated only from valid values
        3. The function doesn't crash on NAType values
        """
        # Arrange
        label_values = [0.8, pd.NA, 0.3, 0.7]
        threshold = 0.5
        # Values above 0.5: [0.8, 0.7] = 2/3 = 0.667
        expected = round(np.mean(np.array([0.8, 0.3, 0.7]) >= 0.5), 3)

        # Act
        result = _calculate_proportion_for_probability_label(label_values, threshold)

        # Assert
        assert result == expected

    def test_returns_none_when_all_values_missing(self):
        """Test returns None when all values are missing.

        This test verifies that:
        1. When all values are None or NAType, function returns None
        2. The function doesn't crash on all-missing data
        3. Edge case is handled gracefully
        """
        # Arrange
        label_values = [None, pd.NA, None]
        threshold = 0.5
        expected = None

        # Act
        result = _calculate_proportion_for_probability_label(label_values, threshold)

        # Assert
        assert result is expected


class TestCalculateProportionForScoreLabel:
    """Tests for _calculate_proportion_for_score_label function."""

    def test_returns_none_for_score_labels(self):
        """Test that score labels return None for proportion calculation.

        This test verifies that:
        1. Score labels consistently return None for proportions
        2. The function handles score labels correctly
        3. The business logic for score labels is implemented properly
        """
        # Arrange
        label_values = [0.5, 0.8, 0.2]
        expected = None

        # Act
        result = _calculate_proportion_for_score_label(label_values)

        # Assert
        assert result is expected

    def test_returns_none_for_empty_list(self):
        """Test returns None for empty list.

        This test verifies that:
        1. Empty input list is handled gracefully
        2. Function returns None instead of crashing
        3. Edge case is properly handled
        """
        # Arrange
        label_values = []
        expected = None

        # Act
        result = _calculate_proportion_for_score_label(label_values)

        # Assert
        assert result is expected


class TestCalculateProportionForBooleanLabel:
    """Tests for _calculate_proportion_for_boolean_label function."""

    def test_calculates_proportion_of_true_values(self):
        """Test proportion calculation for boolean labels.

        This test verifies that:
        1. The proportion of True values is calculated correctly
        2. True values are treated as 1 and False as 0
        3. The result is rounded to three decimal places
        """
        # Arrange
        label_values = [True, False, True, True, False]
        expected = 0.6  # 3 True, 2 False = 3/5 = 0.6

        # Act
        result = _calculate_proportion_for_boolean_label(label_values)

        # Assert
        assert result == expected

    def test_returns_none_for_empty_list(self):
        """Test returns None for empty list.

        This test verifies that:
        1. Empty input list is handled gracefully
        2. Function returns None instead of crashing
        3. Edge case is properly handled
        """
        # Arrange
        label_values = []
        expected = None

        # Act
        result = _calculate_proportion_for_boolean_label(label_values)

        # Assert
        assert result is expected

    def test_handles_all_true_values(self):
        """Test handling when all values are True.

        This test verifies that:
        1. All True values result in 1.0 proportion
        2. Edge case of uniform values is handled correctly
        """
        # Arrange
        label_values = [True, True, True]
        expected = 1.0

        # Act
        result = _calculate_proportion_for_boolean_label(label_values)

        # Assert
        assert result == expected

    def test_handles_all_false_values(self):
        """Test handling when all values are False.

        This test verifies that:
        1. All False values result in 0.0 proportion
        2. Edge case of uniform values is handled correctly
        """
        # Arrange
        label_values = [False, False, False]
        expected = 0.0

        # Act
        result = _calculate_proportion_for_boolean_label(label_values)

        # Assert
        assert result == expected

    def test_handles_none_values(self):
        """Test handling of None values in boolean labels.

        This test verifies that:
        1. None values are filtered out before calculation
        2. The proportion is calculated only from valid boolean values
        3. The function doesn't crash on None values
        """
        # Arrange
        label_values = [True, None, False, True]
        expected = 0.667  # 2 True, 1 False = 2/3 = 0.667

        # Act
        result = _calculate_proportion_for_boolean_label(label_values)

        # Assert
        assert result == expected

    def test_handles_pandas_natype_values(self):
        """Test handling of pandas NAType values in boolean labels.

        This test verifies that:
        1. pandas NAType values are filtered out before calculation
        2. The proportion is calculated only from valid boolean values
        3. The function doesn't crash on NAType values
        """
        # Arrange
        label_values = [True, pd.NA, False, True]
        expected = 0.667  # 2 True, 1 False = 2/3 = 0.667

        # Act
        result = _calculate_proportion_for_boolean_label(label_values)

        # Assert
        assert result == expected

    def test_returns_none_when_all_values_missing(self):
        """Test returns None when all values are missing.

        This test verifies that:
        1. When all values are None or NAType, function returns None
        2. The function doesn't crash on all-missing data
        3. Edge case is handled gracefully
        """
        # Arrange
        label_values = [None, pd.NA, None]
        expected = None

        # Act
        result = _calculate_proportion_for_boolean_label(label_values)

        # Assert
        assert result is expected


class TestCalculateProportionForLabel:
    """Tests for calculate_proportion_for_label function."""

    def test_handles_probability_labels(self):
        """Test handling of probability type labels.

        This test verifies that:
        1. Probability labels are routed to the correct calculation function
        2. The threshold from LABELS_CONFIG is used correctly
        3. The result matches the expected proportion calculation
        """
        # Arrange
        label = "prob_toxic"
        label_values = [0.8, 0.3, 0.7, 0.9]
        # Threshold is 0.5, values above: [0.8, 0.7, 0.9] = 3/4 = 0.75
        expected = round(np.mean(np.array([0.8, 0.3, 0.7, 0.9]) > 0.5), 3)

        # Act
        result = calculate_proportion_for_label(label, label_values)

        # Assert
        assert result == expected

    def test_handles_score_labels(self):
        """Test handling of score type labels.

        This test verifies that:
        1. Score labels are routed to the correct calculation function
        2. Score labels return None for proportion calculation
        3. The label type detection works correctly
        """
        # Arrange
        label = "valence_clf_score"
        label_values = [0.5, 0.8, 0.2]
        expected = None  # Score labels return None for proportion

        # Act
        result = calculate_proportion_for_label(label, label_values)

        # Assert
        assert result is expected

    def test_handles_boolean_labels(self):
        """Test handling of boolean type labels.

        This test verifies that:
        1. Boolean labels are routed to the correct calculation function
        2. The result matches the expected boolean proportion
        3. The label type detection works correctly
        """
        # Arrange
        label = "is_sociopolitical"
        label_values = [True, False, True]
        expected = 0.667  # 2 True, 1 False = 2/3 = 0.667

        # Act
        result = calculate_proportion_for_label(label, label_values)

        # Assert
        assert result == expected


class TestCalculateProportionsForContentLabels:
    """Tests for calculate_proportions_for_content_labels function."""

    def test_calculates_proportions_for_all_labels(self):
        """Test that proportions are calculated for all labels.

        This test verifies that:
        1. All labels in the input dictionary are processed
        2. Each label gets its proportion calculated correctly
        3. The result contains all expected label keys
        4. The function handles different label types appropriately
        """
        # Arrange
        aggregated_label_to_label_values = {
            "prob_toxic": [0.8, 0.3, 0.7, 0.9],
            "prob_constructive": [0.6, 0.9, 0.4],
            "is_sociopolitical": [True, False, True],
            "valence_clf_score": [0.5, 0.8, 0.2],
        }
        expected_toxic = 0.75
        expected_constructive = 0.667
        expected_sociopolitical = 0.667
        expected_valence_clf_score = None

        # Act
        result = calculate_proportions_for_content_labels(
            aggregated_label_to_label_values
        )

        # Assert
        assert result["prob_toxic"] == expected_toxic
        assert result["prob_constructive"] == expected_constructive
        assert result["is_sociopolitical"] == expected_sociopolitical
        assert result["valence_clf_score"] is expected_valence_clf_score

    def test_handles_empty_lists(self):
        """Test handling of empty label value lists.

        This test verifies that:
        1. Empty lists result in None values
        2. Non-empty lists are processed normally
        3. The function handles mixed empty/non-empty inputs gracefully
        """
        # Arrange
        aggregated_label_to_label_values = {
            "prob_toxic": [],
            "prob_constructive": [0.6, 0.9],
            "is_sociopolitical": [],
        }
        expected_toxic = None
        expected_constructive = 1.0
        expected_sociopolitical = None

        # Act
        result = calculate_proportions_for_content_labels(
            aggregated_label_to_label_values
        )

        # Assert
        assert result["prob_toxic"] is expected_toxic
        assert result["prob_constructive"] == expected_constructive
        assert result["is_sociopolitical"] is expected_sociopolitical


class TestTransformMetricFieldNames:
    """Tests for transform_metric_field_names function."""

    def test_transforms_feed_metrics_correctly(self):
        """Test transformation of feed metrics.

        This test verifies that:
        1. Feed metrics get the correct 'feed_' prefix
        2. The metric type is properly appended
        3. All input metrics are transformed consistently
        4. The original metric names are replaced with new names
        """
        # Arrange
        metrics = {
            "prob_toxic": 0.6,
            "is_sociopolitical": 0.8,
            "valence_clf_score": 0.5,
        }
        metric_type = "average"
        interaction_type = "feed"
        args = {}
        expected = {
            "feed_average_toxic": 0.6,
            "feed_average_is_sociopolitical": 0.8,
            "feed_average_valence_clf_score": 0.5,
        }

        # Act
        result = transform_metric_field_names(
            metrics, metric_type, interaction_type, args
        )

        # Assert
        assert len(result) == len(expected)
        assert set(result.keys()) == set(expected.keys())
        for key, value in expected.items():
            assert result[key] == value

    def test_transforms_engagement_metrics_correctly(self):
        """Test transformation of engagement metrics.

        This test verifies that:
        1. Engagement metrics get the correct record type prefix
        2. The metric type is properly appended
        3. The record type is correctly mapped to the suffix
        4. All input metrics are transformed consistently
        """
        # Arrange
        metrics = {"prob_toxic": 0.6, "is_sociopolitical": 0.8}
        metric_type = "proportion"
        interaction_type = "engagement"
        args = {"record_type": "like"}
        expected = {
            "engagement_proportion_liked_posts_toxic": 0.6,
            "engagement_proportion_liked_posts_is_sociopolitical": 0.8,
        }

        # Act
        result = transform_metric_field_names(
            metrics, metric_type, interaction_type, args
        )

        # Assert
        assert len(result) == len(expected)
        assert set(result.keys()) == set(expected.keys())
        for key, value in expected.items():
            assert result[key] == value

    def test_handles_all_record_types(self):
        """Test handling of all record types.

        This test verifies that:
        1. All valid record types are handled correctly
        2. The correct prefix is generated for each record type
        3. The transformation is consistent across all record types
        4. The function maps record types to appropriate suffixes
        """
        # Arrange
        metrics = {"prob_toxic": 0.6}
        metric_type = "average"
        interaction_type = "engagement"

        record_types = ["like", "post", "repost", "reply"]
        expected_prefixes = [
            "engagement_average_liked_posts",
            "engagement_average_posted_posts",
            "engagement_average_reposted_posts",
            "engagement_average_replied_posts",
        ]

        # Act & Assert
        for record_type, expected_prefix in zip(record_types, expected_prefixes):
            args = {"record_type": record_type}
            result = transform_metric_field_names(
                metrics, metric_type, interaction_type, args
            )

            expected_key = f"{expected_prefix}_toxic"
            assert expected_key in result
            assert result[expected_key] == 0.6

    def test_raises_error_for_invalid_interaction_type(self):
        """Test that invalid interaction types raise ValueError.

        This test verifies that:
        1. Invalid interaction types are properly validated
        2. The function raises a descriptive error message
        3. The error handling prevents invalid transformations
        """
        # Arrange
        metrics = {"prob_toxic": 0.6}
        metric_type = "average"
        interaction_type = "invalid"
        args = {}
        expected_error = "Invalid interaction type: invalid"

        # Act & Assert
        with pytest.raises(ValueError, match=expected_error):
            transform_metric_field_names(metrics, metric_type, interaction_type, args)

    def test_raises_error_for_invalid_record_type(self):
        """Test that invalid record types raise ValueError.

        This test verifies that:
        1. Invalid record types are properly validated
        2. The function raises a descriptive error message
        3. The error handling prevents invalid transformations
        """
        # Arrange
        metrics = {"prob_toxic": 0.6}
        metric_type = "average"
        interaction_type = "engagement"
        args = {"record_type": "invalid"}
        expected_error = "Invalid record type: invalid"

        # Act & Assert
        with pytest.raises(ValueError, match=expected_error):
            transform_metric_field_names(metrics, metric_type, interaction_type, args)


class TestCalculateMetricsForContentLabels:
    """Tests for calculate_metrics_for_content_labels function."""

    def test_calculates_both_average_and_proportion_metrics(self):
        """Test that both average and proportion metrics are calculated.

        This test verifies that:
        1. Both average and proportion metrics are generated
        2. The metrics are properly transformed with correct prefixes
        3. All label types are processed correctly
        4. The result contains the expected metric structure
        """
        # Arrange
        record_type = "like"
        interaction_type = "engagement"
        aggregated_label_to_label_values = {
            "prob_toxic": [0.8, 0.3, 0.4],
            "is_sociopolitical": [True, False, True],
            "valence_clf_score": [0.5, 0.8, 0.2],
        }
        expected = {
            "engagement_average_liked_posts_toxic": 0.5,
            "engagement_proportion_liked_posts_toxic": 0.333,
            "engagement_average_liked_posts_is_sociopolitical": 0.667,
            "engagement_proportion_liked_posts_is_sociopolitical": 0.667,
            "engagement_average_liked_posts_valence_clf_score": 0.5,
            "engagement_proportion_liked_posts_valence_clf_score": None,
        }

        # Act
        result = calculate_metrics_for_content_labels(
            record_type, interaction_type, aggregated_label_to_label_values
        )

        # Assert
        for key, value in expected.items():
            if value is None:
                assert result[key] is None
            else:
                assert result[key] == value

    def test_handles_feed_interaction_type(self):
        """Test handling of feed interaction type.

        This test verifies that:
        1. Feed interaction type generates correct prefixes
        2. The metrics are properly transformed
        3. All label types are processed correctly
        4. The result contains the expected feed metric structure
        """
        # Arrange
        record_type = None
        interaction_type = "feed"
        aggregated_label_to_label_values = {
            "prob_toxic": [0.8, 0.3, 0.7],
            "is_sociopolitical": [True, False, True],
        }
        expected = {
            "feed_average_toxic": 0.6,
            "feed_proportion_toxic": 0.667,
            "feed_average_is_sociopolitical": 0.667,
            "feed_proportion_is_sociopolitical": 0.667,
        }

        # Act
        result = calculate_metrics_for_content_labels(
            record_type, interaction_type, aggregated_label_to_label_values
        )

        # Assert
        for key, value in expected.items():
            assert result[key] == value


class TestGetMetricsForUserFeedsFromPartitionDate:
    """Tests for get_metrics_for_user_feeds_from_partition_date function."""

    def test_calculates_feed_metrics_correctly(self):
        """Test calculation of feed metrics.

        This test verifies that:
        1. Feed metrics are calculated correctly from post data
        2. The metrics include both average and proportion calculations
        3. All label types are processed appropriately
        4. The result contains the expected feed metric structure
        """
        # Arrange
        post_uris = ["post1", "post2", "post3"]
        labels_for_feed_content = {
            "post1": {"prob_toxic": 0.8, "is_sociopolitical": True},
            "post2": {"prob_toxic": 0.3, "is_sociopolitical": False},
            "post3": {"prob_toxic": 0.7, "is_sociopolitical": True},
        }

        # Act
        result = get_metrics_for_user_feeds_from_partition_date(
            post_uris, labels_for_feed_content
        )

        # Assert
        # The function returns all labels from LABELS_CONFIG (72 total)
        # We only provided data for 2 labels, so the rest will be None
        # There are 36 labels and we calculate average + proportion for each,
        # hence we have 72 fields.
        assert len(result) == 72

        # Check that our specific labels have the correct calculated values
        assert result["feed_average_toxic"] == 0.6  # (0.8 + 0.3 + 0.7) / 3
        assert (
            result["feed_proportion_toxic"] == 0.667
        )  # 2/3 values above 0.5 threshold
        assert result["feed_average_is_sociopolitical"] == 0.667  # 2/3 True values
        assert result["feed_proportion_is_sociopolitical"] == 0.667  # 2/3 True values

        # Check that labels we didn't provide data for are None
        assert result["feed_average_constructive"] is None
        assert result["feed_average_severe_toxic"] is None
        assert result["feed_average_valence_clf_score"] is None

        # Verify the structure: all labels should have both average and proportion versions
        assert "feed_average_toxic" in result
        assert "feed_proportion_toxic" in result
        assert "feed_average_is_sociopolitical" in result
        assert "feed_proportion_is_sociopolitical" in result

    def test_handles_empty_post_list(self):
        """Test handling of empty post list.

        This test verifies that:
        1. Empty post list is handled gracefully
        2. Function returns a valid metrics structure
        3. Edge case doesn't cause crashes
        4. Result contains expected structure even with no data
        """
        # Arrange
        post_uris = []
        labels_for_feed_content = {}
        expected_type = dict

        # Act
        result = get_metrics_for_user_feeds_from_partition_date(
            post_uris, labels_for_feed_content
        )

        # Assert
        # Should still return metrics structure, but with None values
        assert isinstance(result, expected_type)
        assert len(result) == 72
        for key in result.keys():
            assert result[key] is None


class TestGetMetricsForRecordType:
    """Tests for get_metrics_for_record_type function."""

    def test_calculates_engagement_metrics_correctly(self):
        """Test calculation of engagement metrics for a specific record type.

        This test verifies that:
        1. Engagement metrics are calculated correctly from user data
        2. The metrics include both average and proportion calculations
        3. All label types are processed appropriately
        4. The result contains the expected engagement metric structure
        """
        # Arrange
        record_type = "like"
        user_to_content_engaged_with = {
            "user1": {"2024-01-01": {"like": ["post1", "post2", "post3"]}}
        }
        # Comprehensive test data covering all 36 labels from LABELS_CONFIG
        labels_for_engaged_content = {
            "post1": {
                # Perspective API labels (probability type)
                "prob_toxic": 0.8,
                "prob_constructive": 0.7,
                "prob_severe_toxic": 0.9,
                "prob_identity_attack": 0.2,
                "prob_insult": 0.6,
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
                "prob_spam": 0.1,
                # IME labels (probability type)
                "prob_intergroup": 0.7,
                "prob_moral": 0.8,
                "prob_emotion": 0.6,
                "prob_other": 0.3,
                # Valence classifier labels
                "valence_clf_score": 0.7,
                "is_valence_positive": True,
                "is_valence_negative": False,
                "is_valence_neutral": False,
                # LLM classifier labels
                "is_sociopolitical": True,
                "is_not_sociopolitical": False,
                "is_political_left": True,
                "is_political_right": False,
                "is_political_moderate": False,
                "is_political_unclear": False,
            },
            "post2": {
                # Perspective API labels (probability type)
                "prob_toxic": 0.3,
                "prob_constructive": 0.9,
                "prob_severe_toxic": 0.1,
                "prob_identity_attack": 0.1,
                "prob_insult": 0.2,
                "prob_profanity": 0.0,
                "prob_threat": 0.1,
                "prob_affinity": 0.6,
                "prob_compassion": 0.8,
                "prob_curiosity": 0.9,
                "prob_nuance": 0.8,
                "prob_personal_story": 0.7,
                "prob_reasoning": 0.8,
                "prob_respect": 0.9,
                "prob_alienation": 0.1,
                "prob_fearmongering": 0.0,
                "prob_generalization": 0.2,
                "prob_moral_outrage": 0.1,
                "prob_scapegoating": 0.0,
                "prob_sexually_explicit": 0.0,
                "prob_flirtation": 0.1,
                "prob_spam": 0.0,
                # IME labels (probability type)
                "prob_intergroup": 0.5,
                "prob_moral": 0.7,
                "prob_emotion": 0.8,
                "prob_other": 0.2,
                # Valence classifier labels
                "valence_clf_score": -0.8,
                "is_valence_positive": False,
                "is_valence_negative": True,
                "is_valence_neutral": False,
                # LLM classifier labels
                "is_sociopolitical": False,
                "is_not_sociopolitical": True,
                "is_political_left": False,
                "is_political_right": False,
                "is_political_moderate": True,
                "is_political_unclear": False,
            },
            "post3": {
                # Perspective API labels (probability type)
                "prob_toxic": 0.7,
                "prob_constructive": 0.6,
                "prob_severe_toxic": 0.4,
                "prob_identity_attack": 0.3,
                "prob_insult": 0.5,
                "prob_profanity": 0.2,
                "prob_threat": 0.2,
                "prob_affinity": 0.7,
                "prob_compassion": 0.6,
                "prob_curiosity": 0.8,
                "prob_nuance": 0.7,
                "prob_personal_story": 0.6,
                "prob_reasoning": 0.6,
                "prob_respect": 0.8,
                "prob_alienation": 0.3,
                "prob_fearmongering": 0.2,
                "prob_generalization": 0.5,
                "prob_moral_outrage": 0.4,
                "prob_scapegoating": 0.2,
                "prob_sexually_explicit": 0.1,
                "prob_flirtation": 0.3,
                "prob_spam": 0.1,
                # IME labels (probability type)
                "prob_intergroup": 0.6,
                "prob_moral": 0.6,
                "prob_emotion": 0.7,
                "prob_other": 0.4,
                # Valence classifier labels
                "valence_clf_score": 0.2,
                "is_valence_positive": False,
                "is_valence_negative": False,
                "is_valence_neutral": True,
                # LLM classifier labels
                "is_sociopolitical": True,
                "is_not_sociopolitical": False,
                "is_political_left": False,
                "is_political_right": True,
                "is_political_moderate": False,
                "is_political_unclear": False,
            },
        }
        did = "user1"
        partition_date = "2024-01-01"

        expected = {
            # Perspective API labels
            "engagement_average_liked_posts_toxic": 0.6,
            "engagement_proportion_liked_posts_toxic": 0.667,
            "engagement_average_liked_posts_constructive": 0.733,
            "engagement_proportion_liked_posts_constructive": 1.0,
            "engagement_average_liked_posts_severe_toxic": 0.467,
            "engagement_proportion_liked_posts_severe_toxic": 0.333,
            "engagement_average_liked_posts_identity_attack": 0.2,
            "engagement_proportion_liked_posts_identity_attack": 0.0,
            "engagement_average_liked_posts_insult": 0.433,
            "engagement_proportion_liked_posts_insult": 0.667,
            "engagement_average_liked_posts_profanity": 0.1,
            "engagement_proportion_liked_posts_profanity": 0.0,
            "engagement_average_liked_posts_threat": 0.2,
            "engagement_proportion_liked_posts_threat": 0.0,
            "engagement_average_liked_posts_affinity": 0.7,
            "engagement_proportion_liked_posts_affinity": 1.0,
            "engagement_average_liked_posts_compassion": 0.767,
            "engagement_proportion_liked_posts_compassion": 1.0,
            "engagement_average_liked_posts_curiosity": 0.8,
            "engagement_proportion_liked_posts_curiosity": 1.0,
            "engagement_average_liked_posts_nuance": 0.7,
            "engagement_proportion_liked_posts_nuance": 1.0,
            "engagement_average_liked_posts_personal_story": 0.7,
            "engagement_proportion_liked_posts_personal_story": 1.0,
            "engagement_average_liked_posts_reasoning": 0.7,
            "engagement_proportion_liked_posts_reasoning": 1.0,
            "engagement_average_liked_posts_respect": 0.867,
            "engagement_proportion_liked_posts_respect": 1.0,
            "engagement_average_liked_posts_alienation": 0.2,
            "engagement_proportion_liked_posts_alienation": 0.0,
            "engagement_average_liked_posts_fearmongering": 0.1,
            "engagement_proportion_liked_posts_fearmongering": 0.0,
            "engagement_average_liked_posts_generalization": 0.367,
            "engagement_proportion_liked_posts_generalization": 0.333,
            "engagement_average_liked_posts_moral_outrage": 0.267,
            "engagement_proportion_liked_posts_moral_outrage": 0.0,
            "engagement_average_liked_posts_scapegoating": 0.1,
            "engagement_proportion_liked_posts_scapegoating": 0.0,
            "engagement_average_liked_posts_sexually_explicit": 0.033,
            "engagement_proportion_liked_posts_sexually_explicit": 0.0,
            "engagement_average_liked_posts_flirtation": 0.2,
            "engagement_proportion_liked_posts_flirtation": 0.0,
            "engagement_average_liked_posts_spam": 0.067,
            "engagement_proportion_liked_posts_spam": 0.0,
            # IME labels
            "engagement_average_liked_posts_intergroup": 0.6,
            "engagement_proportion_liked_posts_intergroup": 1.0,
            "engagement_average_liked_posts_moral": 0.7,
            "engagement_proportion_liked_posts_moral": 1.0,
            "engagement_average_liked_posts_emotion": 0.7,
            "engagement_proportion_liked_posts_emotion": 1.0,
            "engagement_average_liked_posts_other": 0.3,
            "engagement_proportion_liked_posts_other": 0.0,
            # Valence classifier labels
            "engagement_average_liked_posts_valence_clf_score": 0.033,
            "engagement_proportion_liked_posts_valence_clf_score": None,
            "engagement_average_liked_posts_is_valence_positive": 0.333,
            "engagement_proportion_liked_posts_is_valence_positive": 0.333,
            "engagement_average_liked_posts_is_valence_negative": 0.333,
            "engagement_proportion_liked_posts_is_valence_negative": 0.333,
            "engagement_average_liked_posts_is_valence_neutral": 0.333,
            "engagement_proportion_liked_posts_is_valence_neutral": 0.333,
            # LLM classifier labels
            "engagement_average_liked_posts_is_sociopolitical": 0.667,
            "engagement_proportion_liked_posts_is_sociopolitical": 0.667,
            "engagement_average_liked_posts_is_not_sociopolitical": 0.333,
            "engagement_proportion_liked_posts_is_not_sociopolitical": 0.333,
            "engagement_average_liked_posts_is_political_left": 0.333,
            "engagement_proportion_liked_posts_is_political_left": 0.333,
            "engagement_average_liked_posts_is_political_right": 0.333,
            "engagement_proportion_liked_posts_is_political_right": 0.333,
            "engagement_average_liked_posts_is_political_moderate": 0.333,
            "engagement_proportion_liked_posts_is_political_moderate": 0.333,
            "engagement_average_liked_posts_is_political_unclear": 0.0,
            "engagement_proportion_liked_posts_is_political_unclear": 0.0,
        }

        # Act
        result = get_metrics_for_record_type(
            record_type,
            user_to_content_engaged_with,
            labels_for_engaged_content,
            did,
            partition_date,
        )

        # Assert
        # The function returns all labels from LABELS_CONFIG (72 total)
        # We provided data for all 36 labels, so we should have calculated values
        # There are 36 labels and we calculate average + proportion for each,
        # hence we have 72 fields.
        assert len(result) == 72

        # Test against expected results
        for key, value in expected.items():
            if value is None:
                assert result[key] is None, f"Expected {key}=None, got {result[key]}"
                continue
            assert result[key] == value, f"Expected {key}={value}, got {result[key]}"

    def test_handles_empty_post_list(self):
        """Test handling of empty post list.

        This test verifies that:
        1. Empty post list is handled gracefully
        2. Function returns a valid metrics structure
        3. Edge case doesn't cause crashes
        4. Result contains expected structure even with no data
        """
        # Arrange
        record_type = "like"
        user_to_content_engaged_with = {
            "user1": {
                "2024-01-01": {
                    "like": []  # Empty list
                }
            }
        }
        labels_for_engaged_content = {}  # Empty dict
        did = "user1"
        partition_date = "2024-01-01"
        expected_type = dict

        # Act
        result = get_metrics_for_record_type(
            record_type,
            user_to_content_engaged_with,
            labels_for_engaged_content,
            did,
            partition_date,
        )

        # Assert
        # Should still return metrics structure, but with None values
        assert isinstance(result, expected_type)
        assert len(result) == 72  # All 36 labels × 2 metric types
        for key in result.keys():
            assert result[key] is None


class TestGetMetricsForRecordTypes:
    """Tests for get_metrics_for_record_types function."""

    def test_calculates_metrics_for_multiple_record_types(self):
        """Test calculation of metrics for multiple record types.

        This test verifies that:
        1. Metrics are calculated for all specified record types
        2. Each record type gets its own metrics dictionary
        3. The metrics are properly structured and named
        4. All label types are processed for each record type
        """
        # Arrange
        record_types = ["like", "repost"]
        user_to_content_engaged_with = {
            "user1": {
                "2024-01-01": {"like": ["post1", "post2"], "repost": ["post3", "post4"]}
            }
        }
        # Comprehensive test data covering all 36 labels from LABELS_CONFIG
        labels_for_engaged_content = {
            "post1": {
                # Perspective API labels (probability type)
                "prob_toxic": 0.8,
                "prob_constructive": 0.7,
                "prob_severe_toxic": 0.9,
                "prob_identity_attack": 0.2,
                "prob_insult": 0.6,
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
                "prob_spam": 0.1,
                # IME labels (probability type)
                "prob_intergroup": 0.7,
                "prob_moral": 0.8,
                "prob_emotion": 0.6,
                "prob_other": 0.3,
                # Valence classifier labels
                "valence_clf_score": 0.7,
                "is_valence_positive": True,
                "is_valence_negative": False,
                "is_valence_neutral": False,
                # LLM classifier labels
                "is_sociopolitical": True,
                "is_not_sociopolitical": False,
                "is_political_left": True,
                "is_political_right": False,
                "is_political_moderate": False,
                "is_political_unclear": False,
            },
            "post2": {
                # Perspective API labels (probability type)
                "prob_toxic": 0.3,
                "prob_constructive": 0.9,
                "prob_severe_toxic": 0.1,
                "prob_identity_attack": 0.1,
                "prob_insult": 0.2,
                "prob_profanity": 0.0,
                "prob_threat": 0.1,
                "prob_affinity": 0.6,
                "prob_compassion": 0.8,
                "prob_curiosity": 0.9,
                "prob_nuance": 0.8,
                "prob_personal_story": 0.7,
                "prob_reasoning": 0.8,
                "prob_respect": 0.9,
                "prob_alienation": 0.1,
                "prob_fearmongering": 0.0,
                "prob_generalization": 0.2,
                "prob_moral_outrage": 0.1,
                "prob_scapegoating": 0.0,
                "prob_sexually_explicit": 0.0,
                "prob_flirtation": 0.1,
                "prob_spam": 0.0,
                # IME labels (probability type)
                "prob_intergroup": 0.5,
                "prob_moral": 0.7,
                "prob_emotion": 0.8,
                "prob_other": 0.2,
                # Valence classifier labels
                "valence_clf_score": -0.8,
                "is_valence_positive": False,
                "is_valence_negative": True,
                "is_valence_neutral": False,
                # LLM classifier labels
                "is_sociopolitical": False,
                "is_not_sociopolitical": True,
                "is_political_left": False,
                "is_political_right": False,
                "is_political_moderate": True,
                "is_political_unclear": False,
            },
            "post3": {
                # Perspective API labels (probability type)
                "prob_toxic": 0.7,
                "prob_constructive": 0.6,
                "prob_severe_toxic": 0.4,
                "prob_identity_attack": 0.3,
                "prob_insult": 0.5,
                "prob_profanity": 0.2,
                "prob_threat": 0.2,
                "prob_affinity": 0.7,
                "prob_compassion": 0.6,
                "prob_curiosity": 0.8,
                "prob_nuance": 0.7,
                "prob_personal_story": 0.6,
                "prob_reasoning": 0.6,
                "prob_respect": 0.8,
                "prob_alienation": 0.3,
                "prob_fearmongering": 0.2,
                "prob_generalization": 0.5,
                "prob_moral_outrage": 0.4,
                "prob_scapegoating": 0.2,
                "prob_sexually_explicit": 0.1,
                "prob_flirtation": 0.3,
                "prob_spam": 0.1,
                # IME labels (probability type)
                "prob_intergroup": 0.6,
                "prob_moral": 0.6,
                "prob_emotion": 0.7,
                "prob_other": 0.4,
                # Valence classifier labels
                "valence_clf_score": 0.2,
                "is_valence_positive": False,
                "is_valence_negative": False,
                "is_valence_neutral": True,
                # LLM classifier labels
                "is_sociopolitical": True,
                "is_not_sociopolitical": False,
                "is_political_left": False,
                "is_political_right": True,
                "is_political_moderate": False,
                "is_political_unclear": False,
            },
            "post4": {
                # Perspective API labels (probability type)
                "prob_toxic": 0.9,
                "prob_constructive": 0.4,
                "prob_severe_toxic": 0.8,
                "prob_identity_attack": 0.6,
                "prob_insult": 0.8,
                "prob_profanity": 0.5,
                "prob_threat": 0.7,
                "prob_affinity": 0.3,
                "prob_compassion": 0.4,
                "prob_curiosity": 0.5,
                "prob_nuance": 0.4,
                "prob_personal_story": 0.3,
                "prob_reasoning": 0.4,
                "prob_respect": 0.5,
                "prob_alienation": 0.7,
                "prob_fearmongering": 0.6,
                "prob_generalization": 0.8,
                "prob_moral_outrage": 0.7,
                "prob_scapegoating": 0.6,
                "prob_sexually_explicit": 0.4,
                "prob_flirtation": 0.5,
                "prob_spam": 0.3,
                # IME labels (probability type)
                "prob_intergroup": 0.8,
                "prob_moral": 0.4,
                "prob_emotion": 0.3,
                "prob_other": 0.7,
                # Valence classifier labels
                "valence_clf_score": -0.5,
                "is_valence_positive": False,
                "is_valence_negative": True,
                "is_valence_neutral": False,
                # LLM classifier labels
                "is_sociopolitical": True,
                "is_not_sociopolitical": False,
                "is_political_left": True,
                "is_political_right": False,
                "is_political_moderate": False,
                "is_political_unclear": True,
            },
        }
        did = "user1"
        partition_date = "2024-01-01"

        # Act
        result = get_metrics_for_record_types(
            record_types,
            user_to_content_engaged_with,
            labels_for_engaged_content,
            did,
            partition_date,
        )

        # Assert
        # Should have metrics for both record types
        assert "like" in result
        assert "repost" in result

        # Each record type should have 72 fields (36 labels × 2 metric types)
        assert len(result["like"]) == 72
        assert len(result["repost"]) == 72

        # Calculate expected results for "like" record type (post1, post2)
        expected_like = {
            # Perspective API labels (probability type)
            "engagement_average_liked_posts_toxic": 0.55,
            "engagement_proportion_liked_posts_toxic": 0.5,
            "engagement_average_liked_posts_constructive": 0.8,
            "engagement_proportion_liked_posts_constructive": 1.0,
            "engagement_average_liked_posts_severe_toxic": 0.5,
            "engagement_proportion_liked_posts_severe_toxic": 0.5,
            "engagement_average_liked_posts_identity_attack": 0.15,
            "engagement_proportion_liked_posts_identity_attack": 0.0,
            "engagement_average_liked_posts_insult": 0.4,
            "engagement_proportion_liked_posts_insult": 0.5,
            "engagement_average_liked_posts_profanity": 0.05,
            "engagement_proportion_liked_posts_profanity": 0.0,
            "engagement_average_liked_posts_threat": 0.2,
            "engagement_proportion_liked_posts_threat": 0.0,
            "engagement_average_liked_posts_affinity": 0.7,
            "engagement_proportion_liked_posts_affinity": 1.0,
            "engagement_average_liked_posts_compassion": 0.85,
            "engagement_proportion_liked_posts_compassion": 1.0,
            "engagement_average_liked_posts_curiosity": 0.8,
            "engagement_proportion_liked_posts_curiosity": 1.0,
            "engagement_average_liked_posts_nuance": 0.7,
            "engagement_proportion_liked_posts_nuance": 1.0,
            "engagement_average_liked_posts_personal_story": 0.75,
            "engagement_proportion_liked_posts_personal_story": 1.0,
            "engagement_average_liked_posts_reasoning": 0.75,
            "engagement_proportion_liked_posts_reasoning": 1.0,
            "engagement_average_liked_posts_respect": 0.9,
            "engagement_proportion_liked_posts_respect": 1.0,
            "engagement_average_liked_posts_alienation": 0.15,
            "engagement_proportion_liked_posts_alienation": 0.0,
            "engagement_average_liked_posts_fearmongering": 0.05,
            "engagement_proportion_liked_posts_fearmongering": 0.0,
            "engagement_average_liked_posts_generalization": 0.3,
            "engagement_proportion_liked_posts_generalization": 0.0,
            "engagement_average_liked_posts_moral_outrage": 0.2,
            "engagement_proportion_liked_posts_moral_outrage": 0.0,
            "engagement_average_liked_posts_scapegoating": 0.05,
            "engagement_proportion_liked_posts_scapegoating": 0.0,
            "engagement_average_liked_posts_sexually_explicit": 0.0,
            "engagement_proportion_liked_posts_sexually_explicit": 0.0,
            "engagement_average_liked_posts_flirtation": 0.15,
            "engagement_proportion_liked_posts_flirtation": 0.0,
            "engagement_average_liked_posts_spam": 0.05,
            "engagement_proportion_liked_posts_spam": 0.0,
            # IME labels (probability type)
            "engagement_average_liked_posts_intergroup": 0.6,
            "engagement_proportion_liked_posts_intergroup": 1.0,
            "engagement_average_liked_posts_moral": 0.75,
            "engagement_proportion_liked_posts_moral": 1.0,
            "engagement_average_liked_posts_emotion": 0.7,
            "engagement_proportion_liked_posts_emotion": 1.0,
            "engagement_average_liked_posts_other": 0.25,
            "engagement_proportion_liked_posts_other": 0.0,
            # Valence classifier labels
            "engagement_average_liked_posts_valence_clf_score": -0.05,
            "engagement_proportion_liked_posts_valence_clf_score": None,
            "engagement_average_liked_posts_is_valence_positive": 0.5,
            "engagement_proportion_liked_posts_is_valence_positive": 0.5,
            "engagement_average_liked_posts_is_valence_negative": 0.5,
            "engagement_proportion_liked_posts_is_valence_negative": 0.5,
            "engagement_average_liked_posts_is_valence_neutral": 0.0,
            "engagement_proportion_liked_posts_is_valence_neutral": 0.0,
            # LLM classifier labels
            "engagement_average_liked_posts_is_sociopolitical": 0.5,
            "engagement_proportion_liked_posts_is_sociopolitical": 0.5,
            "engagement_average_liked_posts_is_not_sociopolitical": 0.5,
            "engagement_proportion_liked_posts_is_not_sociopolitical": 0.5,
            "engagement_average_liked_posts_is_political_left": 0.5,
            "engagement_proportion_liked_posts_is_political_left": 0.5,
            "engagement_average_liked_posts_is_political_right": 0.0,
            "engagement_proportion_liked_posts_is_political_right": 0.0,
            "engagement_average_liked_posts_is_political_moderate": 0.5,
            "engagement_proportion_liked_posts_is_political_moderate": 0.5,
            "engagement_average_liked_posts_is_political_unclear": 0.0,
            "engagement_proportion_liked_posts_is_political_unclear": 0.0,
        }

        # Calculate expected results for "repost" record type (post3, post4)
        expected_repost = {
            # Perspective API labels (probability type)
            "engagement_average_reposted_posts_toxic": 0.8,
            "engagement_proportion_reposted_posts_toxic": 1.0,
            "engagement_average_reposted_posts_constructive": 0.5,
            "engagement_proportion_reposted_posts_constructive": 0.5,
            "engagement_average_reposted_posts_severe_toxic": 0.6,
            "engagement_proportion_reposted_posts_severe_toxic": 0.5,
            "engagement_average_reposted_posts_identity_attack": 0.45,
            "engagement_proportion_reposted_posts_identity_attack": 0.5,
            "engagement_average_reposted_posts_insult": 0.65,
            "engagement_proportion_reposted_posts_insult": 1.0,
            "engagement_average_reposted_posts_profanity": 0.35,
            "engagement_proportion_reposted_posts_profanity": 0.5,
            "engagement_average_reposted_posts_threat": 0.45,
            "engagement_proportion_reposted_posts_threat": 0.5,
            "engagement_average_reposted_posts_affinity": 0.5,
            "engagement_proportion_reposted_posts_affinity": 0.5,
            "engagement_average_reposted_posts_compassion": 0.5,
            "engagement_proportion_reposted_posts_compassion": 0.5,
            "engagement_average_reposted_posts_curiosity": 0.65,
            "engagement_proportion_reposted_posts_curiosity": 1.0,
            "engagement_average_reposted_posts_nuance": 0.55,
            "engagement_proportion_reposted_posts_nuance": 0.5,
            "engagement_average_reposted_posts_personal_story": 0.45,
            "engagement_proportion_reposted_posts_personal_story": 0.5,
            "engagement_average_reposted_posts_reasoning": 0.5,
            "engagement_proportion_reposted_posts_reasoning": 0.5,
            "engagement_average_reposted_posts_respect": 0.65,
            "engagement_proportion_reposted_posts_respect": 1.0,
            "engagement_average_reposted_posts_alienation": 0.5,
            "engagement_proportion_reposted_posts_alienation": 0.5,
            "engagement_average_reposted_posts_fearmongering": 0.4,
            "engagement_proportion_reposted_posts_fearmongering": 0.5,
            "engagement_average_reposted_posts_generalization": 0.65,
            "engagement_proportion_reposted_posts_generalization": 1.0,
            "engagement_average_reposted_posts_moral_outrage": 0.55,
            "engagement_proportion_reposted_posts_moral_outrage": 0.5,
            "engagement_average_reposted_posts_scapegoating": 0.4,
            "engagement_proportion_reposted_posts_scapegoating": 0.5,
            "engagement_average_reposted_posts_sexually_explicit": 0.25,
            "engagement_proportion_reposted_posts_sexually_explicit": 0.0,
            "engagement_average_reposted_posts_flirtation": 0.4,
            "engagement_proportion_reposted_posts_flirtation": 0.5,
            "engagement_average_reposted_posts_spam": 0.2,
            "engagement_proportion_reposted_posts_spam": 0.0,
            # IME labels (probability type)
            "engagement_average_reposted_posts_intergroup": 0.7,
            "engagement_proportion_reposted_posts_intergroup": 1.0,
            "engagement_average_reposted_posts_moral": 0.5,
            "engagement_proportion_reposted_posts_moral": 0.5,
            "engagement_average_reposted_posts_emotion": 0.5,
            "engagement_proportion_reposted_posts_emotion": 0.5,
            "engagement_average_reposted_posts_other": 0.55,
            "engagement_proportion_reposted_posts_other": 0.5,
            # Valence classifier labels
            "engagement_average_reposted_posts_valence_clf_score": -0.15,
            "engagement_proportion_reposted_posts_valence_clf_score": None,
            "engagement_average_reposted_posts_is_valence_positive": 0.0,
            "engagement_proportion_reposted_posts_is_valence_positive": 0.0,
            "engagement_average_reposted_posts_is_valence_negative": 0.5,
            "engagement_proportion_reposted_posts_is_valence_negative": 0.5,
            "engagement_average_reposted_posts_is_valence_neutral": 0.5,
            "engagement_proportion_reposted_posts_is_valence_neutral": 0.5,
            # LLM classifier labels
            "engagement_average_reposted_posts_is_sociopolitical": 1.0,
            "engagement_proportion_reposted_posts_is_sociopolitical": 1.0,
            "engagement_average_reposted_posts_is_not_sociopolitical": 0.0,
            "engagement_proportion_reposted_posts_is_not_sociopolitical": 0.0,
            "engagement_average_reposted_posts_is_political_left": 0.5,
            "engagement_proportion_reposted_posts_is_political_left": 0.5,
            "engagement_average_reposted_posts_is_political_right": 0.5,
            "engagement_proportion_reposted_posts_is_political_right": 0.5,
            "engagement_average_reposted_posts_is_political_moderate": 0.0,
            "engagement_proportion_reposted_posts_is_political_moderate": 0.0,
            "engagement_average_reposted_posts_is_political_unclear": 0.5,
            "engagement_proportion_reposted_posts_is_political_unclear": 0.5,
        }

        # Test against expected results for "like" record type
        for key, value in expected_like.items():
            if value is None:
                assert (
                    result["like"][key] is None
                ), f"Expected like.{key}=None, got {result['like'][key]}"
                continue
            assert (
                result["like"][key] == value
            ), f"Expected like.{key}={value}, got {result['like'][key]}"

        # Test against expected results for "repost" record type
        for key, value in expected_repost.items():
            if value is None:
                assert (
                    result["repost"][key] is None
                ), f"Expected repost.{key}=None, got {result['repost'][key]}"
                continue
            assert (
                result["repost"][key] == value
            ), f"Expected repost.{key}={value}, got {result['repost'][key]}"


class TestFlattenContentMetricsAcrossRecordTypes:
    """Tests for flatten_content_metrics_across_record_types function."""

    def test_flattens_metrics_correctly(self):
        """Test flattening of metrics across record types.

        This test verifies that:
        1. Metrics from multiple record types are flattened into a single dictionary
        2. All metric keys are preserved with their values
        3. The flattening process doesn't lose any data
        4. The result structure is as expected
        """
        # Arrange
        record_type_to_metrics_map = {
            "like": {
                # Perspective API labels (probability type)
                "engagement_average_liked_posts_toxic": 0.55,
                "engagement_proportion_liked_posts_toxic": 0.5,
                "engagement_average_liked_posts_constructive": 0.8,
                "engagement_proportion_liked_posts_constructive": 1.0,
                "engagement_average_liked_posts_severe_toxic": 0.5,
                "engagement_proportion_liked_posts_severe_toxic": 0.5,
                "engagement_average_liked_posts_identity_attack": 0.15,
                "engagement_proportion_liked_posts_identity_attack": 0.0,
                "engagement_average_liked_posts_insult": 0.4,
                "engagement_proportion_liked_posts_insult": 0.5,
                "engagement_average_liked_posts_profanity": 0.05,
                "engagement_proportion_liked_posts_profanity": 0.0,
                "engagement_average_liked_posts_threat": 0.2,
                "engagement_proportion_liked_posts_threat": 0.0,
                "engagement_average_liked_posts_affinity": 0.7,
                "engagement_proportion_liked_posts_affinity": 1.0,
                "engagement_average_liked_posts_compassion": 0.85,
                "engagement_proportion_liked_posts_compassion": 1.0,
                "engagement_average_liked_posts_curiosity": 0.8,
                "engagement_proportion_liked_posts_curiosity": 1.0,
                "engagement_average_liked_posts_nuance": 0.7,
                "engagement_proportion_liked_posts_nuance": 1.0,
                "engagement_average_liked_posts_personal_story": 0.75,
                "engagement_proportion_liked_posts_personal_story": 1.0,
                "engagement_average_liked_posts_reasoning": 0.75,
                "engagement_proportion_liked_posts_reasoning": 1.0,
                "engagement_average_liked_posts_respect": 0.9,
                "engagement_proportion_liked_posts_respect": 1.0,
                "engagement_average_liked_posts_alienation": 0.15,
                "engagement_proportion_liked_posts_alienation": 0.0,
                "engagement_average_liked_posts_fearmongering": 0.05,
                "engagement_proportion_liked_posts_fearmongering": 0.0,
                "engagement_average_liked_posts_generalization": 0.3,
                "engagement_proportion_liked_posts_generalization": 0.0,
                "engagement_average_liked_posts_moral_outrage": 0.2,
                "engagement_proportion_liked_posts_moral_outrage": 0.0,
                "engagement_average_liked_posts_scapegoating": 0.05,
                "engagement_proportion_liked_posts_scapegoating": 0.0,
                "engagement_average_liked_posts_sexually_explicit": 0.0,
                "engagement_proportion_liked_posts_sexually_explicit": 0.0,
                "engagement_average_liked_posts_flirtation": 0.15,
                "engagement_proportion_liked_posts_flirtation": 0.0,
                "engagement_average_liked_posts_spam": 0.05,
                "engagement_proportion_liked_posts_spam": 0.0,
                # IME labels (probability type)
                "engagement_average_liked_posts_intergroup": 0.6,
                "engagement_proportion_liked_posts_intergroup": 1.0,
                "engagement_average_liked_posts_moral": 0.75,
                "engagement_proportion_liked_posts_moral": 1.0,
                "engagement_average_liked_posts_emotion": 0.7,
                "engagement_proportion_liked_posts_emotion": 1.0,
                "engagement_average_liked_posts_other": 0.25,
                "engagement_proportion_liked_posts_other": 0.0,
                # Valence classifier labels
                "engagement_average_liked_posts_valence_clf_score": -0.05,
                "engagement_proportion_liked_posts_valence_clf_score": None,
                "engagement_average_liked_posts_is_valence_positive": 0.5,
                "engagement_proportion_liked_posts_is_valence_positive": 0.5,
                "engagement_average_liked_posts_is_valence_negative": 0.5,
                "engagement_proportion_liked_posts_is_valence_negative": 0.5,
                "engagement_average_liked_posts_is_valence_neutral": 0.0,
                "engagement_proportion_liked_posts_is_valence_neutral": 0.0,
                # LLM classifier labels
                "engagement_average_liked_posts_is_sociopolitical": 0.5,
                "engagement_proportion_liked_posts_is_sociopolitical": 0.5,
                "engagement_average_liked_posts_is_not_sociopolitical": 0.5,
                "engagement_proportion_liked_posts_is_not_sociopolitical": 0.5,
                "engagement_average_liked_posts_is_political_left": 0.5,
                "engagement_proportion_liked_posts_is_political_left": 0.5,
                "engagement_average_liked_posts_is_political_right": 0.0,
                "engagement_proportion_liked_posts_is_political_right": 0.0,
                "engagement_average_liked_posts_is_political_moderate": 0.5,
                "engagement_proportion_liked_posts_is_political_moderate": 0.5,
                "engagement_average_liked_posts_is_political_unclear": 0.0,
                "engagement_proportion_liked_posts_is_political_unclear": 0.0,
            },
            "repost": {
                # Perspective API labels (probability type)
                "engagement_average_reposted_posts_toxic": 0.8,
                "engagement_proportion_reposted_posts_toxic": 1.0,
                "engagement_average_reposted_posts_constructive": 0.5,
                "engagement_proportion_reposted_posts_constructive": 0.5,
                "engagement_average_reposted_posts_severe_toxic": 0.6,
                "engagement_proportion_reposted_posts_severe_toxic": 0.5,
                "engagement_average_reposted_posts_identity_attack": 0.45,
                "engagement_proportion_reposted_posts_identity_attack": 0.5,
                "engagement_average_reposted_posts_insult": 0.65,
                "engagement_proportion_reposted_posts_insult": 1.0,
                "engagement_average_reposted_posts_profanity": 0.35,
                "engagement_proportion_reposted_posts_profanity": 0.5,
                "engagement_average_reposted_posts_threat": 0.45,
                "engagement_proportion_reposted_posts_threat": 0.5,
                "engagement_average_reposted_posts_affinity": 0.5,
                "engagement_proportion_reposted_posts_affinity": 0.5,
                "engagement_average_reposted_posts_compassion": 0.5,
                "engagement_proportion_reposted_posts_compassion": 0.5,
                "engagement_average_reposted_posts_curiosity": 0.65,
                "engagement_proportion_reposted_posts_curiosity": 1.0,
                "engagement_average_reposted_posts_nuance": 0.55,
                "engagement_proportion_reposted_posts_nuance": 0.5,
                "engagement_average_reposted_posts_personal_story": 0.45,
                "engagement_proportion_reposted_posts_personal_story": 0.5,
                "engagement_average_reposted_posts_reasoning": 0.5,
                "engagement_proportion_reposted_posts_reasoning": 0.5,
                "engagement_average_reposted_posts_respect": 0.65,
                "engagement_proportion_reposted_posts_respect": 1.0,
                "engagement_average_reposted_posts_alienation": 0.5,
                "engagement_proportion_reposted_posts_alienation": 0.5,
                "engagement_average_reposted_posts_fearmongering": 0.4,
                "engagement_proportion_reposted_posts_fearmongering": 0.5,
                "engagement_average_reposted_posts_generalization": 0.65,
                "engagement_proportion_reposted_posts_generalization": 1.0,
                "engagement_average_reposted_posts_moral_outrage": 0.55,
                "engagement_proportion_reposted_posts_moral_outrage": 0.5,
                "engagement_average_reposted_posts_scapegoating": 0.4,
                "engagement_proportion_reposted_posts_scapegoating": 0.5,
                "engagement_average_reposted_posts_sexually_explicit": 0.25,
                "engagement_proportion_reposted_posts_sexually_explicit": 0.0,
                "engagement_average_reposted_posts_flirtation": 0.4,
                "engagement_proportion_reposted_posts_flirtation": 0.5,
                "engagement_average_reposted_posts_spam": 0.2,
                "engagement_proportion_reposted_posts_spam": 0.0,
                # IME labels (probability type)
                "engagement_average_reposted_posts_intergroup": 0.7,
                "engagement_proportion_reposted_posts_intergroup": 1.0,
                "engagement_average_reposted_posts_moral": 0.5,
                "engagement_proportion_reposted_posts_moral": 0.5,
                "engagement_average_reposted_posts_emotion": 0.5,
                "engagement_proportion_reposted_posts_emotion": 0.5,
                "engagement_average_reposted_posts_other": 0.55,
                "engagement_proportion_reposted_posts_other": 0.5,
                # Valence classifier labels
                "engagement_average_reposted_posts_valence_clf_score": -0.15,
                "engagement_proportion_reposted_posts_valence_clf_score": None,
                "engagement_average_reposted_posts_is_valence_positive": 0.0,
                "engagement_proportion_reposted_posts_is_valence_positive": 0.0,
                "engagement_average_reposted_posts_is_valence_negative": 0.5,
                "engagement_proportion_reposted_posts_is_valence_negative": 0.5,
                "engagement_average_reposted_posts_is_valence_neutral": 0.5,
                "engagement_proportion_reposted_posts_is_valence_neutral": 0.5,
                # LLM classifier labels
                "engagement_average_reposted_posts_is_sociopolitical": 1.0,
                "engagement_proportion_reposted_posts_is_sociopolitical": 1.0,
                "engagement_average_reposted_posts_is_not_sociopolitical": 0.0,
                "engagement_proportion_reposted_posts_is_not_sociopolitical": 0.0,
                "engagement_average_reposted_posts_is_political_left": 0.5,
                "engagement_proportion_reposted_posts_is_political_left": 0.5,
                "engagement_average_reposted_posts_is_political_right": 0.5,
                "engagement_proportion_reposted_posts_is_political_right": 0.5,
                "engagement_average_reposted_posts_is_political_moderate": 0.0,
                "engagement_proportion_reposted_posts_is_political_moderate": 0.0,
                "engagement_average_reposted_posts_is_political_unclear": 0.5,
                "engagement_proportion_reposted_posts_is_political_unclear": 0.5,
            },
        }

        # Act
        result = flatten_content_metrics_across_record_types(record_type_to_metrics_map)

        # Assert
        # Should have 144 fields total (72 from like + 72 from repost)
        assert len(result) == 144

        # Test against expected results for "like" record type
        for key, value in record_type_to_metrics_map["like"].items():
            if value is None:
                assert result[key] is None, f"Expected {key}=None, got {result[key]}"
                continue
            assert result[key] == value, f"Expected {key}={value}, got {result[key]}"

        # Test against expected results for "repost" record type
        for key, value in record_type_to_metrics_map["repost"].items():
            if value is None:
                assert result[key] is None, f"Expected {key}=None, got {result[key]}"
                continue
            assert result[key] == value, f"Expected {key}={value}, got {result[key]}"

    def test_handles_empty_metrics_map(self):
        """Test handling of empty metrics map.

        This test verifies that:
        1. Empty input is handled gracefully
        2. Function returns empty dictionary
        3. Edge case doesn't cause crashes
        """
        # Arrange
        record_type_to_metrics_map = {}
        expected = {}

        # Act
        result = flatten_content_metrics_across_record_types(record_type_to_metrics_map)

        # Assert
        assert result == expected
