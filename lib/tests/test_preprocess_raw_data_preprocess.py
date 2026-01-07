"""Tests for services.preprocess_raw_data.preprocess.py.

This test suite verifies the behavior of `postprocess_posts`, which is responsible
for projecting a DataFrame into the schema expected by `FilteredPreprocessedPostModel`.
"""

import pandas as pd
import pytest

from services.preprocess_raw_data.models import FilteredPreprocessedPostModel
from services.preprocess_raw_data.preprocess import postprocess_posts


class TestPostprocessPosts:
    """Tests for postprocess_posts function."""

    def test_projects_to_model_fields_and_converts_nan_to_none(self):
        """Test that output matches model schema and normalizes missing optionals."""
        # Arrange
        required_fields = [
            name
            for name, field in FilteredPreprocessedPostModel.model_fields.items()
            if field.is_required()
        ]
        required_record = {
            name: (True if name == "passed_filters" else f"{name}_value")
            for name in required_fields
        }
        # Intentionally omit an optional field and include an extra field.
        required_record.pop("author_handle", None)
        df = pd.DataFrame([{**required_record, "extra_field": "should_be_dropped"}])

        expected_fields = set(FilteredPreprocessedPostModel.model_fields.keys())

        # Act
        result = postprocess_posts(df)

        # Assert
        assert isinstance(result, list)
        assert len(result) == 1
        assert set(result[0].keys()) == expected_fields
        assert "extra_field" not in result[0]
        assert result[0]["author_handle"] is None

    def test_raises_value_error_when_required_columns_missing(self):
        """Test that missing required columns raises a clear ValueError."""
        # Arrange
        required_fields = [
            name
            for name, field in FilteredPreprocessedPostModel.model_fields.items()
            if field.is_required()
        ]
        required_record = {
            name: (True if name == "passed_filters" else f"{name}_value")
            for name in required_fields
        }
        missing_required = required_fields[0]
        required_record.pop(missing_required)
        df = pd.DataFrame([required_record])

        # Act / Assert
        with pytest.raises(ValueError, match="Missing required columns"):
            postprocess_posts(df)

