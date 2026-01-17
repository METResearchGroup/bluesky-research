"""Tests for models.py."""

import pytest
from pydantic import ValidationError

from services.backfill.models import (
    BackfillPeriod,
    EnqueueServicePayload,
    IntegrationRunnerConfigurationPayload,
    IntegrationRunnerServicePayload,
    PostScope,
    PostToEnqueueModel,
    PostUsedInFeedModel,
)


class TestEnqueueServicePayload_validate_record_type:
    """Tests for EnqueueServicePayload.validate_record_type validator."""

    def test_accepts_valid_record_type_all_posts(self):
        """Test that 'all_posts' is accepted as valid record type."""
        # Arrange
        record_type = "all_posts"

        # Act
        payload = EnqueueServicePayload(
            record_type=record_type,
            integrations=["ml_inference_perspective_api"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        # Assert
        assert payload.record_type == record_type

    def test_accepts_valid_record_type_feed_posts(self):
        """Test that 'feed_posts' is accepted as valid record type."""
        # Arrange
        record_type = "feed_posts"

        # Act
        payload = EnqueueServicePayload(
            record_type=record_type,
            integrations=["ml_inference_perspective_api"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        # Assert
        assert payload.record_type == record_type

    def test_raises_value_error_for_invalid_record_type(self):
        """Test that invalid record type raises ValueError with helpful message."""
        # Arrange
        record_type = "invalid_type"

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            EnqueueServicePayload(
                record_type=record_type,
                integrations=["ml_inference_perspective_api"],
                start_date="2024-01-01",
                end_date="2024-01-31",
            )

        # Assert
        errors = exc_info.value.errors()
        assert len(errors) > 0
        error = errors[0]
        assert error["type"] == "value_error"
        error_msg_lower = str(error["msg"]).lower()
        assert "invalid record type" in error_msg_lower or "record type" in error_msg_lower

    def test_error_message_includes_valid_options(self):
        """Test that error message includes list of valid PostScope values."""
        # Arrange
        record_type = "invalid_type"

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            EnqueueServicePayload(
                record_type=record_type,
                integrations=["ml_inference_perspective_api"],
                start_date="2024-01-01",
                end_date="2024-01-31",
            )

        # Assert
        error_msg = str(exc_info.value)
        assert "all_posts" in error_msg.lower() or "feed_posts" in error_msg.lower()


class TestEnqueueServicePayload_validate_dates_exist:
    """Tests for EnqueueServicePayload.validate_dates_exist validator."""

    def test_accepts_valid_dates(self):
        """Test that valid date strings are accepted."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-31"

        # Act
        payload = EnqueueServicePayload(
            record_type="all_posts",
            integrations=["ml_inference_perspective_api"],
            start_date=start_date,
            end_date=end_date,
        )

        # Assert
        assert payload.start_date == start_date
        assert payload.end_date == end_date

    def test_raises_value_error_for_empty_start_date(self):
        """Test that empty start_date raises ValueError."""
        # Arrange
        start_date = ""
        end_date = "2024-01-31"

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            EnqueueServicePayload(
                record_type="all_posts",
                integrations=["ml_inference_perspective_api"],
                start_date=start_date,
                end_date=end_date,
            )

        # Assert
        errors = exc_info.value.errors()
        assert len(errors) > 0
        error = errors[0]
        assert error["type"] == "value_error"
        assert "required" in str(error["msg"]).lower()

    def test_raises_value_error_for_empty_end_date(self):
        """Test that empty end_date raises ValueError."""
        # Arrange
        start_date = "2024-01-01"
        end_date = ""

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            EnqueueServicePayload(
                record_type="all_posts",
                integrations=["ml_inference_perspective_api"],
                start_date=start_date,
                end_date=end_date,
            )

        # Assert
        errors = exc_info.value.errors()
        assert len(errors) > 0
        error = errors[0]
        assert error["type"] == "value_error"
        assert "required" in str(error["msg"]).lower()


class TestEnqueueServicePayload_validate_date_range:
    """Tests for EnqueueServicePayload.validate_date_range model validator."""

    def test_accepts_valid_date_range(self):
        """Test that valid date range (start_date < end_date) is accepted."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-31"

        # Act
        payload = EnqueueServicePayload(
            record_type="all_posts",
            integrations=["ml_inference_perspective_api"],
            start_date=start_date,
            end_date=end_date,
        )

        # Assert
        assert payload.start_date == start_date
        assert payload.end_date == end_date

    def test_raises_value_error_when_start_date_equals_end_date(self):
        """Test that start_date == end_date raises ValueError."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-01"

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            EnqueueServicePayload(
                record_type="all_posts",
                integrations=["ml_inference_perspective_api"],
                start_date=start_date,
                end_date=end_date,
            )

        # Assert
        errors = exc_info.value.errors()
        assert len(errors) > 0
        error = errors[0]
        assert error["type"] == "value_error"
        assert "before" in str(error["msg"]).lower()

    def test_raises_value_error_when_start_date_after_end_date(self):
        """Test that start_date > end_date raises ValueError."""
        # Arrange
        start_date = "2024-01-31"
        end_date = "2024-01-01"

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            EnqueueServicePayload(
                record_type="all_posts",
                integrations=["ml_inference_perspective_api"],
                start_date=start_date,
                end_date=end_date,
            )

        # Assert
        errors = exc_info.value.errors()
        assert len(errors) > 0
        error = errors[0]
        assert error["type"] == "value_error"
        assert "before" in str(error["msg"]).lower()

    def test_error_message_includes_dates(self):
        """Test that error message includes the actual dates."""
        # Arrange
        start_date = "2024-01-31"
        end_date = "2024-01-01"

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            EnqueueServicePayload(
                record_type="all_posts",
                integrations=["ml_inference_perspective_api"],
                start_date=start_date,
                end_date=end_date,
            )

        # Assert
        error_msg = str(exc_info.value)
        assert start_date in error_msg
        assert end_date in error_msg


class TestEnqueueServicePayload_integrations:
    """Tests for EnqueueServicePayload.integrations field."""

    def test_accepts_non_empty_integrations_list(self):
        """Test that non-empty integrations list is accepted."""
        # Arrange
        integrations = ["ml_inference_perspective_api", "ml_inference_sociopolitical"]

        # Act
        payload = EnqueueServicePayload(
            record_type="all_posts",
            integrations=integrations,
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        # Assert
        assert payload.integrations == integrations

    def test_accepts_single_integration(self):
        """Test that single integration in list is accepted."""
        # Arrange
        integrations = ["ml_inference_perspective_api"]

        # Act
        payload = EnqueueServicePayload(
            record_type="all_posts",
            integrations=integrations,
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        # Assert
        assert payload.integrations == integrations

    def test_raises_validation_error_for_empty_integrations_list(self):
        """Test that empty integrations list raises ValidationError."""
        # Arrange
        integrations = []

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            EnqueueServicePayload(
                record_type="all_posts",
                integrations=integrations,
                start_date="2024-01-01",
                end_date="2024-01-31",
            )

        # Assert
        errors = exc_info.value.errors()
        assert len(errors) > 0
        error = errors[0]
        assert error["type"] in ["value_error", "string_too_short", "too_short"]
