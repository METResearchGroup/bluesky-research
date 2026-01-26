"""Tests for models.py."""

import pytest
from pydantic import ValidationError

from services.backfill.models import (
    BackfillPeriod,
    EnqueueServicePayload,
    IntegrationRunnerConfigurationPayload,
)


class TestEnqueueServicePayload_validate_record_type:
    """Tests for EnqueueServicePayload.validate_record_type validator."""

    def test_accepts_valid_record_type_all_posts(self):
        """Test that 'posts' is accepted as valid record type."""
        # Arrange
        record_type = "posts"

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
        """Test that 'posts_used_in_feeds' is accepted as valid record type."""
        # Arrange
        record_type = "posts_used_in_feeds"

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
        assert "posts" in error_msg.lower() or "posts_used_in_feeds" in error_msg.lower()


class TestEnqueueServicePayload_validate_dates_exist:
    """Tests for EnqueueServicePayload.validate_dates_exist validator."""

    def test_accepts_valid_dates(self):
        """Test that valid date strings are accepted."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-31"

        # Act
        payload = EnqueueServicePayload(
            record_type="posts",
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
                record_type="posts",
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
                record_type="posts",
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
            record_type="posts",
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
                record_type="posts",
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
                record_type="posts",
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
                record_type="posts",
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
            record_type="posts",
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
            record_type="posts",
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
                record_type="posts",
                integrations=integrations,
                start_date="2024-01-01",
                end_date="2024-01-31",
            )


class TestEnqueueServicePayload_validate_sampling_config:
    """Tests for EnqueueServicePayload.validate_sampling_config model validator."""

    def test_accepts_defaults_when_sampling_disabled(self):
        """Test that sampling defaults are accepted when not enabled."""
        # Arrange & Act
        payload = EnqueueServicePayload(
            record_type="posts",
            integrations=["ml_inference_perspective_api"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        # Assert
        assert payload.sample_records is False
        assert payload.sample_proportion is None

    def test_raises_when_sample_records_true_and_no_proportion(self):
        """Test that enabling sampling requires a sample_proportion."""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            EnqueueServicePayload(
                record_type="posts",
                integrations=["ml_inference_perspective_api"],
                start_date="2024-01-01",
                end_date="2024-01-31",
                sample_records=True,
                sample_proportion=None,
            )

        # Assert
        assert "sample_proportion is required" in str(exc_info.value)

    @pytest.mark.parametrize("proportion", [-0.1, 1.1])
    def test_raises_when_proportion_out_of_range(self, proportion: float):
        """Test that sample_proportion must be within [0, 1]."""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            EnqueueServicePayload(
                record_type="posts",
                integrations=["ml_inference_perspective_api"],
                start_date="2024-01-01",
                end_date="2024-01-31",
                sample_records=True,
                sample_proportion=proportion,
            )

        # Assert
        assert "between 0 and 1" in str(exc_info.value)

    @pytest.mark.parametrize("proportion", [0.0, 1.0])
    def test_accepts_boundary_values(self, proportion: float):
        """Test that 0.0 and 1.0 are accepted when sampling is enabled."""
        # Arrange & Act
        payload = EnqueueServicePayload(
            record_type="posts",
            integrations=["ml_inference_perspective_api"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            sample_records=True,
            sample_proportion=proportion,
        )

        # Assert
        assert payload.sample_records is True
        assert payload.sample_proportion == proportion

    def test_raises_when_proportion_provided_but_sampling_disabled(self):
        """Test that sample_proportion cannot be set unless sample_records is enabled."""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            EnqueueServicePayload(
                record_type="posts",
                integrations=["ml_inference_perspective_api"],
                start_date="2024-01-01",
                end_date="2024-01-31",
                sample_records=False,
                sample_proportion=0.5,
            )

        # Assert
        assert "only be set when sample_records is enabled" in str(exc_info.value)


class TestIntegrationRunnerConfigurationPayload:
    """Tests for IntegrationRunnerConfigurationPayload model."""

    def test_max_records_per_run_defaults_to_none(self):
        """Test that max_records_per_run defaults to None when not provided."""
        # Arrange & Act
        payload = IntegrationRunnerConfigurationPayload(
            integration_name="ml_inference_intergroup",
            backfill_period=BackfillPeriod.DAYS,
            backfill_duration=None,
        )

        # Assert
        assert payload.max_records_per_run is None

    def test_max_records_per_run_accepts_positive_integer(self):
        """Test that max_records_per_run accepts positive integer values."""
        # Arrange
        expected = 123

        # Act
        payload = IntegrationRunnerConfigurationPayload(
            integration_name="ml_inference_intergroup",
            backfill_period=BackfillPeriod.DAYS,
            backfill_duration=None,
            max_records_per_run=expected,
        )

        # Assert
        assert payload.max_records_per_run == expected

    def test_max_records_per_run_accepts_zero(self):
        """Test that max_records_per_run accepts 0 as a valid value."""
        # Arrange
        expected = 0

        # Act
        payload = IntegrationRunnerConfigurationPayload(
            integration_name="ml_inference_intergroup",
            backfill_period=BackfillPeriod.DAYS,
            backfill_duration=None,
            max_records_per_run=expected,
        )

        # Assert
        assert payload.max_records_per_run == expected

    def test_max_records_per_run_negative_raises(self):
        """Test that max_records_per_run raises ValidationError for negative values."""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            IntegrationRunnerConfigurationPayload(
                integration_name="ml_inference_intergroup",
                backfill_period=BackfillPeriod.DAYS,
                backfill_duration=None,
                max_records_per_run=-1,
            )

        # Assert
        errors = exc_info.value.errors()
        assert len(errors) > 0
        error = errors[0]
        assert error["type"] == "greater_than_equal"
        assert error["loc"] == ("max_records_per_run",)

    def test_max_records_per_run_serialization(self):
        """Test that max_records_per_run is included in model serialization."""
        # Arrange
        payload = IntegrationRunnerConfigurationPayload(
            integration_name="ml_inference_intergroup",
            backfill_period=BackfillPeriod.DAYS,
            backfill_duration=7,
            max_records_per_run=10,
        )

        # Act
        result = payload.model_dump()

        # Assert
        assert result["max_records_per_run"] == 10
