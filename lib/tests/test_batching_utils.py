"""Unit tests for batching utilities."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from lib.batching_utils import create_batches, update_batching_progress


class TestCreateBatches:
    """Tests for create_batches function."""

    def test_splits_list_into_equal_sized_batches_and_remainder(self):
        """Test that list is split into batches of size N with a final remainder batch."""
        # Arrange
        batch_list = list(range(10))
        batch_size = 3
        expected = [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

        # Act
        result = create_batches(batch_list=batch_list, batch_size=batch_size)

        # Assert
        assert result == expected

    def test_returns_single_batch_when_batch_size_exceeds_list_length(self):
        """Test that a single batch is returned when batch_size > len(batch_list)."""
        # Arrange
        batch_list = [1, 2, 3]
        batch_size = 10
        expected = [[1, 2, 3]]

        # Act
        result = create_batches(batch_list=batch_list, batch_size=batch_size)

        # Assert
        assert result == expected

    def test_returns_empty_list_when_input_is_empty(self):
        """Test that an empty input list returns an empty list of batches."""
        # Arrange
        batch_list: list[int] = []
        batch_size = 5
        expected: list[list[int]] = []

        # Act
        result = create_batches(batch_list=batch_list, batch_size=batch_size)

        # Assert
        assert result == expected

    def test_raises_value_error_when_batch_size_is_zero(self):
        """Test that batch_size=0 raises ValueError (invalid range step)."""
        # Arrange
        batch_list = [1, 2, 3]

        # Act / Assert
        with pytest.raises(ValueError, match="range\\(\\) arg 3 must not be zero"):
            create_batches(batch_list=batch_list, batch_size=0)


class TestUpdateBatchingProgress:
    """Tests for update_batching_progress function."""

    @pytest.mark.parametrize("total_batches", [0, -1, -10])
    def test_returns_without_logging_when_total_batches_is_non_positive(self, total_batches):
        """Test that total_batches <= 0 returns early and does not log."""
        # Arrange
        logger = Mock()

        # Act
        update_batching_progress(
            batch_index=0,
            batch_interval=5,
            total_batches=total_batches,
            logger=logger,
        )

        # Assert
        logger.info.assert_not_called()

    @pytest.mark.parametrize(
        "batch_index,batch_interval,total_batches,expected_percentage",
        [
            (0, 5, 10, 0),
            (5, 5, 10, 50),
            (10, 5, 10, 100),
        ],
    )
    def test_logs_progress_when_batch_index_is_multiple_of_interval(
        self,
        batch_index,
        batch_interval,
        total_batches,
        expected_percentage,
    ):
        """Test that an info log is emitted at the requested interval with correct percent."""
        # Arrange
        logger = Mock()
        expected_message = (
            f"[Completion percentage: {expected_percentage}%] Processing batch {batch_index}/{total_batches}..."
        )

        # Act
        update_batching_progress(
            batch_index=batch_index,
            batch_interval=batch_interval,
            total_batches=total_batches,
            logger=logger,
        )

        # Assert
        logger.info.assert_called_once_with(expected_message)

    def test_does_not_log_when_batch_index_is_not_multiple_of_interval(self):
        """Test that non-interval batch indices do not emit logs."""
        # Arrange
        logger = Mock()

        # Act
        update_batching_progress(
            batch_index=1,
            batch_interval=5,
            total_batches=10,
            logger=logger,
        )

        # Assert
        logger.info.assert_not_called()

    def test_raises_zero_division_error_when_batch_interval_is_zero(self):
        """Test that batch_interval=0 raises ZeroDivisionError (modulo by zero)."""
        # Arrange
        logger = Mock()

        # Act / Assert
        with pytest.raises(ZeroDivisionError):
            update_batching_progress(
                batch_index=1,
                batch_interval=0,
                total_batches=10,
                logger=logger,
            )
