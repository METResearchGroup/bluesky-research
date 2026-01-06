import pytest
from unittest.mock import patch, MagicMock

from services.backfill.core.validate import (
    validate_is_valid_generic_bluesky_type,
    validate_time_range_record,
    filter_only_valid_bsky_records,
    validate_dids,
)

class TestValidateIsValidGenericBlueskyType:
    """Tests for validate_is_valid_generic_bluesky_type function.

    This class tests validation of generic Bluesky types, verifying:
    - Records with valid types are accepted
    - Records with invalid types are rejected
    """
    def test_valid_type(self):
        """Test record with valid generic Bluesky type."""
        record = {"value": {"$type": "app.bsky.feed.post"}}
        assert validate_is_valid_generic_bluesky_type(record) is True

    def test_invalid_type(self):
        """Test record with invalid (custom) type."""
        record = {"value": {"$type": "sh.tangled.graph.follow"}}
        assert validate_is_valid_generic_bluesky_type(record) is False


class TestValidateTimeRangeRecord:
    """Tests for validate_time_range_record function.

    This class tests validation of record time ranges, verifying:
    - Records within the range are accepted
    - Records outside the range are rejected
    - Default range is used if not specified
    """
    @patch("lib.datetime_utils.convert_bsky_dt_to_pipeline_dt")
    def test_within_range(self, mock_convert):
        """Test record within the default time range."""
        mock_convert.return_value = "2024-09-15-12:00:00"
        record = {"value": {"createdAt": "2024-09-15T12:00:00Z"}}
        assert validate_time_range_record(record) is True

    @patch("lib.datetime_utils.convert_bsky_dt_to_pipeline_dt")
    def test_before_range(self, mock_convert):
        """Test record before the default time range."""
        mock_convert.return_value = "2024-08-31-23:59:59"
        record = {"value": {"createdAt": "2024-08-31T23:59:59Z"}}
        assert validate_time_range_record(record) is False

    @patch("lib.datetime_utils.convert_bsky_dt_to_pipeline_dt")
    def test_after_range(self, mock_convert):
        """Test record after the default time range."""
        mock_convert.return_value = "2024-12-02-00:00:01"
        record = {"value": {"createdAt": "2024-12-02T00:00:01Z"}}
        assert validate_time_range_record(record) is False

    @patch("lib.datetime_utils.convert_bsky_dt_to_pipeline_dt")
    def test_custom_range(self, mock_convert):
        """Test record with custom time range."""
        mock_convert.return_value = "2024-10-01-00:00:00"
        record = {"value": {"createdAt": "2024-10-01T00:00:00Z"}}
        assert validate_time_range_record(
            record, start_timestamp="2024-10-01-00:00:00", end_timestamp="2024-10-31-23:59:59"
        ) is True


class TestFilterOnlyValidBskyRecords:
    """Tests for filter_only_valid_bsky_records function.

    This class tests filtering of records by type, verifying:
    - Only records with types in the allowed list are accepted
    """
    def test_valid_type(self):
        """Test record with allowed type is accepted."""
        record = {"value": {"$type": "app.bsky.feed.post"}}
        types_to_sync = ["app.bsky.feed.post", "app.bsky.feed.like"]
        assert filter_only_valid_bsky_records(record, types_to_sync) is True

    def test_invalid_type(self):
        """Test record with disallowed type is rejected."""
        record = {"value": {"$type": "app.bsky.graph.follow"}}
        types_to_sync = ["app.bsky.feed.post", "app.bsky.feed.like"]
        assert filter_only_valid_bsky_records(record, types_to_sync) is False


class TestValidateDids:
    """Tests for validate_dids function.

    This class tests validation and deduplication of DIDs, verifying:
    - Duplicates are removed
    - Invalid formats are rejected
    - Previously backfilled users are excluded if requested
    - Empty strings are skipped
    - Logging occurs for invalid/duplicate DIDs
    """
    @patch("services.backfill.core.validate.load_latest_backfilled_users")
    def test_deduplication_and_format(self, mock_load):
        """Test deduplication and format validation."""
        mock_load.return_value = []
        dids = [
            "did:plc:abc123",
            "did:plc:abc123",  # duplicate
            "did:plc:def456",
            "invalid:plc:xyz",
            "",
        ]
        valid = validate_dids(dids, exclude_previously_backfilled_users=False)
        assert valid == ["did:plc:abc123", "did:plc:def456"]

    @patch("services.backfill.core.validate.load_latest_backfilled_users")
    def test_exclude_previously_backfilled(self, mock_load):
        """Test exclusion of previously backfilled users."""
        mock_load.return_value = [{"did": "did:plc:abc123"}]
        dids = ["did:plc:abc123", "did:plc:def456"]
        valid = validate_dids(dids, exclude_previously_backfilled_users=True)
        assert valid == ["did:plc:def456"]

    @patch("services.backfill.core.validate.logger")
    @patch("services.backfill.core.validate.load_latest_backfilled_users")
    def test_logging_for_invalid_and_duplicates(self, mock_load, mock_logger):
        """Test that logger is called for invalid and duplicate DIDs."""
        mock_load.return_value = []
        dids = ["did:plc:abc123", "did:plc:abc123", "invalid:plc:xyz", ""]
        validate_dids(dids, exclude_previously_backfilled_users=False)
        assert mock_logger.warning.call_count >= 2
        assert mock_logger.info.call_count >= 1 