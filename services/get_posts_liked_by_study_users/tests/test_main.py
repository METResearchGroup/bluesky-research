"""Tests for get_posts_liked_by_study_users main entry."""

from unittest.mock import MagicMock, patch

from services.get_posts_liked_by_study_users.main import get_posts_liked_by_study_users


class TestGetPostsLikedByStudyUsers:
    """Tests for get_posts_liked_by_study_users function."""

    @patch(
        "services.get_posts_liked_by_study_users.main.get_and_export_liked_posts_for_partition_dates"
    )
    def test_forwards_payload_dates_to_export_helper(
        self, mock_export_dates: MagicMock
    ) -> None:
        """Passes start_date, end_date, and exclusions from payload to helper."""
        payload = {
            "start_date": "2024-09-01",
            "end_date": "2024-09-05",
            "exclude_partition_dates": ["2024-09-03"],
        }

        get_posts_liked_by_study_users(payload)

        mock_export_dates.assert_called_once_with(
            start_date="2024-09-01",
            end_date="2024-09-05",
            exclude_partition_dates=["2024-09-03"],
        )
