import pandas as pd
import pytest

from services.preprocess_raw_data.filters import filter_posts


@pytest.fixture
def sample_posts() -> pd.DataFrame:
    """Build representative input posts for filtering."""
    return pd.DataFrame(
        [
            {
                "uri": "at://post/1",
                "text": "hello world",
                "labels": "safe",
                "author_did": "did:plc:user1",
                "author_handle": "user1.bsky.social",
                "created_at": "2025-01-01T00:00:00Z",
            },
            {
                "uri": "at://post/2",
                "text": None,
                "labels": "safe",
                "author_did": "did:plc:user2",
                "author_handle": "user2.bsky.social",
                "created_at": "2025-01-02T00:00:00Z",
            },
            {
                "uri": "at://post/3",
                "text": "hola mundo",
                "labels": "safe",
                "author_did": "did:plc:user3",
                "author_handle": "user3.bsky.social",
                "created_at": "2025-01-03T00:00:00Z",
            },
            {
                "uri": "at://post/4",
                "text": "nsfw text",
                "labels": "nsfw",
                "author_did": "did:plc:user4",
                "author_handle": "user4.bsky.social",
                "created_at": "2025-01-04T00:00:00Z",
            },
            {
                "uri": "at://post/5",
                "text": "spam text",
                "labels": "safe",
                "author_did": "did:plc:user5",
                "author_handle": "user5.bsky.social",
                "created_at": "2025-01-05T00:00:00Z",
            },
            {
                "uri": "at://post/6",
                "text": "author flagged",
                "labels": "safe",
                "author_did": "did:plc:user6",
                "author_handle": "user6.bsky.social",
                "created_at": "2025-01-06T00:00:00Z",
            },
        ]
    )


class TestFilterPosts:
    """Tests for filter_posts function."""

    def test_filters_posts_and_preserves_contract_default_timestamp(
        self, monkeypatch: pytest.MonkeyPatch, sample_posts: pd.DataFrame
    ) -> None:
        """Test mixed filtering behavior and default timestamp path."""
        # Arrange
        expected_timestamp = "2026-05-08T10:00:00Z"

        def mock_filter_text_is_english(texts: pd.Series) -> pd.Series:
            return texts.str.contains("hola").map(lambda is_hola: not is_hola)

        def mock_filter_post_content_nsfw(texts: pd.Series, labels: pd.Series) -> pd.Series:
            return labels.eq("nsfw")

        def mock_filter_post_author_nsfw(
            author_dids: pd.Series, author_handles: pd.Series
        ) -> pd.Series:
            _ = author_handles
            return author_dids.eq("did:plc:user6")

        def mock_filter_posts_have_spam(texts: pd.Series) -> pd.Series:
            return texts.str.contains("spam")

        monkeypatch.setattr(
            "services.preprocess_raw_data.filters.filter_text_is_english",
            mock_filter_text_is_english,
        )
        monkeypatch.setattr(
            "services.preprocess_raw_data.filters.filter_post_content_nsfw",
            mock_filter_post_content_nsfw,
        )
        monkeypatch.setattr(
            "services.preprocess_raw_data.filters.filter_post_author_nsfw",
            mock_filter_post_author_nsfw,
        )
        monkeypatch.setattr(
            "services.preprocess_raw_data.filters.filter_posts_have_spam",
            mock_filter_posts_have_spam,
        )
        monkeypatch.setattr(
            "services.preprocess_raw_data.filters.generate_current_datetime_str",
            lambda: expected_timestamp,
        )

        # Act
        result_df, result_metadata = filter_posts(posts=sample_posts, custom_args={})

        # Assert
        assert len(result_df) == 4
        assert result_df["uri"].tolist() == [
            "at://post/1",
            "at://post/4",
            "at://post/5",
            "at://post/6",
        ]
        assert result_df["passed_filters"].tolist() == [True, False, False, False]
        assert result_df["filtered_by_func"].tolist() == [
            None,
            "post_is_nsfw",
            "is_spam",
            "author_is_nsfw",
        ]
        assert result_df["preprocessing_timestamp"].tolist() == [expected_timestamp] * 4
        assert result_df["filtered_at"].tolist() == [expected_timestamp] * 4
        assert "post_is_nsfw" not in result_df.columns
        assert "author_is_nsfw" not in result_df.columns
        assert "is_spam" not in result_df.columns

        expected_metadata = {
            "num_posts": 4,
            "num_records_after_filtering": {
                "posts": {
                    "passed": 1,
                    "failed_total": 3,
                    "failed_breakdown": {
                        "is_english": 4,
                        "post_is_nsfw": 1,
                        "author_is_nsfw": 1,
                        "is_spam": 1,
                    },
                }
            },
        }
        assert result_metadata == expected_metadata

    def test_uses_custom_timestamp_field_when_provided(
        self, monkeypatch: pytest.MonkeyPatch, sample_posts: pd.DataFrame
    ) -> None:
        """Test custom_args timestamp override uses source field values."""
        # Arrange
        custom_args = {"new_timestamp_field": "created_at"}

        monkeypatch.setattr(
            "services.preprocess_raw_data.filters.filter_text_is_english",
            lambda texts: pd.Series([True] * len(texts), index=texts.index),
        )
        monkeypatch.setattr(
            "services.preprocess_raw_data.filters.filter_post_content_nsfw",
            lambda texts, labels: pd.Series([False] * len(texts), index=texts.index),
        )
        monkeypatch.setattr(
            "services.preprocess_raw_data.filters.filter_post_author_nsfw",
            lambda author_dids, author_handles: pd.Series(
                [False] * len(author_dids), index=author_dids.index
            ),
        )
        monkeypatch.setattr(
            "services.preprocess_raw_data.filters.filter_posts_have_spam",
            lambda texts: pd.Series([False] * len(texts), index=texts.index),
        )

        # Act
        result_df, result_metadata = filter_posts(posts=sample_posts, custom_args=custom_args)

        # Assert
        expected_created_at = [
            "2025-01-01T00:00:00Z",
            "2025-01-03T00:00:00Z",
            "2025-01-04T00:00:00Z",
            "2025-01-05T00:00:00Z",
            "2025-01-06T00:00:00Z",
        ]
        assert result_df["preprocessing_timestamp"].tolist() == expected_created_at
        assert result_df["filtered_at"].tolist() == expected_created_at
        assert result_metadata["num_posts"] == 5
        assert result_metadata["num_records_after_filtering"]["posts"]["passed"] == 5
        assert result_metadata["num_records_after_filtering"]["posts"]["failed_total"] == 0
        assert (
            result_metadata["num_records_after_filtering"]["posts"]["failed_breakdown"][
                "is_english"
            ]
            == 5
        )

    def test_handles_empty_dataframe_after_english_filter(
        self, monkeypatch: pytest.MonkeyPatch, sample_posts: pd.DataFrame
    ) -> None:
        """Test edge case where no posts remain after english filtering."""
        # Arrange
        call_tracker: dict[str, int] = {
            "filter_text_is_english": 0,
            "filter_post_content_nsfw": 0,
            "filter_post_author_nsfw": 0,
            "filter_posts_have_spam": 0,
            "generate_current_datetime_str": 0,
        }

        def mock_filter_text_is_english(texts: pd.Series) -> pd.Series:
            call_tracker["filter_text_is_english"] += 1
            return pd.Series([False] * len(texts), index=texts.index)

        def mock_filter_post_content_nsfw(texts: pd.Series, labels: pd.Series) -> pd.Series:
            call_tracker["filter_post_content_nsfw"] += 1
            return pd.Series([], dtype=bool, index=texts.index)

        def mock_filter_post_author_nsfw(
            author_dids: pd.Series, author_handles: pd.Series
        ) -> pd.Series:
            call_tracker["filter_post_author_nsfw"] += 1
            return pd.Series([], dtype=bool, index=author_dids.index)

        def mock_filter_posts_have_spam(texts: pd.Series) -> pd.Series:
            call_tracker["filter_posts_have_spam"] += 1
            return pd.Series([], dtype=bool, index=texts.index)

        def mock_generate_current_datetime_str() -> str:
            call_tracker["generate_current_datetime_str"] += 1
            return "2026-05-08T10:00:00Z"

        monkeypatch.setattr(
            "services.preprocess_raw_data.filters.filter_text_is_english",
            mock_filter_text_is_english,
        )
        monkeypatch.setattr(
            "services.preprocess_raw_data.filters.filter_post_content_nsfw",
            mock_filter_post_content_nsfw,
        )
        monkeypatch.setattr(
            "services.preprocess_raw_data.filters.filter_post_author_nsfw",
            mock_filter_post_author_nsfw,
        )
        monkeypatch.setattr(
            "services.preprocess_raw_data.filters.filter_posts_have_spam",
            mock_filter_posts_have_spam,
        )
        monkeypatch.setattr(
            "services.preprocess_raw_data.filters.generate_current_datetime_str",
            mock_generate_current_datetime_str,
        )

        # Act
        result_df, result_metadata = filter_posts(posts=sample_posts, custom_args={})

        # Assert
        assert result_df.empty
        assert result_df.columns.tolist() == [
            "uri",
            "text",
            "labels",
            "author_did",
            "author_handle",
            "created_at",
            "is_english",
            "passed_filters",
            "filtered_by_func",
            "preprocessing_timestamp",
            "filtered_at",
        ]
        assert result_metadata == {
            "num_posts": 0,
            "num_records_after_filtering": {
                "posts": {
                    "passed": 0,
                    "failed_total": 0,
                    "failed_breakdown": {
                        "is_english": 0,
                        "post_is_nsfw": 0,
                        "author_is_nsfw": 0,
                        "is_spam": 0,
                    },
                }
            },
        }
        assert call_tracker == {
            "filter_text_is_english": 1,
            "filter_post_content_nsfw": 1,
            "filter_post_author_nsfw": 1,
            "filter_posts_have_spam": 1,
            "generate_current_datetime_str": 1,
        }
