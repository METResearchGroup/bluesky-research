import pandas as pd
import pytest

from services.preprocess_raw_data import preprocess


@pytest.fixture
def sample_posts_df() -> pd.DataFrame:
    """Create sample posts DataFrame for preprocessing tests."""
    return pd.DataFrame(
        [
            {
                "uri": "at://post/1",
                "text": "  hello\nworld  ",
                "author": "did:plc:user1",
            },
            {
                "uri": "at://post/2",
                "text": "\nsecond post\n",
                "author": "did:plc:user2",
            },
        ]
    )


class TestPreparePostsForPreprocessing:
    """Tests for prepare_posts_for_preprocessing function."""

    def test_normalizes_text_and_renames_author_column(
        self, sample_posts_df: pd.DataFrame
    ) -> None:
        """Test text cleanup, author rename, and source column initialization."""
        # Arrange
        expected_texts = ["hello world", "second post"]

        # Act
        result = preprocess.prepare_posts_for_preprocessing(sample_posts_df.copy())

        # Assert
        assert result["text"].tolist() == expected_texts
        assert "author_did" in result.columns
        assert "author" not in result.columns
        assert result["author_did"].tolist() == ["did:plc:user1", "did:plc:user2"]
        assert "author_handle" in result.columns
        assert result["author_handle"].tolist() == [None, None]
        assert "source" in result.columns
        assert result["source"].tolist() == [None, None]

    def test_keeps_existing_author_handle_column(self) -> None:
        """Test existing author_handle values are preserved."""
        # Arrange
        input_df = pd.DataFrame(
            [
                {
                    "uri": "at://post/3",
                    "text": " hello ",
                    "author_did": "did:plc:user3",
                    "author_handle": "user3.bsky.social",
                }
            ]
        )

        # Act
        result = preprocess.prepare_posts_for_preprocessing(input_df.copy())

        # Assert
        assert result["text"].tolist() == ["hello"]
        assert result["author_handle"].tolist() == ["user3.bsky.social"]
        assert result["author_did"].tolist() == ["did:plc:user3"]
        assert "source" in result.columns


class TestPostprocessPosts:
    """Tests for postprocess_posts function."""

    def test_converts_dataframe_to_records(self) -> None:
        """Test DataFrame rows are converted to list-of-dicts records."""
        # Arrange
        input_df = pd.DataFrame(
            [
                {"uri": "at://post/1", "text": "hello"},
                {"uri": "at://post/2", "text": "world"},
            ]
        )
        expected = [
            {"uri": "at://post/1", "text": "hello"},
            {"uri": "at://post/2", "text": "world"},
        ]

        # Act
        result = preprocess.postprocess_posts(input_df)

        # Assert
        assert result == expected

    def test_logs_and_reraises_when_conversion_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test conversion errors are logged and re-raised."""
        # Arrange
        class ExplodingDataFrame:
            def to_dict(self, orient: str) -> list[dict]:
                raise RuntimeError("boom")

        logged_messages: list[str] = []

        class DummyLogger:
            def error(self, message: str) -> None:
                logged_messages.append(message)

        monkeypatch.setattr(preprocess, "logger", DummyLogger())

        # Act / Assert
        with pytest.raises(RuntimeError, match="boom"):
            preprocess.postprocess_posts(ExplodingDataFrame())  # type: ignore[arg-type]
        assert len(logged_messages) == 1
        assert "Error postprocessing posts in preprocessing pipeline: boom" in logged_messages[0]


class TestPreprocessLatestPosts:
    """Tests for preprocess_latest_posts function."""

    def test_orchestrates_preprocess_pipeline_and_returns_metadata(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test end-to-end orchestration with dependency call verification."""
        # Arrange
        input_posts = [
            {"batch_id": "batch-1", "text": "hello", "author": "did:plc:user1"},
            {"batch_id": "batch-2", "text": "world", "author": "did:plc:user2"},
        ]
        custom_args = {"new_timestamp_field": "created_at"}

        prepared_df = pd.DataFrame(
            [{"batch_id": "batch-1", "text": "hello"}, {"batch_id": "batch-2", "text": "world"}]
        )
        filtered_df = pd.DataFrame([{"batch_id": "batch-1", "text": "hello"}])
        expected_metadata = {"num_posts": 1}
        expected_postprocessed = [{"batch_id": "batch-1", "text": "hello"}]

        call_args: dict[str, object] = {}

        def mock_prepare_posts_for_preprocessing(latest_posts: pd.DataFrame) -> pd.DataFrame:
            call_args["prepare_input"] = latest_posts.copy()
            return prepared_df

        def mock_filter_posts(
            posts: pd.DataFrame, custom_args: dict
        ) -> tuple[pd.DataFrame, dict]:
            call_args["filter_posts_input"] = posts
            call_args["filter_custom_args"] = custom_args
            return filtered_df, expected_metadata

        def mock_postprocess_posts(posts: pd.DataFrame) -> list[dict]:
            call_args["postprocess_input"] = posts
            return expected_postprocessed

        def mock_write_posts_to_cache(posts: list[dict], batch_ids: list[str]) -> None:
            call_args["write_posts"] = posts
            call_args["write_batch_ids"] = batch_ids

        monkeypatch.setattr(
            preprocess, "prepare_posts_for_preprocessing", mock_prepare_posts_for_preprocessing
        )
        monkeypatch.setattr(preprocess, "filter_posts", mock_filter_posts)
        monkeypatch.setattr(preprocess, "postprocess_posts", mock_postprocess_posts)
        monkeypatch.setattr(preprocess, "write_posts_to_cache", mock_write_posts_to_cache)

        # Act
        result = preprocess.preprocess_latest_posts(posts=input_posts, custom_args=custom_args)

        # Assert
        assert result == expected_metadata
        assert isinstance(call_args["prepare_input"], pd.DataFrame)
        assert call_args["filter_posts_input"] is prepared_df
        assert call_args["filter_custom_args"] == custom_args
        assert call_args["postprocess_input"] is filtered_df
        assert call_args["write_posts"] == expected_postprocessed
        assert call_args["write_batch_ids"] == ["batch-1", "batch-2"]
