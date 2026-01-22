"""Tests for classifier.py.

This test suite verifies the functionality of IntergroupClassifier:
- classify_batch: Main batch classification method
- _merge_llm_responses_with_batch: Merging LLM responses with batch posts
- _generate_failed_labels: Generating failed labels
- _generate_batch_prompts: Generating batch prompts
"""

import pytest
from unittest.mock import Mock, patch
from pydantic import ValidationError

from ml_tooling.llm.exceptions import (
    LLMException,
    LLMAuthError,
    LLMInvalidRequestError,
    LLMPermissionDeniedError,
    LLMUnrecoverableError,
)
from services.ml_inference.intergroup.classifier import IntergroupClassifier
from services.ml_inference.intergroup.constants import DEFAULT_LLM_MODEL_NAME
from services.ml_inference.intergroup.models import (
    IntergroupLabelModel,
    LabelChoiceModel,
)
from services.ml_inference.models import PostToLabelModel

# Test timestamp constant for IntergroupLabelModel instances
TEST_LABEL_TIMESTAMP = "2024-01-01-12:00:00"


class TestIntergroupClassifierClassifyBatch:
    """Tests for IntergroupClassifier.classify_batch method."""

    @pytest.fixture
    def mock_llm_service(self):
        """Mock LLMService."""
        with patch(
            "ml_tooling.llm.llm_service.get_llm_service"
        ) as mock_get_service:
            mock_service = Mock()
            mock_get_service.return_value = mock_service
            yield mock_service

    @pytest.fixture
    def mock_generate_prompts(self):
        """Mock generate_batch_prompts function."""
        with patch(
            "services.ml_inference.intergroup.classifier.generate_batch_prompts"
        ) as mock:
            yield mock

    def test_successful_batch_classification(self, mock_llm_service, mock_generate_prompts):
        """Test successful batch classification.

        Expected behavior:
            - Should generate prompts, call LLM service, merge responses, and return labels
            - Should return IntergroupLabelModel instances
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri="uri_1",
                text="test post 1",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            ),
            PostToLabelModel(
                uri="uri_2",
                text="test post 2",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=2,
                batch_metadata="{}",
            ),
        ]
        mock_generate_prompts.return_value = ["prompt_1", "prompt_2"]

        llm_responses = [
            LabelChoiceModel(label=1),
            LabelChoiceModel(label=0),
        ]
        mock_llm_service.structured_batch_completion.return_value = llm_responses

        classifier = IntergroupClassifier()

        # Act
        result = classifier.classify_batch(batch=posts)

        # Assert
        assert len(result) == 2
        assert all(isinstance(label, IntergroupLabelModel) for label in result)
        assert result[0].uri == "uri_1"
        assert result[0].label == 1
        assert result[0].was_successfully_labeled is True
        assert result[1].uri == "uri_2"
        assert result[1].label == 0
        assert result[1].was_successfully_labeled is True

    def test_structured_batch_completion_called_with_label_choice_model(
        self, mock_llm_service, mock_generate_prompts
    ):
        """Test that structured_batch_completion is called with LabelChoiceModel as response_model.

        Expected behavior:
            - Should use LabelChoiceModel (not IntergroupLabelModel) as response_model
        """
        # Arrange
        from services.ml_inference.intergroup.classifier import LabelChoiceModel

        posts = [
            PostToLabelModel(
                uri="uri_1",
                text="test post",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
        ]
        mock_generate_prompts.return_value = ["prompt_1"]
        mock_llm_service.structured_batch_completion.return_value = [
            LabelChoiceModel(label=1)
        ]

        classifier = IntergroupClassifier()

        # Act
        classifier.classify_batch(batch=posts)

        # Assert
        mock_llm_service.structured_batch_completion.assert_called_once()
        call_kwargs = mock_llm_service.structured_batch_completion.call_args[1]
        assert call_kwargs["response_model"] == LabelChoiceModel

    def test_structured_batch_completion_called_with_correct_parameters(
        self, mock_llm_service, mock_generate_prompts
    ):
        """Test that structured_batch_completion is called with correct parameters.

        Expected behavior:
            - Should pass prompts, model, and role parameters correctly
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri="uri_1",
                text="test post",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
        ]
        mock_generate_prompts.return_value = ["generated_prompt"]
        mock_llm_service.structured_batch_completion.return_value = [
            LabelChoiceModel(label=1)
        ]

        classifier = IntergroupClassifier()

        # Act
        classifier.classify_batch(batch=posts, llm_model_name="custom-model")

        # Assert
        mock_llm_service.structured_batch_completion.assert_called_once()
        call_kwargs = mock_llm_service.structured_batch_completion.call_args[1]
        assert call_kwargs["prompts"] == ["generated_prompt"]
        assert call_kwargs["model"] == "custom-model"
        assert call_kwargs["role"] == "user"

    def test_prompts_generated_correctly(self, mock_llm_service, mock_generate_prompts):
        """Test that prompts are generated correctly via _generate_batch_prompts.

        Expected behavior:
            - Should call _generate_batch_prompts before calling LLM service
        """
        # Arrange
        from services.ml_inference.intergroup.prompts import INTERGROUP_PROMPT

        posts = [
            PostToLabelModel(
                uri="uri_1",
                text="test post",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
        ]
        mock_generate_prompts.return_value = ["prompt"]
        mock_llm_service.structured_batch_completion.return_value = [
            LabelChoiceModel(label=1)
        ]

        classifier = IntergroupClassifier()

        # Act
        classifier.classify_batch(batch=posts)

        # Assert
        mock_generate_prompts.assert_called_once()
        call_kwargs = mock_generate_prompts.call_args[1]
        assert call_kwargs["batch"] == posts
        assert call_kwargs["prompt_template"] == INTERGROUP_PROMPT

    def test_merge_llm_responses_called(self, mock_llm_service, mock_generate_prompts):
        """Test that _merge_llm_responses_with_batch is called with batch and llm_responses.

        Expected behavior:
            - Should call merge method after getting LLM responses
            - Should pass both batch and llm_responses
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri="uri_1",
                text="test post",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
        ]
        mock_generate_prompts.return_value = ["prompt"]
        llm_responses = [LabelChoiceModel(label=1)]
        mock_llm_service.structured_batch_completion.return_value = llm_responses

        classifier = IntergroupClassifier()

        with patch.object(
            classifier, "_merge_llm_responses_with_batch"
        ) as mock_merge:
            mock_merge.return_value = [
                IntergroupLabelModel(
                    uri="uri_1",
                    text="test post",
                    preprocessing_timestamp="2024-01-01-12:00:00",
                    was_successfully_labeled=True,
                    label=1,
                    label_timestamp=TEST_LABEL_TIMESTAMP,
                )
            ]

            # Act
            classifier.classify_batch(batch=posts)

            # Assert
            mock_merge.assert_called_once_with(
                batch=posts, llm_responses=llm_responses
            )

    def test_intergroup_label_model_instances_returned(
        self, mock_llm_service, mock_generate_prompts
    ):
        """Test that IntergroupLabelModel instances are returned.

        Expected behavior:
            - Should return list of IntergroupLabelModel instances
            - All instances should have correct structure
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri="uri_1",
                text="test post",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
        ]
        mock_generate_prompts.return_value = ["prompt"]
        mock_llm_service.structured_batch_completion.return_value = [
            LabelChoiceModel(label=1)
        ]

        classifier = IntergroupClassifier()

        # Act
        result = classifier.classify_batch(batch=posts)

        # Assert
        assert all(isinstance(label, IntergroupLabelModel) for label in result)
        assert len(result) == 1
        assert result[0].was_successfully_labeled is True

    def test_llm_auth_error_propagates(self, mock_llm_service, mock_generate_prompts):
        """Test with LLMAuthError (should propagate, not caught).

        Expected behavior:
            - Should propagate LLMAuthError without catching it
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri="uri_1",
                text="test post",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
        ]
        mock_generate_prompts.return_value = ["prompt"]
        mock_llm_service.structured_batch_completion.side_effect = LLMAuthError(
            "Auth failed"
        )

        classifier = IntergroupClassifier()

        # Act & Assert
        with pytest.raises(LLMAuthError):
            classifier.classify_batch(batch=posts)

    def test_llm_invalid_request_error_propagates(
        self, mock_llm_service, mock_generate_prompts
    ):
        """Test with LLMInvalidRequestError (should propagate, not caught).

        Expected behavior:
            - Should propagate LLMInvalidRequestError without catching it
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri="uri_1",
                text="test post",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
        ]
        mock_generate_prompts.return_value = ["prompt"]
        mock_llm_service.structured_batch_completion.side_effect = (
            LLMInvalidRequestError("Invalid request")
        )

        classifier = IntergroupClassifier()

        # Act & Assert
        with pytest.raises(LLMInvalidRequestError):
            classifier.classify_batch(batch=posts)

    def test_llm_permission_denied_error_propagates(
        self, mock_llm_service, mock_generate_prompts
    ):
        """Test with LLMPermissionDeniedError (should propagate, not caught).

        Expected behavior:
            - Should propagate LLMPermissionDeniedError without catching it
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri="uri_1",
                text="test post",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
        ]
        mock_generate_prompts.return_value = ["prompt"]
        mock_llm_service.structured_batch_completion.side_effect = (
            LLMPermissionDeniedError("Permission denied")
        )

        classifier = IntergroupClassifier()

        # Act & Assert
        with pytest.raises(LLMPermissionDeniedError):
            classifier.classify_batch(batch=posts)

    def test_llm_unrecoverable_error_propagates(
        self, mock_llm_service, mock_generate_prompts
    ):
        """Test with LLMUnrecoverableError (should propagate, not caught).

        Expected behavior:
            - Should propagate LLMUnrecoverableError without catching it
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri="uri_1",
                text="test post",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
        ]
        mock_generate_prompts.return_value = ["prompt"]
        mock_llm_service.structured_batch_completion.side_effect = (
            LLMUnrecoverableError("Unrecoverable error")
        )

        classifier = IntergroupClassifier()

        # Act & Assert
        with pytest.raises(LLMUnrecoverableError):
            classifier.classify_batch(batch=posts)

    def test_llm_exception_generates_failed_labels(
        self, mock_llm_service, mock_generate_prompts
    ):
        """Test with LLMException after retries exhausted (should generate failed labels).

        Expected behavior:
            - Should catch LLMException and generate failed labels
            - Should call _generate_failed_labels
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri="uri_1",
                text="test post",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
        ]
        mock_generate_prompts.return_value = ["prompt"]
        mock_llm_service.structured_batch_completion.side_effect = LLMException(
            "Retries exhausted"
        )

        classifier = IntergroupClassifier()

        # Act
        result = classifier.classify_batch(batch=posts)

        # Assert
        assert len(result) == 1
        assert result[0].was_successfully_labeled is False
        assert result[0].label == -1
        assert result[0].reason == "Retries exhausted"

    def test_value_error_from_merge_generates_failed_labels(
        self, mock_llm_service, mock_generate_prompts
    ):
        """Test with ValueError from _merge_llm_responses_with_batch (should generate failed labels).

        Expected behavior:
            - Should catch ValueError from merge method and generate failed labels
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri="uri_1",
                text="test post",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
        ]
        mock_generate_prompts.return_value = ["prompt"]
        # Return wrong number of responses to trigger ValueError in merge
        mock_llm_service.structured_batch_completion.return_value = [
            LabelChoiceModel(label=1),
            LabelChoiceModel(label=0),
        ]  # 2 responses for 1 post

        classifier = IntergroupClassifier()

        # Act
        result = classifier.classify_batch(batch=posts)

        # Assert
        assert len(result) == 1
        assert result[0].was_successfully_labeled is False
        assert result[0].label == -1
        assert "Number of LLM responses" in result[0].reason

    def test_validation_error_generates_failed_labels(
        self, mock_llm_service, mock_generate_prompts
    ):
        """Test with ValidationError (should generate failed labels).

        Expected behavior:
            - Should catch ValidationError and generate failed labels
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri="uri_1",
                text="test post",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
        ]
        mock_generate_prompts.return_value = ["prompt"]
        # Create ValidationError properly - it takes errors as a list of dicts
        mock_llm_service.structured_batch_completion.side_effect = ValidationError.from_exception_data(
            "LabelChoiceModel", [{"type": "missing", "loc": ("label",), "msg": "Field required"}]
        )

        classifier = IntergroupClassifier()

        # Act
        result = classifier.classify_batch(batch=posts)

        # Assert
        assert len(result) == 1
        assert result[0].was_successfully_labeled is False
        assert result[0].label == -1

    def test_generate_failed_labels_called_on_retryable_errors(
        self, mock_llm_service, mock_generate_prompts
    ):
        """Test that _generate_failed_labels is called on retryable errors.

        Expected behavior:
            - Should call _generate_failed_labels when catching retryable errors
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri="uri_1",
                text="test post",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
        ]
        mock_generate_prompts.return_value = ["prompt"]
        mock_llm_service.structured_batch_completion.side_effect = LLMException(
            "Error"
        )

        classifier = IntergroupClassifier()

        with patch.object(
            classifier, "_generate_failed_labels"
        ) as mock_generate_failed:
            mock_generate_failed.return_value = [
                IntergroupLabelModel(
                    uri="uri_1",
                    text="test post",
                    preprocessing_timestamp="2024-01-01-12:00:00",
                    was_successfully_labeled=False,
                    reason="Error",
                    label=-1,
                    label_timestamp=TEST_LABEL_TIMESTAMP,
                )
            ]

            # Act
            classifier.classify_batch(batch=posts)

            # Assert
            mock_generate_failed.assert_called_once_with(
                batch=posts, reason="Error"
            )

    def test_custom_llm_model_name(self, mock_llm_service, mock_generate_prompts):
        """Test custom llm_model_name parameter.

        Expected behavior:
            - Should use custom model name when provided
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri="uri_1",
                text="test post",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
        ]
        mock_generate_prompts.return_value = ["prompt"]
        mock_llm_service.structured_batch_completion.return_value = [
            LabelChoiceModel(label=1)
        ]

        classifier = IntergroupClassifier()

        # Act
        classifier.classify_batch(batch=posts, llm_model_name="custom-model-name")

        # Assert
        call_kwargs = mock_llm_service.structured_batch_completion.call_args[1]
        assert call_kwargs["model"] == "custom-model-name"

    def test_default_llm_model_name(self, mock_llm_service, mock_generate_prompts):
        """Test default llm_model_name (DEFAULT_LLM_MODEL_NAME).

        Expected behavior:
            - Should use DEFAULT_LLM_MODEL_NAME when not specified
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri="uri_1",
                text="test post",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
        ]
        mock_generate_prompts.return_value = ["prompt"]
        mock_llm_service.structured_batch_completion.return_value = [
            LabelChoiceModel(label=1)
        ]

        classifier = IntergroupClassifier()

        # Act
        classifier.classify_batch(batch=posts)

        # Assert
        call_kwargs = mock_llm_service.structured_batch_completion.call_args[1]
        assert call_kwargs["model"] == DEFAULT_LLM_MODEL_NAME


class TestIntergroupClassifierMergeLlmResponsesWithBatch:
    """Tests for IntergroupClassifier._merge_llm_responses_with_batch method."""

    def test_successful_merge_single_post(self):
        """Test successful merge with single post.

        Expected behavior:
            - Should create IntergroupLabelModel with correct fields
            - Should merge post data with LLM response label
        """
        # Arrange
        post = PostToLabelModel(
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            batch_id=1,
            batch_metadata="{}",
        )
        llm_response = LabelChoiceModel(label=1)

        classifier = IntergroupClassifier()

        # Act
        result = classifier._merge_llm_responses_with_batch(
            batch=[post], llm_responses=[llm_response]
        )

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], IntergroupLabelModel)
        assert result[0].uri == "uri_1"
        assert result[0].text == "test post"
        assert result[0].preprocessing_timestamp == "2024-01-01-12:00:00"
        assert result[0].label == 1

    def test_successful_merge_multiple_posts(self):
        """Test successful merge with multiple posts.

        Expected behavior:
            - Should create IntergroupLabelModel for each post
            - Should correctly match posts with responses
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri=f"uri_{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=i,
                batch_metadata="{}",
            )
            for i in range(3)
        ]
        llm_responses = [
            LabelChoiceModel(label=1),
            LabelChoiceModel(label=0),
            LabelChoiceModel(label=1),
        ]

        classifier = IntergroupClassifier()

        # Act
        result = classifier._merge_llm_responses_with_batch(
            batch=posts, llm_responses=llm_responses
        )

        # Assert
        assert len(result) == 3
        assert all(isinstance(label, IntergroupLabelModel) for label in result)
        assert result[0].uri == "uri_0"
        assert result[0].label == 1
        assert result[1].uri == "uri_1"
        assert result[1].label == 0
        assert result[2].uri == "uri_2"
        assert result[2].label == 1

    def test_intergroup_label_model_instances_created_correctly(self):
        """Test that IntergroupLabelModel instances are created correctly.

        Expected behavior:
            - Should create properly structured IntergroupLabelModel instances
            - All required fields should be present
        """
        # Arrange
        post = PostToLabelModel(
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            batch_id=1,
            batch_metadata="{}",
        )
        llm_response = LabelChoiceModel(label=0)

        classifier = IntergroupClassifier()

        # Act
        result = classifier._merge_llm_responses_with_batch(
            batch=[post], llm_responses=[llm_response]
        )

        # Assert
        assert len(result) == 1
        label = result[0]
        assert hasattr(label, "uri")
        assert hasattr(label, "text")
        assert hasattr(label, "preprocessing_timestamp")
        assert hasattr(label, "was_successfully_labeled")
        assert hasattr(label, "reason")
        assert hasattr(label, "label")

    def test_was_successfully_labeled_true(self):
        """Test that was_successfully_labeled=True for all merged labels.

        Expected behavior:
            - All merged labels should have was_successfully_labeled=True
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri=f"uri_{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=i,
                batch_metadata="{}",
            )
            for i in range(2)
        ]
        llm_responses = [
            LabelChoiceModel(label=1),
            LabelChoiceModel(label=0),
        ]

        classifier = IntergroupClassifier()

        # Act
        result = classifier._merge_llm_responses_with_batch(
            batch=posts, llm_responses=llm_responses
        )

        # Assert
        assert all(label.was_successfully_labeled is True for label in result)

    def test_reason_none(self):
        """Test that reason=None for all merged labels.

        Expected behavior:
            - All merged labels should have reason=None
        """
        # Arrange
        post = PostToLabelModel(
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            batch_id=1,
            batch_metadata="{}",
        )
        llm_response = LabelChoiceModel(label=1)

        classifier = IntergroupClassifier()

        # Act
        result = classifier._merge_llm_responses_with_batch(
            batch=[post], llm_responses=[llm_response]
        )

        # Assert
        assert result[0].reason is None

    def test_label_value_from_llm_response(self):
        """Test that label value comes from llm_response.label.

        Expected behavior:
            - Label value should match LLM response label
        """
        # Arrange
        post = PostToLabelModel(
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            batch_id=1,
            batch_metadata="{}",
        )
        llm_response = LabelChoiceModel(label=0)

        classifier = IntergroupClassifier()

        # Act
        result = classifier._merge_llm_responses_with_batch(
            batch=[post], llm_responses=[llm_response]
        )

        # Assert
        assert result[0].label == 0

    def test_uri_text_preprocessing_timestamp_preserved(self):
        """Test that uri, text, preprocessing_timestamp are preserved from post.

        Expected behavior:
            - Should preserve all post fields in merged label
        """
        # Arrange
        post = PostToLabelModel(
            uri="custom_uri",
            text="custom text",
            preprocessing_timestamp="2024-12-31-23:59:59",
            batch_id=999,
            batch_metadata="custom_metadata",
        )
        llm_response = LabelChoiceModel(label=1)

        classifier = IntergroupClassifier()

        # Act
        result = classifier._merge_llm_responses_with_batch(
            batch=[post], llm_responses=[llm_response]
        )

        # Assert
        assert result[0].uri == "custom_uri"
        assert result[0].text == "custom text"
        assert result[0].preprocessing_timestamp == "2024-12-31-23:59:59"

    def test_value_error_when_lengths_dont_match(self):
        """Test that ValueError is raised when llm_responses length doesn't match batch length.

        Expected behavior:
            - Should raise ValueError when counts don't match
        """
        # Arrange
        post = PostToLabelModel(
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            batch_id=1,
            batch_metadata="{}",
        )
        llm_responses = [
            LabelChoiceModel(label=1),
            LabelChoiceModel(label=0),
        ]  # 2 responses for 1 post

        classifier = IntergroupClassifier()

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            classifier._merge_llm_responses_with_batch(
                batch=[post], llm_responses=llm_responses
            )

        assert "Number of LLM responses" in str(exc_info.value)
        assert "does not match" in str(exc_info.value)

    def test_value_error_message_includes_both_counts(self):
        """Test ValueError message includes both counts.

        Expected behavior:
            - Error message should include both response count and batch count
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri=f"uri_{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=i,
                batch_metadata="{}",
            )
            for i in range(3)
        ]
        llm_responses = [
            LabelChoiceModel(label=1),
        ]  # 1 response for 3 posts

        classifier = IntergroupClassifier()

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            classifier._merge_llm_responses_with_batch(
                batch=posts, llm_responses=llm_responses
            )

        error_message = str(exc_info.value)
        assert "1" in error_message  # response count
        assert "3" in error_message  # batch count

    def test_empty_batch_and_responses(self):
        """Test with empty batch and empty responses (edge case).

        Expected behavior:
            - Should return empty list without error
        """
        # Arrange
        posts = []
        llm_responses = []

        classifier = IntergroupClassifier()

        # Act
        result = classifier._merge_llm_responses_with_batch(
            batch=posts, llm_responses=llm_responses
        )

        # Assert
        assert result == []
        assert isinstance(result, list)


class TestIntergroupClassifierGenerateFailedLabels:
    """Tests for IntergroupClassifier._generate_failed_labels method."""

    def test_single_post(self):
        """Test with single post.

        Expected behavior:
            - Should create one failed label
            - Should have correct structure
        """
        # Arrange
        post = PostToLabelModel(
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            batch_id=1,
            batch_metadata="{}",
        )

        classifier = IntergroupClassifier()

        # Act
        result = classifier._generate_failed_labels(batch=[post], reason="Test error")

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], IntergroupLabelModel)

    def test_multiple_posts(self):
        """Test with multiple posts.

        Expected behavior:
            - Should create failed label for each post
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri=f"uri_{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=i,
                batch_metadata="{}",
            )
            for i in range(3)
        ]

        classifier = IntergroupClassifier()

        # Act
        result = classifier._generate_failed_labels(
            batch=posts, reason="Batch error"
        )

        # Assert
        assert len(result) == 3
        assert all(isinstance(label, IntergroupLabelModel) for label in result)

    def test_all_labels_have_was_successfully_labeled_false(self):
        """Test that all labels have was_successfully_labeled=False.

        Expected behavior:
            - All generated labels should have was_successfully_labeled=False
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri=f"uri_{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=i,
                batch_metadata="{}",
            )
            for i in range(2)
        ]

        classifier = IntergroupClassifier()

        # Act
        result = classifier._generate_failed_labels(
            batch=posts, reason="Error"
        )

        # Assert
        assert all(label.was_successfully_labeled is False for label in result)

    def test_reason_included(self):
        """Test that reason is included in labels.

        Expected behavior:
            - All labels should have reason field set
        """
        # Arrange
        post = PostToLabelModel(
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            batch_id=1,
            batch_metadata="{}",
        )

        classifier = IntergroupClassifier()

        # Act
        result = classifier._generate_failed_labels(
            batch=[post], reason="Custom error message"
        )

        # Assert
        assert result[0].reason == "Custom error message"

    def test_label_value_default_failed_label_value(self):
        """Test that label value is _DEFAULT_FAILED_LABEL_VALUE (-1).

        Expected behavior:
            - All failed labels should have label=-1
        """
        # Arrange
        from services.ml_inference.intergroup.classifier import _DEFAULT_FAILED_LABEL_VALUE

        posts = [
            PostToLabelModel(
                uri=f"uri_{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=i,
                batch_metadata="{}",
            )
            for i in range(2)
        ]

        classifier = IntergroupClassifier()

        # Act
        result = classifier._generate_failed_labels(batch=posts, reason="Error")

        # Assert
        assert all(label.label == _DEFAULT_FAILED_LABEL_VALUE for label in result)
        assert all(label.label == -1 for label in result)

    def test_uri_text_preprocessing_timestamp_preserved(self):
        """Test that uri, text, preprocessing_timestamp are preserved.

        Expected behavior:
            - Should preserve all post fields in failed labels
        """
        # Arrange
        post = PostToLabelModel(
            uri="preserved_uri",
            text="preserved text",
            preprocessing_timestamp="2024-12-31-23:59:59",
            batch_id=123,
            batch_metadata="{}",
        )

        classifier = IntergroupClassifier()

        # Act
        result = classifier._generate_failed_labels(batch=[post], reason="Error")

        # Assert
        assert result[0].uri == "preserved_uri"
        assert result[0].text == "preserved text"
        assert result[0].preprocessing_timestamp == "2024-12-31-23:59:59"


class TestIntergroupClassifierGenerateBatchPrompts:
    """Tests for IntergroupClassifier._generate_batch_prompts method."""

    @pytest.fixture
    def mock_generate_batch_prompts(self):
        """Mock generate_batch_prompts function."""
        with patch(
            "services.ml_inference.intergroup.classifier.generate_batch_prompts"
        ) as mock:
            yield mock

    def test_single_post(self, mock_generate_batch_prompts):
        """Test with single post.

        Expected behavior:
            - Should call generate_batch_prompts with single post
        """
        # Arrange
        post = PostToLabelModel(
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            batch_id=1,
            batch_metadata="{}",
        )
        mock_generate_batch_prompts.return_value = ["prompt_1"]

        classifier = IntergroupClassifier()

        # Act
        result = classifier._generate_batch_prompts(batch=[post])

        # Assert
        assert result == ["prompt_1"]
        mock_generate_batch_prompts.assert_called_once()

    def test_multiple_posts(self, mock_generate_batch_prompts):
        """Test with multiple posts.

        Expected behavior:
            - Should call generate_batch_prompts with all posts
        """
        # Arrange
        posts = [
            PostToLabelModel(
                uri=f"uri_{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=i,
                batch_metadata="{}",
            )
            for i in range(3)
        ]
        mock_generate_batch_prompts.return_value = ["prompt_1", "prompt_2", "prompt_3"]

        classifier = IntergroupClassifier()

        # Act
        result = classifier._generate_batch_prompts(batch=posts)

        # Assert
        assert len(result) == 3
        mock_generate_batch_prompts.assert_called_once()

    def test_generate_batch_prompts_called_with_correct_parameters(
        self, mock_generate_batch_prompts
    ):
        """Test that generate_batch_prompts is called with correct parameters.

        Expected behavior:
            - Should pass batch, prompt_template, and template_variable_to_model_field_mapping
        """
        # Arrange
        from services.ml_inference.intergroup.prompts import INTERGROUP_PROMPT

        posts = [
            PostToLabelModel(
                uri="uri_1",
                text="test post",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
        ]
        mock_generate_batch_prompts.return_value = ["prompt"]

        classifier = IntergroupClassifier()

        # Act
        classifier._generate_batch_prompts(batch=posts)

        # Assert
        mock_generate_batch_prompts.assert_called_once()
        call_kwargs = mock_generate_batch_prompts.call_args[1]
        assert call_kwargs["batch"] == posts
        assert call_kwargs["prompt_template"] == INTERGROUP_PROMPT
        assert call_kwargs["template_variable_to_model_field_mapping"] == {"input": "text"}

