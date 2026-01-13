from pydantic import ValidationError

from lib.log.logger import get_logger
from ml_tooling.llm.prompt_utils import generate_batch_prompts
from ml_tooling.llm.exceptions import (
    LLMException,
    LLMAuthError,
    LLMInvalidRequestError,
    LLMPermissionDeniedError,
    LLMUnrecoverableError,
)
from services.ml_inference.intergroup.constants import DEFAULT_LLM_MODEL_NAME
from services.ml_inference.intergroup.models import IntergroupLabelModel
from services.ml_inference.intergroup.prompts import INTERGROUP_PROMPT
from services.ml_inference.models import PostToLabelModel


logger = get_logger(__name__)

_DEFAULT_FAILED_LABEL_VALUE = -1


class IntergroupClassifier:
    """Pure intergroup classifier.

    Responsibilities:
    - prompt generation
    - calling LLM service for a provided batch
    - error handling + generating failed labels
    """

    def __init__(self):
        from ml_tooling.llm.llm_service import LLMService, get_llm_service

        self.llm_service: LLMService = get_llm_service()

    def classify_batch(
        self,
        batch: list[PostToLabelModel],
        llm_model_name: str = DEFAULT_LLM_MODEL_NAME,
    ) -> list[IntergroupLabelModel]:
        """Classifies a single batch of posts."""
        batch_prompts = self._generate_batch_prompts(batch=batch)
        try:
            labels: list[IntergroupLabelModel] = (
                self.llm_service.structured_batch_completion(
                    prompts=batch_prompts,
                    response_model=IntergroupLabelModel,
                    model=llm_model_name,
                    role="user",
                )
            )
            return labels
        except (
            LLMAuthError,
            LLMInvalidRequestError,
            LLMPermissionDeniedError,
            LLMUnrecoverableError,
        ):
            # Non-retryable errors: configuration issues, let them propagate
            # These indicate problems that need immediate attention
            raise
        except (LLMException, ValueError, ValidationError) as e:
            # Retryable errors that exhausted all retries: generate failed labels
            # These indicate transient issues or persistent validation problems
            logger.error(
                f"Failed to classify batch after retries exhausted: {e}",
                exc_info=True,
            )
            logger.info(f"Generating failed labels for batch due to error: {e}")
            # Generate failed labels for all posts in the batch.
            # NOTE: we currently treat the entire batch as failed, which is
            # our current design choice.
            return self._generate_failed_labels(batch=batch, reason=str(e))

    def _generate_failed_labels(
        self,
        batch: list[PostToLabelModel],
        reason: str,
    ) -> list[IntergroupLabelModel]:
        """Generates a list of failed labels. We create default
        failed labels for posts that couldn't be labeled."""
        failed_labels: list[IntergroupLabelModel] = [
            IntergroupLabelModel(
                uri=post.uri,
                text=post.text,
                preprocessing_timestamp=post.preprocessing_timestamp,
                was_successfully_labeled=False,
                reason=reason,
                label=_DEFAULT_FAILED_LABEL_VALUE,
            )
            for post in batch
        ]
        return failed_labels

    def _generate_batch_prompts(
        self,
        batch: list[PostToLabelModel],
    ) -> list[str]:
        """Generates a list of prompts for a batch of posts."""
        return generate_batch_prompts(
            batch=batch,
            prompt_template=INTERGROUP_PROMPT,
            template_variable_to_model_field_mapping={"input": "text"},
        )
