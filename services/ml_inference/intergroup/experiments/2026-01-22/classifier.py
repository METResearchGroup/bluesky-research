"""Contains both versions of the IntergroupClassifier class, so that I can
make edits to each and then have the production code use the one that I want to
keep.

Original version is the IntergroupClassifier
Batched version is the IntergroupBatchedClassifier
"""

from datetime import datetime, timezone

from pydantic import ValidationError

from lib.batching_utils import create_batches
from lib.constants import timestamp_format
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
from services.ml_inference.intergroup.models import (
    IntergroupLabelModel,
    LabelChoiceModel,
    BatchedLabelChoiceModel,
)
from services.ml_inference.intergroup.prompts import INTERGROUP_PROMPT
from services.ml_inference.models import PostToLabelModel

from prompts import BATCHED_INTERGROUP_PROMPT

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
            llm_responses: list[LabelChoiceModel] = (
                self.llm_service.structured_batch_completion(
                    prompts=batch_prompts,
                    response_model=LabelChoiceModel,
                    model=llm_model_name,
                    role="user",
                )
            )
            return self._merge_llm_responses_with_batch(
                batch=batch, llm_responses=llm_responses
            )
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

    def _merge_llm_responses_with_batch(
        self,
        batch: list[PostToLabelModel],
        llm_responses: list[LabelChoiceModel],
    ) -> list[IntergroupLabelModel]:
        """Merges LLM responses with input batch to create full IntergroupLabelModel instances.

        Args:
            batch: List of input posts to be labeled
            llm_responses: List of lightweight LLM responses containing only the label

        Returns:
            List of complete IntergroupLabelModel instances with all required fields

        Raises:
            ValueError: If the number of LLM responses doesn't match the number of posts
        """
        if len(llm_responses) != len(batch):
            raise ValueError(
                f"Number of LLM responses ({len(llm_responses)}) does not match "
                f"number of posts ({len(batch)})."
            )

        label_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)
        labels: list[IntergroupLabelModel] = [
            IntergroupLabelModel(
                uri=post.uri,
                text=post.text,
                preprocessing_timestamp=post.preprocessing_timestamp,
                was_successfully_labeled=True,
                reason=None,
                label=llm_response.label,
                label_timestamp=label_timestamp,
            )
            for post, llm_response in zip(batch, llm_responses)
        ]
        return labels

    def _generate_failed_labels(
        self,
        batch: list[PostToLabelModel],
        reason: str,
    ) -> list[IntergroupLabelModel]:
        """Generates a list of failed labels. We create default
        failed labels for posts that couldn't be labeled."""
        label_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)
        failed_labels: list[IntergroupLabelModel] = [
            IntergroupLabelModel(
                uri=post.uri,
                text=post.text,
                preprocessing_timestamp=post.preprocessing_timestamp,
                was_successfully_labeled=False,
                reason=reason,
                label=_DEFAULT_FAILED_LABEL_VALUE,
                label_timestamp=label_timestamp,
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


class IntergroupBatchedClassifier(IntergroupClassifier):
    def classify_batch(
        self,
        batch: list[PostToLabelModel],
        llm_model_name: str = DEFAULT_LLM_MODEL_NAME,
    ) -> list[IntergroupLabelModel]:
        """Classifies a single batch of posts."""
        batch_prompt = self._generate_batch_prompt(batch=batch)
        try:
            # should be a list of 1
            batched_llm_responses: list[BatchedLabelChoiceModel] = (
                self.llm_service.structured_batch_completion(
                    prompts=[batch_prompt],
                    response_model=BatchedLabelChoiceModel,
                    model=llm_model_name,
                    role="user",
                )
            )
            return self._merge_batched_llm_responses_with_batch(
                batch=batch, batched_llm_responses=batched_llm_responses
            )
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

    def classify_batch_with_prompt_batching(
        self,
        batch: list[PostToLabelModel],
        concurrent_request_count: int,
        prompt_batch_size: int,
        llm_model_name: str = DEFAULT_LLM_MODEL_NAME,
    ):
        """Classifies a single batch of posts with concurrent requests.
        
        Splits up a single batch of posts into concurrent requests, where
        each request has a prompt of size prompt_batch_size.
        """
        # e.g., if we have 400 posts, concurrent request count is 20, and prompt batch size is 5,
        # we will have 4 concurrent request batches, each concurrent request batch
        # containing 20 prompts, and each prompt containing 5 posts.
        concurrent_request_batches = self._create_concurrent_request_batches(batch=batch, prompt_batch_size=prompt_batch_size, concurrent_request_count=concurrent_request_count)
        total_results: list[IntergroupLabelModel] = []

        # loop through each concurrent request batch and then run all the
        # prompts in that batch concurrently.
        for prompts_for_concurrent_request_batch in concurrent_request_batches:
            labels_for_concurrent_request_batch: list[IntergroupLabelModel] = []
            try:
                batched_llm_responses: list[BatchedLabelChoiceModel] = (
                    self.llm_service.structured_batch_completion(
                        prompts=prompts_for_concurrent_request_batch,
                        response_model=BatchedLabelChoiceModel,
                        model=llm_model_name,
                        role="user",
                    )
                )
                labels_for_concurrent_request_batch = self._merge_batched_llm_responses_with_batch(
                    batch=batch, batched_llm_responses=batched_llm_responses
                )
            except (
                LLMAuthError,
                LLMInvalidRequestError,
                LLMPermissionDeniedError,
                LLMUnrecoverableError,
            ) as e:
                # Non-retryable errors: configuration issues, let them propagate
                # These indicate problems that need immediate attention
                logger.error(
                    f"Non-retryable error encountered while classifying concurrent request batch: {e}",
                    exc_info=True,
                )
                labels_for_concurrent_request_batch = self._generate_failed_labels(batch=batch, reason=str(e))
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
                labels_for_concurrent_request_batch = self._generate_failed_labels(batch=batch, reason=str(e))
            finally:
                total_results.extend(labels_for_concurrent_request_batch)

        return total_results

    def _create_single_concurrent_request_batch(
        self,
        batch: list[PostToLabelModel],
        prompt_batch_size: int,
    ) -> list[str]:
        """Creates a list of prompts to run concurrently as one batch."""
        batches_per_prompt: list[list[PostToLabelModel]] = (
            create_batches(batch_list=batch, batch_size=prompt_batch_size)
        )
        prompts: list[str] = [
            self._generate_batch_prompt(batch=batch)
            for batch in batches_per_prompt
        ]
        return prompts
    
    # TODO: figure out what to do if not divisible. Can just truncate
    # the last batch (e.g., get whatever is left).
    def _create_concurrent_request_batches(
        self,
        batch: list[PostToLabelModel],
        prompt_batch_size: int,
        concurrent_request_count: int,
    ) -> list[list[str]]:
        """Given a batch of post, create a nested list of prompts, where
        each inner list is a batch of prompts that will be run concurrently.
        """
        records_per_concurrent_request_batch: list[list[PostToLabelModel]] = (
            create_batches(batch_list=batch, batch_size=concurrent_request_count)
        )
        concurrent_request_batches: list[list[str]] = [
            self._create_single_concurrent_request_batch(batch=batch, prompt_batch_size=prompt_batch_size)
            for batch in records_per_concurrent_request_batch
        ]
        return concurrent_request_batches

    def _generate_batch_prompt(
        self,
        batch: list[PostToLabelModel],
    ) -> str:
        """Generates a batch prompt for a batch of posts.
        
        We include all batch posts into a single prompt.
        """
        enumerated_texts: str = ""
        for i, post in enumerate(batch):
            enumerated_texts += f"{i+1}. {post.text}\n"
        return BATCHED_INTERGROUP_PROMPT.format(enumerated_texts=enumerated_texts)

    # TODO: would be great to retry if any of the ValueError's are raised.
    # Currently, we only have retries within LLMService.
    # Two ways to approach it:
    # 1: Retry within IntergroupBatchedClassifier.
    # 2. Retry within LLMService (e.g., have the length validation as part of the
    # pydantic model validation (NOTE: can investigate this later))
    def _merge_batched_llm_responses_with_batch(
        self,
        batch: list[PostToLabelModel],
        batched_llm_responses: list[BatchedLabelChoiceModel],
    ) -> list[IntergroupLabelModel]:
        """Merges batched LLM responses with input batch to create full IntergroupLabelModel instances."""
        if len(batched_llm_responses) != 1:
            raise ValueError(
                f"Number of batched LLM responses ({len(batched_llm_responses)}) does not match 1."
            )
        batched_llm_response: BatchedLabelChoiceModel = batched_llm_responses[0]
        batch_labels: list[LabelChoiceModel] = batched_llm_response.labels
        if len(batch_labels) != len(batch):
            raise ValueError(
                f"Number of batch labels ({len(batch_labels)}) does not match number of posts ({len(batch)})."
            )
        labels: list[IntergroupLabelModel] = [
            IntergroupLabelModel(
                uri=batch[i].uri,
                text=batch[i].text,
                preprocessing_timestamp=batch[i].preprocessing_timestamp,
                was_successfully_labeled=True,
                reason=None,
                label=label.label,
                label_timestamp=datetime.now(timezone.utc).strftime(timestamp_format),
            )
            for i, label in enumerate(batch_labels)
        ]
        return labels
