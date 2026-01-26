"""Contains both versions of the IntergroupClassifier class, so that I can
make edits to each and then have the production code use the one that I want to
keep.

Original version is the IntergroupClassifier
Batched version is the IntergroupBatchedClassifier
"""

from datetime import datetime, timezone

from pydantic import ValidationError, BaseModel

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

class PromptBatchModel(BaseModel):
    prompt_batch_id: int
    prompt_batch: list[PostToLabelModel]
    prompt: str

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
    ) -> list[IntergroupLabelModel]:
        """Classifies a single batch of posts with concurrent requests.
        
        Splits up a single batch of posts into concurrent requests, where
        each request has a prompt of size prompt_batch_size.

        For example, if we have 400 posts, concurrent request count is 20, and
        prompt batch size is 5, then we will have 4 concurrent request batches,
        each concurrent request batch containing 20 prompts, and each prompt
        containing 5 posts.
    
        Args:
            batch: List of posts to classify
            concurrent_request_count: Number of concurrent requests to make (must be > 0)
            prompt_batch_size: Number of posts per prompt (must be > 0)
            llm_model_name: Model name to use for classification
            
        Returns:
            List of IntergroupLabelModel instances, one per post in the input batch
            
        Raises:
            ValueError: If concurrent_request_count or prompt_batch_size are <= 0
        """
        # Input validation
        if concurrent_request_count <= 0:
            raise ValueError(
                f"concurrent_request_count must be > 0, got {concurrent_request_count}"
            )
        if prompt_batch_size <= 0:
            raise ValueError(
                f"prompt_batch_size must be > 0, got {prompt_batch_size}"
            )
        
        # Handle empty batch
        if not batch:
            logger.info("Empty batch provided, returning empty list")
            return []
        
        prompt_batches: list[PromptBatchModel] = self._create_prompt_batches(
            batch=batch, prompt_batch_size=prompt_batch_size,
        )
        concurrent_request_batches: list[list[PromptBatchModel]] = (
            self._create_concurrent_request_batches(
                prompt_batches=prompt_batches,
                concurrent_request_count=concurrent_request_count,
            )
        )
        total_results: list[IntergroupLabelModel] = []
        for concurrent_request_batch in concurrent_request_batches:
            labels_for_concurrent_request_batch = (
                self._classify_single_concurrent_request_batch(
                    concurrent_request_batch=concurrent_request_batch,
                    llm_model_name=llm_model_name,
                )
            )
            total_results.extend(labels_for_concurrent_request_batch)

        return total_results

    def _create_prompt_batches(
        self,
        batch: list[PostToLabelModel],
        prompt_batch_size: int,
    ) -> list[PromptBatchModel]:
        """Given a list of posts, create a list of prompt batches, where
        each prompt batch contains a list of posts and the prompt for that batch.
        """
        prompt_batches = create_batches(batch_list=batch, batch_size=prompt_batch_size)
        return [
            PromptBatchModel(
                prompt_batch_id=i,
                prompt_batch=prompt_batch,
                prompt=self._generate_batch_prompt(batch=prompt_batch)
            )
            for i, prompt_batch in enumerate(prompt_batches)
        ]

    def _create_concurrent_request_batches(
        self,
        prompt_batches: list[PromptBatchModel],
        concurrent_request_count: int,
    ) -> list[list[PromptBatchModel]]:
        """Given a list of prompt batches, create a list of concurrent request batches, where
        each concurrent request batch contains a list of prompt batches that will be run concurrently.
        """
        return create_batches(
            batch_list=prompt_batches, batch_size=concurrent_request_count
        )

    def _classify_single_concurrent_request_batch(
        self,
        concurrent_request_batch: list[PromptBatchModel],
        llm_model_name: str = DEFAULT_LLM_MODEL_NAME,
    ) -> list[IntergroupLabelModel]:
        """Given a single concurrent request batch, classify all the prompts in
        that batch.
        
        Args:
            concurrent_request_batch: List of prompt batches to classify concurrently
            llm_model_name: Model name to use for classification
            
        Returns:
            List of IntergroupLabelModel instances for all posts in the concurrent request batch
        """
        prompt_id_to_prompt_batch: dict[int, PromptBatchModel] = {
            prompt_batch.prompt_batch_id: prompt_batch
            for prompt_batch in concurrent_request_batch
        }
        prompts: list[str] = [
            prompt_batch.prompt
            for prompt_batch in concurrent_request_batch
        ]
        # NOTE: our implementation currently assumes that the responses are
        # returned in the same order as the prompts. Reasonable assumption
        # for now, as we've not seen evidence of this otherwise.
        prompt_ids_list: list[int] = [
            prompt_batch.prompt_batch_id
            for prompt_batch in concurrent_request_batch
        ]

        labels_for_concurrent_request_batch: list[IntergroupLabelModel] = []

        # Collect all posts from concurrent_request_batch for error handling
        all_posts_in_concurrent_batch: list[PostToLabelModel] = [
            post
            for prompt_batch in concurrent_request_batch
            for post in prompt_batch.prompt_batch
        ]

        try:
            batched_llm_responses: list[BatchedLabelChoiceModel] = (
                self.llm_service.structured_batch_completion(
                    prompts=prompts,
                    response_model=BatchedLabelChoiceModel,
                    model=llm_model_name,
                    role="user",
                )
            )
            # Validate that we got the expected number of responses
            if len(batched_llm_responses) != len(prompt_ids_list):
                raise ValueError(
                    f"Number of LLM responses ({len(batched_llm_responses)}) does not match "
                    f"number of prompts ({len(prompt_ids_list)})."
                )
            # TODO: move to a helper function.
            for (prompt_id, batched_llm_response) in zip(prompt_ids_list, batched_llm_responses):
                prompt_batch = prompt_id_to_prompt_batch[prompt_id]
                try:
                    labels_for_single_request_batch = self._merge_batched_llm_responses_with_batch(
                        batch=prompt_batch.prompt_batch,
                        batched_llm_responses=[batched_llm_response]
                    )
                    labels_for_concurrent_request_batch.extend(labels_for_single_request_batch)
                except (ValueError, ValidationError) as e:
                    # Per-prompt-batch validation error (e.g., wrong number of labels)
                    # Generate failed labels only for this specific prompt batch
                    logger.error(
                        f"Failed to merge LLM response for prompt batch {prompt_id} "
                        f"(contains {len(prompt_batch.prompt_batch)} posts): {e}",
                        exc_info=True,
                    )
                    failed_labels = self._generate_failed_labels(
                        batch=prompt_batch.prompt_batch,
                        reason=f"Validation error for prompt batch {prompt_id}: {str(e)}"
                    )
                    labels_for_concurrent_request_batch.extend(failed_labels)
                    # Continue processing remaining prompt batches
            
        except (
            LLMAuthError,
            LLMInvalidRequestError,
            LLMPermissionDeniedError,
            LLMUnrecoverableError,
        ):
            # Non-retryable errors: configuration issues, let them propagate
            # These indicate problems that need immediate attention
            raise

        except (LLMException) as e:
            # LLM service-level errors (retries exhausted, network issues, etc.)
            # These affect the entire concurrent request batch
            logger.error(
                f"Failed to classify concurrent request batch after retries exhausted: {e}",
                exc_info=True,
            )
            logger.info(f"Generating failed labels for concurrent request batch due to error: {e}")
            # Generate failed labels for all posts in the concurrent request batch.
            # NOTE: we currently treat the entire concurrent request batch as failed, which is
            # our current design choice.
            labels_for_concurrent_request_batch = self._generate_failed_labels(
                batch=all_posts_in_concurrent_batch, reason=str(e)
            )

        return labels_for_concurrent_request_batch

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
