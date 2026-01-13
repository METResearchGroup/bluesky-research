from pydantic import ValidationError

from lib.batching_utils import create_batches
from lib.log.logger import get_logger
from ml_tooling.llm.prompt_utils import generate_batch_prompts
from ml_tooling.llm.exceptions import (
    LLMException,
    LLMAuthError,
    LLMInvalidRequestError,
    LLMPermissionDeniedError,
    LLMUnrecoverableError,
)
from services.ml_inference.export_data import (
    return_failed_labels_to_input_queue,
    split_labels_into_successful_and_failed_labels,
    write_posts_to_cache,
)
from services.ml_inference.intergroup.constants import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_LLM_MODEL_NAME,
)
from services.ml_inference.intergroup.models import IntergroupLabelModel
from services.ml_inference.intergroup.prompts import INTERGROUP_PROMPT
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


logger = get_logger(__name__)


class IntergroupClassifier:
    def __init__(self):
        from ml_tooling.llm.llm_service import LLMService, get_llm_service

        self.llm_service: LLMService = get_llm_service()
        # self.export_service: DataExporterService = None

    def classify_posts(
        self,
        posts: list[FilteredPreprocessedPostModel],
    ) -> None:
        """Public-facing API for classifying posts."""
        self._batch_classify_posts(posts=posts)

    def _batch_classify_posts(
        self,
        posts: list[FilteredPreprocessedPostModel],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        batches: list[list[FilteredPreprocessedPostModel]] = create_batches(
            batch_list=posts, batch_size=batch_size
        )
        total_batches: int = len(batches)

        # TODO: add a progress bar.
        for i, batch in enumerate(batches):
            logger.info(f"Classifying batch {i+1} out of {total_batches} batches...")
            labels: list[IntergroupLabelModel] = self._classify_single_batch(batch)
            self._export_single_batch(labels=labels)
            logger.info(
                f"----Successfully classified and exported batch {i+1} out of {total_batches} batches."
            )
        logger.info(f"Finished classifying and exporting all {total_batches} batches.")

    def _classify_single_batch(
        self,
        batch: list[FilteredPreprocessedPostModel],
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
            # Generate failed labels for all posts in the batch
            return self._generate_failed_labels(batch=batch, reason=str(e))

    def _generate_failed_labels(
        self,
        batch: list[FilteredPreprocessedPostModel],
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
                label=0,  # set default of 0 for now.
            )
            for post in batch
        ]
        return failed_labels

    def _generate_batch_prompts(
        self,
        batch: list[FilteredPreprocessedPostModel],
    ) -> list[str]:
        """Generates a list of prompts for a batch of posts."""
        return generate_batch_prompts(
            batch=batch,
            prompt_template=INTERGROUP_PROMPT,
            template_variable_to_model_field_mapping={"input": "text"},
        )

    def _export_single_batch(self, labels: list[IntergroupLabelModel]) -> None:
        """Exports a single batch of labels.

        Separates successful and failed labels, then exports them using
        the shared export functions from export_data module.

        Args:
            labels: List of IntergroupLabelModel instances from classification
            batch: Original batch of posts (used for batch_id if available)
            batch_size: Batch size for queue operations
        """
        successful_labels, failed_labels = (
            split_labels_into_successful_and_failed_labels(labels=labels)
        )

        if failed_labels:
            logger.warning(
                f"Failed to label {len(failed_labels)} posts. Re-inserting into queue."
            )
            return_failed_labels_to_input_queue(
                inference_type="intergroup",
                failed_label_models=[label.model_dump() for label in failed_labels],
            )

        # TODO: connect this to the batching queue system (but
        # also make it sufficiently decoupled so that this isn't 100% tied to that
        # system).
        # Export successful labels to output queue
        if successful_labels:
            logger.info(f"Successfully labeled {len(successful_labels)} posts.")
            # NOTE: write_posts_to_cache requires batch_id in the posts.
            # If batch_id is not available in the labels or batch, this will fail.
            # The intergroup classifier may need to be integrated with the queue system
            # to provide batch_id, or batch_id needs to be added to the labels.
            write_posts_to_cache(
                inference_type="intergroup",
                posts=[label.model_dump() for label in successful_labels],
            )
