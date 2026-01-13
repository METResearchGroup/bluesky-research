from lib.batching_utils import create_batches
from lib.log.logger import get_logger
from ml_tooling.llm.prompt_utils import generate_batch_prompts
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

    # TODO: add stronger typing for input and output.
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
        # TODO: add deadletter queue for failed batches.
        # NOTE: logic can be:
        # 1. Add failed posts to single in-memory deadletter queue.
        # 2. Retry deadletter queue once, after everything else is done.
        # 3. Anything that fails, I can put in the Queue class

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
        labels: list[IntergroupLabelModel] = (
            self.llm_service.structured_batch_completion(
                prompts=batch_prompts,
                response_model=IntergroupLabelModel,
                model=llm_model_name,
                role="user",
            )
        )
        return labels

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

    def _export_single_batch(
        self,
        labels: list[IntergroupLabelModel],
    ) -> None:
        """Exports a single batch of labels."""
        # TODO: use the Queue class to export the labels.
        # TODO: delegate to a data exporter. Think about the pattern
        # to use here.
        print(labels)
