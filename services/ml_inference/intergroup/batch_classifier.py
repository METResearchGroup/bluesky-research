"""Batch classification + export adapter for intergroup.

This module bridges:
- queue/orchestration layer: `list[PostToLabelModel]` posts that include `batch_id` and `batch_metadata`
- classifier layer: `IntergroupClassifier` operating on `PostToLabelModel`

It owns I/O (queue exports) and batching; the classifier stays pure.
"""

from __future__ import annotations

import sys
from typing import Optional

from tqdm import tqdm

from lib.batching_utils import create_batches, update_batching_progress
from lib.helper import track_performance
from lib.log.logger import get_logger
from services.ml_inference.export_data import (
    attach_batch_id_to_label_dicts,
    return_failed_labels_to_input_queue,
    split_labels_into_successful_and_failed_labels,
    write_posts_to_cache,
)
from services.ml_inference.intergroup.classifier import IntergroupClassifier
from services.ml_inference.intergroup.constants import DEFAULT_BATCH_SIZE
from services.ml_inference.intergroup.models import IntergroupLabelModel
from services.ml_inference.models import (
    BatchClassificationMetadataModel,
    PostToLabelModel,
    LabelWithBatchId,
)

logger = get_logger(__name__)


@track_performance
def run_batch_classification(
    posts: list[PostToLabelModel],
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
) -> BatchClassificationMetadataModel:
    """Run intergroup batch classification over queue-provided posts.

    Expects `posts` to come from `get_posts_to_classify()`, which constructs
    PostToLabelModel instances from `Queue.load_dict_items_from_queue()` payloads.

    Returns BatchClassificationMetadataModel with classification results metadata.
    """
    if not posts:
        return BatchClassificationMetadataModel(
            total_batches=0,
            total_posts_successfully_labeled=0,
            total_posts_failed_to_label=0,
        )

    batches: list[list[PostToLabelModel]] = create_batches(
        batch_list=posts, batch_size=batch_size
    )
    total_batches = len(batches)
    total_successful = 0
    total_failed = 0

    classifier = IntergroupClassifier()

    # Create tqdm progress bar (conditional on interactive terminal)
    pbar = tqdm(
        batches,
        desc="Classifying batches",
        unit="batch",
        disable=not sys.stdout.isatty(),  # Disable in CI/CD
        file=sys.stderr,  # Write to stderr to avoid log conflicts
    )

    try:
        for i, batch in enumerate(pbar):
            update_batching_progress(
                batch_index=i,
                batch_interval=10,
                total_batches=total_batches,
                logger=logger,
            )

            uri_to_batch_id = {p.uri: p.batch_id for p in batch}

            label_models: list[IntergroupLabelModel] = classifier.classify_batch(batch)
            successful_labels, failed_labels = (
                split_labels_into_successful_and_failed_labels(labels=label_models)
            )

            if len(failed_labels) > 0:
                updated_total_failed_count: int = _manage_failed_labels(
                    failed_labels=failed_labels,
                    uri_to_batch_id=uri_to_batch_id,
                    total_failed_so_far=total_failed,
                )
                total_failed = updated_total_failed_count

            if len(successful_labels) > 0:
                updated_total_successful_count: int = _manage_successful_labels(
                    successful_labels=successful_labels,
                    uri_to_batch_id=uri_to_batch_id,
                    total_successful_so_far=total_successful,
                )
                total_successful = updated_total_successful_count

            # Update progress bar with live counts
            pbar.set_postfix(
                {
                    "successful": total_successful,
                    "failed": total_failed,
                }
            )
    finally:
        pbar.close()  # Explicitly close progress bar

    return BatchClassificationMetadataModel(
        total_batches=total_batches,
        total_posts_successfully_labeled=total_successful,
        total_posts_failed_to_label=total_failed,
    )


def _manage_failed_labels(
    failed_labels: list[IntergroupLabelModel],
    uri_to_batch_id: dict[str, int],
    total_failed_so_far: int,
) -> int:
    """Manages failed labels and returns the updated total number of
    failed labels.
    """
    failed_label_models: list[LabelWithBatchId] = attach_batch_id_to_label_dicts(
        labels=[label.model_dump() for label in failed_labels],
        uri_to_batch_id=uri_to_batch_id,
    )
    logger.warning(
        f"Failed to label {len(failed_label_models)} posts. Re-inserting into queue."
    )
    return_failed_labels_to_input_queue(
        inference_type="intergroup",
        failed_label_models=failed_label_models,
    )
    return total_failed_so_far + len(failed_label_models)


def _manage_successful_labels(
    successful_labels: list[IntergroupLabelModel],
    uri_to_batch_id: dict[str, int],
    total_successful_so_far: int,
) -> int:
    """Manages successful labels and returns the updated total number of
    successful labels.

    Attaches batch_id to labels for queue management operations.
    Creates validated LabelWithBatchId instances.
    """
    # Use helper function - enforces contract and documents intent
    successful_label_models: list[LabelWithBatchId] = attach_batch_id_to_label_dicts(
        labels=[label.model_dump() for label in successful_labels],
        uri_to_batch_id=uri_to_batch_id,
    )
    logger.info(f"Successfully labeled {len(successful_label_models)} posts.")
    write_posts_to_cache(
        inference_type="intergroup",
        posts=successful_label_models,
    )
    return total_successful_so_far + len(successful_label_models)
