"""Batch classification + export adapter for intergroup.

This module bridges:
- queue/orchestration layer: `list[dict]` posts that include `batch_id` and `batch_metadata`
- classifier layer: `IntergroupClassifier` operating on `PostToLabelModel`

It owns I/O (queue exports) and batching; the classifier stays pure.
"""

from __future__ import annotations

from typing import Optional

from lib.batching_utils import create_batches, update_batching_progress
from lib.helper import track_performance
from lib.log.logger import get_logger
from services.ml_inference.export_data import (
    return_failed_labels_to_input_queue,
    split_labels_into_successful_and_failed_labels,
    write_posts_to_cache,
)
from services.ml_inference.intergroup.classifier import IntergroupClassifier
from services.ml_inference.intergroup.constants import DEFAULT_BATCH_SIZE
from services.ml_inference.models import (
    BatchClassificationMetadataModel,
    PostToLabelModel,
)

logger = get_logger(__name__)


# TODO: only needed here since the current interface for the intergroup classifier
# takes a strongly-typed pydantic model, while previous interfaces took a dict.
# this should really be refactored so every interface uses PostToLabelModel
# directly instead of a dict. Should create a Github issue for this and make
# it a fast-follow item.
def _dict_to_post_model(post: dict) -> PostToLabelModel:
    """Convert a queue payload dict to a PostToLabelModel."""
    return PostToLabelModel(
        uri=post["uri"],
        text=post["text"],
        preprocessing_timestamp=post["preprocessing_timestamp"],
        batch_id=post["batch_id"],
        batch_metadata=post["batch_metadata"],
    )


@track_performance
def run_batch_classification(
    posts: list[dict],
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
) -> BatchClassificationMetadataModel:
    """Run intergroup batch classification over queue-provided posts.

    Expects `posts` to come from `Queue.load_dict_items_from_queue()`, which
    attaches:
    - `batch_id`: queue row id (used by `write_posts_to_cache` for deletion)

    Returns BatchClassificationMetadataModel with classification results metadata.
    """
    if not posts:
        return BatchClassificationMetadataModel(
            total_batches=0,
            total_posts_successfully_labeled=0,
            total_posts_failed_to_label=0,
        )

    batches: list[list[dict]] = create_batches(batch_list=posts, batch_size=batch_size)
    total_batches = len(batches)
    total_successful = 0
    total_failed = 0

    classifier = IntergroupClassifier()

    for i, batch in enumerate(batches):
        update_batching_progress(
            batch_index=i,
            batch_interval=10,
            total_batches=total_batches,
            logger=logger,
        )

        uri_to_batch_id = {p["uri"]: p["batch_id"] for p in batch}
        post_models = [_dict_to_post_model(p) for p in batch]

        label_models = classifier.classify_batch(post_models)
        successful_labels, failed_labels = (
            split_labels_into_successful_and_failed_labels(labels=label_models)
        )

        # TODO: move to different function.
        # Export failures (re-insert into input queue as new pending batch rows)
        if failed_labels:
            failed_dicts: list[dict] = []
            for label in failed_labels:
                d = label.model_dump()
                # keep batch_id for observability even though input re-queue ignores it
                d["batch_id"] = uri_to_batch_id.get(d["uri"])
                failed_dicts.append(d)
            logger.warning(
                f"Failed to label {len(failed_dicts)} posts. Re-inserting into queue."
            )
            return_failed_labels_to_input_queue(
                inference_type="intergroup",
                failed_label_models=failed_dicts,
                batch_size=batch_size,
            )
            total_failed += len(failed_dicts)

        # TODO: move to different function.
        # Export successes (write to output queue and delete source batch rows)
        if successful_labels:
            successful_dicts: list[dict] = []
            for label in successful_labels:
                d = label.model_dump()
                d["batch_id"] = uri_to_batch_id.get(d["uri"])
                successful_dicts.append(d)
            logger.info(f"Successfully labeled {len(successful_dicts)} posts.")
            write_posts_to_cache(
                inference_type="intergroup",
                posts=successful_dicts,
                batch_size=batch_size,
            )
            total_successful += len(successful_dicts)

    return BatchClassificationMetadataModel(
        total_batches=total_batches,
        total_posts_successfully_labeled=total_successful,
        total_posts_failed_to_label=total_failed,
    )
