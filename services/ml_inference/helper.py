"""Helper tooling for ML inference."""

from collections import defaultdict
import json
from typing import Literal, Optional

import pandas as pd
from pydantic import BaseModel

from lib.helper import determine_backfill_latest_timestamp
from lib.db.queue import Queue
from lib.helper import track_performance
from lib.datetime_utils import generate_current_datetime_str
from lib.log.logger import get_logger
from lib.utils import filter_posts_df
from services.ml_inference.config import InferenceConfig, QueueInferenceType
from services.ml_inference.models import ClassificationSessionModel
from services.ml_inference.models import PostToLabelModel


logger = get_logger(__name__)


@track_performance
def get_posts_to_classify(
    inference_type: QueueInferenceType,
    timestamp: Optional[str] = None,
    previous_run_metadata: Optional[dict] = None,
    columns: Optional[list[str]] = None,
    max_records_per_run: Optional[int] = None,
) -> list[PostToLabelModel]:
    """Retrieves posts from the appropriate queue for classification.

    This is the single entry point for getting data for inference. All inference modules
    should use this function to get their data. The function handles queue selection,
    post filtering, and data formatting.

    Args:
        inference_type: Type of inference to run (QueueInferenceType).
            Maps to specific queue names:
            - "perspective_api" -> "input_ml_inference_perspective_api"
            - "sociopolitical" -> "input_ml_inference_sociopolitical"
            - "ime" -> "input_ml_inference_ime"
            - "valence_classifier" -> "input_ml_inference_valence_classifier"
            - "intergroup" -> "input_ml_inference_intergroup"
        timestamp (Optional[str]): Optional timestamp in YYYY-MM-DD-HH:MM:SS format to
            override latest inference timestamp for filtering posts.
        previous_run_metadata (Optional[dict]): Metadata from previous run containing:
            - metadata (str): JSON string with:
                - latest_id_classified (Optional[int]): ID of last processed post
                - inference_timestamp (Optional[str]): Timestamp of last inference
        columns (Optional[list[str]]): List of columns to return in output.
            Defaults to ["uri", "text", "preprocessing_timestamp", "batch_id", "batch_metadata"].
        max_records_per_run (Optional[int]): Maximum number of records to process for this run.
            If provided, only complete batches will be returned up to the limit (see
            cap_max_records_for_run). This limit is applied before claiming batches.

    Returns:
        list[PostToLabelModel]: List of posts to classify as strongly-typed models.
            Posts are filtered to remove:
            - Duplicates (based on uri)
            - Invalid text (missing, empty, or too short)
            - Missing timestamps
            If required columns are missing, raises ValueError.

    Control Flow:
        1. Maps inference type to appropriate queue name
        2. Creates Queue instance for specified inference type
        3. Extracts metadata from previous run if provided
        4. Loads posts from queue with filters:
            - After latest_id_classified (if provided)
            - After latest_inference_timestamp or override timestamp
            - With status="pending"
        5. If no posts found, returns empty list
        6. Creates DataFrame from posts
        7. Drops duplicate URIs
        8. Filters posts using filter_posts_df()
        9. Ensures all requested columns exist (adds missing ones with None)
        10. Optionally caps records (complete batches only) if max_records_per_run is provided
        11. Claims selected batch IDs (pending -> processing) to avoid concurrent duplication
        12. Returns only posts from successfully claimed batches as PostToLabelModel instances
    """
    # Map inference types to queue names
    queue_mapping = {
        "perspective_api": "input_ml_inference_perspective_api",
        "sociopolitical": "input_ml_inference_sociopolitical",
        "ime": "input_ml_inference_ime",
        "valence_classifier": "input_ml_inference_valence_classifier",
        "intergroup": "input_ml_inference_intergroup",
    }

    if inference_type not in queue_mapping:
        raise ValueError(f"Invalid inference type: {inference_type}")

    queue = Queue(queue_name=queue_mapping[inference_type])

    if previous_run_metadata is not None:
        latest_job_metadata = json.loads(previous_run_metadata.get("metadata", "{}"))
        latest_id_classified = latest_job_metadata.get("latest_id_classified", None)
        latest_inference_timestamp = latest_job_metadata.get(
            "inference_timestamp", None
        )
    else:
        latest_id_classified = None
        latest_inference_timestamp = None

    if timestamp is not None:
        logger.info(
            f"Using backfill timestamp {timestamp} instead of latest inference timestamp: {latest_inference_timestamp}"
        )
        latest_inference_timestamp = timestamp

    latest_payloads: list[dict] = queue.load_dict_items_from_queue(
        limit=None,
        min_id=latest_id_classified,
        min_timestamp=latest_inference_timestamp,
        status="pending",
    )
    logger.info(f"Loaded {len(latest_payloads)} posts to classify.")
    logger.info(f"Getting posts to classify for inference type {inference_type}.")
    logger.info(f"Latest inference timestamp: {latest_inference_timestamp}")

    if not latest_payloads:
        logger.info("No posts to classify.")
        return []

    if columns is None:
        columns = [
            "uri",
            "text",
            "preprocessing_timestamp",
            "batch_id",
            "batch_metadata",
        ]

    # Create DataFrame with all columns from the payloads
    posts_df = pd.DataFrame(latest_payloads)

    # Drop duplicates before any other processing
    if "uri" in posts_df.columns:
        posts_df = posts_df.drop_duplicates(subset=["uri"])
    logger.info(f"After dropping duplicates, {len(posts_df)} posts remain.")

    # Filter posts if needed
    posts_df = filter_posts_df(posts_df)
    logger.info(f"After filtering, {len(posts_df)} posts remain.")

    # Verify required columns exist.
    missing_columns = [col for col in columns if col not in posts_df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Select only requested columns
    dicts = posts_df[columns].to_dict(orient="records")  # type: ignore[arg-type]

    posts_to_classify = [PostToLabelModel(**row) for row in dicts]

    # Apply max_records_per_run (complete batches only) before claiming.
    if max_records_per_run is not None:
        posts_to_classify = cap_max_records_for_run(
            posts_to_classify=posts_to_classify,
            max_records_per_run=max_records_per_run,
        )

    if not posts_to_classify:
        logger.info("No posts to classify after filtering/limiting.")
        return []

    selected_batch_ids = list({post.batch_id for post in posts_to_classify})
    logger.info(
        f"Attempting to claim {len(selected_batch_ids)} batch IDs for inference type {inference_type}."
    )

    claimed_items = queue.batch_claim_items_by_ids(ids=selected_batch_ids)
    claimed_batch_ids = {item.id for item in claimed_items if item.id is not None}

    final_posts = [p for p in posts_to_classify if p.batch_id in claimed_batch_ids]
    if not final_posts and selected_batch_ids:
        logger.info(
            "No posts to classify after claiming batches. This can happen when other workers "
            "claim the same pending batches concurrently."
        )
    else:
        logger.info(
            f"Claimed {len(claimed_batch_ids)} batches and returning {len(final_posts)} posts."
        )

    return final_posts


def cap_max_records_for_run(
    posts_to_classify: list[PostToLabelModel],
    max_records_per_run: int,
) -> list[PostToLabelModel]:
    """Caps the number of records to process, ensuring only complete batches are included.

    This function groups posts by batch_id and only includes complete batches. It iterates
    through batches in order and adds all posts from each batch until adding the next batch
    would exceed max_records_per_run. This ensures that downstream processing that clears
    completed posts by batch_id doesn't accidentally delete partially processed batches.

    Args:
        posts_to_classify: List of posts to classify
        max_records_per_run: Maximum number of records to process. The actual number may be
            slightly less if needed to include only complete batches.

    Returns:
        List of posts to classify, containing only complete batches up to the limit.
    """
    if max_records_per_run < 0:
        raise ValueError("max_records_per_run must be >= 0")

    original_count = len(posts_to_classify)

    # If no limit or limit is greater than or equal to all posts, return all
    if max_records_per_run == 0:
        return []
    if max_records_per_run >= original_count:
        return posts_to_classify

    # Group posts by batch_id, preserving order of first appearance
    batches: dict[int, list[PostToLabelModel]] = defaultdict(list)
    batch_order: list[int] = []

    for post in posts_to_classify:
        batch_id = post.batch_id
        if batch_id not in batches:
            batch_order.append(batch_id)
        batches[batch_id].append(post)

    # Iterate through batches and add complete batches until we'd exceed the limit
    capped_posts: list[PostToLabelModel] = []
    current_count = 0

    for batch_id in batch_order:
        batch_posts: list[PostToLabelModel] = batches[batch_id]
        batch_size: int = len(batch_posts)

        # Check if adding this batch would exceed the limit
        if (current_count + batch_size) > max_records_per_run:
            # Don't add this batch - we'd exceed the limit
            break

        # Add all posts from this batch
        capped_posts.extend(batch_posts)
        current_count += batch_size

    if len(capped_posts) < original_count:
        num_batches_included = len(set(post.batch_id for post in capped_posts))
        logger.info(
            f"Limited posts from {original_count} to {len(capped_posts)} "
            f"(max_records_per_run={max_records_per_run}, "
            f"included {num_batches_included} complete batches)"
        )

    return capped_posts


@track_performance
def orchestrate_classification(
    config: InferenceConfig,
    backfill_period: Optional[Literal["days", "hours"]] = None,
    backfill_duration: Optional[int] = None,
    max_records_per_run: Optional[int] = None,
    run_classification: bool = True,
    previous_run_metadata: Optional[dict] = None,
    event: Optional[dict] = None,
) -> ClassificationSessionModel:
    """Orchestrates classification of latest posts using the provided configuration.

    This is the shared orchestration logic for all inference types. It handles:
    - Backfill timestamp calculation
    - Post retrieval from queues
    - Classification execution
    - Session metadata creation

    Args:
        config: InferenceConfig instance defining the inference type behavior
        backfill_period: Time unit for backfilling ("days" or "hours")
        backfill_duration: Number of time units to backfill
        max_records_per_run: Maximum number of records to process in this run. If None, processes all available records.
        run_classification: Whether to run classification (True) or just export cached results (False)
        previous_run_metadata: Metadata from previous runs to avoid duplicates
        event: Original event/payload for traceability and strategy-specific config

    Returns:
        ClassificationSessionModel: Labeling session summary containing:
            - inference_type: The inference type identifier
            - inference_timestamp: Execution timestamp
            - total_classified_posts: Number of posts processed
            - event: Original event payload
            - inference_metadata: Classification results metadata
    """
    if run_classification:
        backfill_latest_timestamp: Optional[str] = determine_backfill_latest_timestamp(
            backfill_duration=backfill_duration,
            backfill_period=backfill_period,
        )
        posts_to_classify: list[PostToLabelModel] = get_posts_to_classify(
            inference_type=config.queue_inference_type,
            timestamp=backfill_latest_timestamp,
            previous_run_metadata=previous_run_metadata,
            max_records_per_run=max_records_per_run,
        )

        logger.info(config.get_log_message(len(posts_to_classify)))

        if len(posts_to_classify) == 0:
            logger.warning(config.empty_result_message)
            return ClassificationSessionModel(
                inference_type=config.inference_type,
                inference_timestamp=generate_current_datetime_str(),
                total_classified_posts=0,
                event=event,
                inference_metadata={},
            )

        # Extract strategy-specific kwargs from event
        classification_kwargs = config.extract_classification_kwargs(event)

        # Execute classification
        classification_metadata = config.classification_func(
            posts=posts_to_classify,
            **classification_kwargs,
        )
        # Convert Pydantic models to dict for ClassificationSessionModel
        # NOTE: should deprecate this once we've fully migrated to using
        # pydantic models for all classification results.
        if isinstance(classification_metadata, BaseModel):
            classification_metadata = classification_metadata.model_dump()
        total_classified = len(posts_to_classify)
    else:
        logger.info("Skipping classification and exporting cached results...")
        classification_metadata = {}
        total_classified = 0

    timestamp = generate_current_datetime_str()
    return ClassificationSessionModel(
        inference_type=config.inference_type,
        inference_timestamp=timestamp,
        total_classified_posts=total_classified,
        event=event,
        inference_metadata=classification_metadata,
    )
