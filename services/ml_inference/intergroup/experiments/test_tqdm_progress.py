"""Experimental script to demonstrate tqdm progress bars for intergroup batch classification.

This script mocks the I/O operations (LLM calls, queue operations, S3 writes) to provide
a realistic demonstration of how tqdm progress bars would work in the actual classification flow.
"""

from __future__ import annotations

import random
import sys
import time
from typing import Optional

from tqdm import tqdm

from lib.batching_utils import create_batches, update_batching_progress
from lib.log.logger import get_logger
from services.ml_inference.intergroup.constants import DEFAULT_BATCH_SIZE
from services.ml_inference.intergroup.models import IntergroupLabelModel
from services.ml_inference.models import (
    BatchClassificationMetadataModel,
    PostToLabelModel,
)

logger = get_logger(__name__)


class MockIntergroupClassifier:
    """Mock classifier that simulates LLM API calls with realistic delays."""

    def __init__(self, failure_rate: float = 0.1):
        """Initialize mock classifier.

        Args:
            failure_rate: Probability that a batch will fail (default: 0.1 = 10%)
        """
        self.failure_rate = failure_rate

    def classify_batch(self, batch: list[PostToLabelModel]) -> list[IntergroupLabelModel]:
        """Simulate batch classification with realistic delays."""
        # Simulate LLM API call delay (0.5-2 seconds per batch)
        time.sleep(random.uniform(0.5, 2.0))

        # Simulate occasional failures (e.g., rate limits, API errors)
        if random.random() < self.failure_rate:
            # Generate failed labels for entire batch
            return [
                IntergroupLabelModel(
                    uri=post.uri,
                    text=post.text,
                    preprocessing_timestamp=post.preprocessing_timestamp,
                    was_successfully_labeled=False,
                    reason="Mock LLM API error (rate limit exceeded)",
                    label=-1,
                )
                for post in batch
            ]

        # Generate successful labels
        return [
            IntergroupLabelModel(
                uri=post.uri,
                text=post.text,
                preprocessing_timestamp=post.preprocessing_timestamp,
                was_successfully_labeled=True,
                reason=None,
                label=random.randint(0, 1),  # Mock binary label
            )
            for post in batch
        ]


def _mock_return_failed_labels_to_queue(failed_labels: list) -> None:
    """Mock queue operation - simulates delay of re-inserting failed labels."""
    time.sleep(0.1 * len(failed_labels) / 10)  # Scale delay with count


def _mock_write_posts_to_cache(posts: list) -> None:
    """Mock S3 write operation - simulates delay of writing successful labels."""
    time.sleep(0.2 * len(posts) / 10)  # Scale delay with count


def _split_labels_into_successful_and_failed(
    labels: list[IntergroupLabelModel],
) -> tuple[list[IntergroupLabelModel], list[IntergroupLabelModel]]:
    """Split labels into successful and failed."""
    successful = [l for l in labels if l.was_successfully_labeled]
    failed = [l for l in labels if not l.was_successfully_labeled]
    return successful, failed


def _attach_batch_id_to_labels(
    labels: list[IntergroupLabelModel],
    uri_to_batch_id: dict[str, int],
) -> list[dict]:
    """Attach batch_id to labels (mocked - returns dicts)."""
    return [
        {**label.model_dump(), "batch_id": uri_to_batch_id[label.uri]}
        for label in labels
    ]


def run_mock_batch_classification(
    posts: list[PostToLabelModel],
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
    failure_rate: float = 0.1,
) -> BatchClassificationMetadataModel:
    """Run mock batch classification with tqdm progress bars.

    This demonstrates how tqdm would be integrated into the actual classification flow.
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

    classifier = MockIntergroupClassifier(failure_rate=failure_rate)

    # Create tqdm progress bar (conditional on interactive terminal)
    pbar = tqdm(
        batches,
        desc="Classifying batches",
        unit="batch",
        disable=not sys.stdout.isatty(),  # Disable in CI/CD/non-interactive
        file=sys.stderr,  # Write to stderr to avoid log conflicts
    )

    try:
        for i, batch in enumerate(pbar):
            # Keep existing logging (demonstrates both mechanisms working together)
            update_batching_progress(
                batch_index=i,
                batch_interval=10,
                total_batches=total_batches,
                logger=logger,
            )

            uri_to_batch_id = {p.uri: p.batch_id for p in batch}

            # Simulate LLM classification
            label_models: list[IntergroupLabelModel] = classifier.classify_batch(batch)

            # Split into successful/failed
            successful_labels, failed_labels = _split_labels_into_successful_and_failed(
                labels=label_models
            )

            # Handle failed labels (mocked queue operation)
            if len(failed_labels) > 0:
                failed_label_dicts = _attach_batch_id_to_labels(
                    failed_labels, uri_to_batch_id
                )
                logger.warning(
                    f"Failed to label {len(failed_label_dicts)} posts. Re-inserting into queue."
                )
                _mock_return_failed_labels_to_queue(failed_label_dicts)
                total_failed += len(failed_label_dicts)

            # Handle successful labels (mocked S3 write)
            if len(successful_labels) > 0:
                successful_label_dicts = _attach_batch_id_to_labels(
                    successful_labels, uri_to_batch_id
                )
                logger.info(f"Successfully labeled {len(successful_label_dicts)} posts.")
                _mock_write_posts_to_cache(successful_label_dicts)
                total_successful += len(successful_label_dicts)

            # Update progress bar with live counts
            postfix = {
                "successful": total_successful,
                "failed": total_failed,
            }
            if (total_successful + total_failed) > 0:
                success_rate = (total_successful / (total_successful + total_failed)) * 100
                postfix["success_rate"] = f"{success_rate:.1f}%"
            pbar.set_postfix(postfix)

    finally:
        pbar.close()

    return BatchClassificationMetadataModel(
        total_batches=total_batches,
        total_posts_successfully_labeled=total_successful,
        total_posts_failed_to_label=total_failed,
    )


def create_mock_posts(count: int = 200) -> list[PostToLabelModel]:
    """Create mock posts for testing."""
    sample_texts = [
        "This is a discussion about politics and social issues.",
        "Two groups are debating different perspectives on climate change.",
        "A simple post about the weather today.",
        "Intergroup dynamics are complex and require careful analysis.",
        "Sports teams competing in a championship match.",
    ]

    return [
        PostToLabelModel(
            uri=f"at://did:plc:mock{i}",
            text=random.choice(sample_texts),
            preprocessing_timestamp="2024-01-01T00:00:00Z",
            batch_id=i // DEFAULT_BATCH_SIZE,
            batch_metadata='{"source": "mock"}',
        )
        for i in range(count)
    ]


if __name__ == "__main__":
    print("=" * 70)
    print("Mock Intergroup Batch Classification with tqdm Progress Bars")
    print("=" * 70)
    print()

    # Create mock posts
    num_posts = 200
    print(f"Creating {num_posts} mock posts...")
    posts = create_mock_posts(count=num_posts)
    print(f"Created {len(posts)} posts, will create {len(create_batches(posts, DEFAULT_BATCH_SIZE))} batches\n")

    # Run classification with progress bars
    result = run_mock_batch_classification(
        posts=posts,
        batch_size=DEFAULT_BATCH_SIZE,
        failure_rate=0.1,  # 10% failure rate for realism
    )

    print()
    print("=" * 70)
    print("Classification Complete!")
    print("=" * 70)
    print(f"Total batches processed: {result.total_batches}")
    print(f"Successfully labeled: {result.total_posts_successfully_labeled}")
    print(f"Failed to label: {result.total_posts_failed_to_label}")
    print()