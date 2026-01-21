"""Experimental script to demonstrate tqdm progress bars for intergroup batch classification.

This script exercises the real `run_batch_classification()` implementation and mocks only
the true side-effect boundaries:
- LLM classification (`IntergroupClassifier.classify_batch`)
- Queue/cache I/O (`write_posts_to_cache`, `return_failed_labels_to_input_queue`)

That makes it a more convincing end-to-end check that the tqdm bar in
`services/ml_inference/intergroup/batch_classifier.py` is wired correctly.
"""

from __future__ import annotations

import random
import sys
import time
from unittest.mock import patch

from lib.batching_utils import create_batches
from services.ml_inference.intergroup.constants import DEFAULT_BATCH_SIZE
from services.ml_inference.intergroup.models import IntergroupLabelModel
from services.ml_inference.models import PostToLabelModel


class FakeIntergroupClassifier:
    """Fake classifier that simulates LLM batch calls with realistic delays."""

    def __init__(self, failure_rate: float = 0.1):
        """Initialize fake classifier.

        Args:
            failure_rate: Probability that a batch will fail (default: 0.1 = 10%)
        """
        self.failure_rate = failure_rate

    def classify_batch(self, batch: list[PostToLabelModel]) -> list[IntergroupLabelModel]:
        """Simulate LLM batch classification with realistic delays."""
        # Simulate LLM API call delay (0.5-2 seconds per batch)
        time.sleep(random.uniform(0.5, 2.0))

        # Simulate occasional failures (e.g., rate limits, transient API errors)
        if random.random() < self.failure_rate:
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


def _mock_write_posts_to_cache(*, inference_type, posts, **_kwargs) -> None:
    """Mock cache/queue write. Simulates latency, but performs no I/O."""
    _ = inference_type
    time.sleep(0.02 + 0.001 * len(posts))


def _mock_return_failed_labels_to_input_queue(
    *, inference_type, failed_label_models, **_kwargs
) -> None:
    """Mock failure requeue. Simulates latency, but performs no I/O."""
    _ = inference_type
    time.sleep(0.01 + 0.001 * len(failed_label_models))


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
    # Make runs more repeatable while still "realistic".
    random.seed(0)

    # Create mock posts
    num_posts = 200
    posts = create_mock_posts(count=num_posts)
    total_batches = len(create_batches(posts, DEFAULT_BATCH_SIZE))
    print(f"Prepared {len(posts)} posts ({total_batches} batches @ batch_size={DEFAULT_BATCH_SIZE}).")

    fake_classifier = FakeIntergroupClassifier(failure_rate=0.15)

    with patch(
        "services.ml_inference.intergroup.batch_classifier.IntergroupClassifier",
        return_value=fake_classifier,
    ), patch(
        "services.ml_inference.intergroup.batch_classifier.write_posts_to_cache",
        side_effect=_mock_write_posts_to_cache,
    ), patch(
        "services.ml_inference.intergroup.batch_classifier.return_failed_labels_to_input_queue",
        side_effect=_mock_return_failed_labels_to_input_queue,
    ), patch.object(sys.stdout, "isatty", return_value=True):
        from services.ml_inference.intergroup.batch_classifier import run_batch_classification

        result = run_batch_classification(posts=posts, batch_size=DEFAULT_BATCH_SIZE)

    print(result.model_dump())