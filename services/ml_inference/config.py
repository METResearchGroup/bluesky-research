"""Configuration for inference types."""

from dataclasses import dataclass
from typing import Callable, Literal, Optional, TypeAlias

# Type alias for queue inference types - used across ML inference modules
QueueInferenceType: TypeAlias = Literal[
    "sociopolitical",
    "perspective_api",
    "ime",
    "valence_classifier",
    "intergroup",
]


@dataclass
class InferenceConfig:
    """Configuration for an inference type.

    This dataclass encapsulates all the variation between different inference
    types, allowing the common orchestration logic to be shared while keeping
    type-specific behavior configurable.

    Attributes:
        inference_type: The inference type identifier (e.g., "perspective_api", "ime")
        queue_inference_type: The inference type string used for queue operations.
            Usually matches inference_type, but can differ if needed for backward compatibility
        classification_func: The function to call for batch classification
        log_message_template: Template for log message when classification starts.
            Use {count} placeholder for number of posts.
        empty_result_message: Log message when no posts are found to classify
    """

    inference_type: str
    queue_inference_type: QueueInferenceType
    classification_func: Callable
    log_message_template: str = "Classifying {count} posts..."
    empty_result_message: str = "No posts to classify. Exiting..."

    def extract_classification_kwargs(self, event: Optional[dict]) -> dict:
        """Extract keyword arguments for classification function from event.

        Override this method in subclasses or use a custom function if the
        inference type needs to extract configuration from the event dict.

        Args:
            event: Optional event/payload dictionary

        Returns:
            Dictionary of keyword arguments to pass to classification_func
        """
        return {}

    def get_log_message(self, post_count: int) -> str:
        """Generate log message for classification start.

        Args:
            post_count: Number of posts to classify

        Returns:
            Formatted log message
        """
        return self.log_message_template.format(count=post_count)
