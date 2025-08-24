"""Shared pipeline framework for analytics system.

This module provides abstract base classes and concrete implementations
for standardizing how analytics pipelines work across the system.
"""

from .base import (
    BaseResearchPipeline,
    BaseFeedAnalysisPipeline,
    PipelineState,
    PipelineResult,
)
from .feed_analysis import FeedAnalysisPipeline
from .weekly_thresholds import WeeklyThresholdsPipeline
from .engagement_analysis import EngagementAnalysisPipeline
from .orchestration import (
    PipelineOrchestrator,
    PipelineExecutionResult,
    PipelineError,
)

__all__ = [
    # Base Classes
    "BaseResearchPipeline",
    "BaseFeedAnalysisPipeline",
    "PipelineState",
    "PipelineResult",
    # Concrete Implementations
    "FeedAnalysisPipeline",
    "WeeklyThresholdsPipeline",
    "EngagementAnalysisPipeline",
    # Orchestration
    "PipelineOrchestrator",
    "PipelineExecutionResult",
    "PipelineError",
]
