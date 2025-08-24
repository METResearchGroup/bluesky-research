"""Simple pipeline framework for consistent analytics patterns.

This module provides abstract base classes and concrete implementations
for standardizing analytics workflows. The focus is on simple, direct
execution rather than complex orchestration.
"""

from .base import (
    BaseResearchPipeline,
    BaseFeedAnalysisPipeline,
    PipelineState,
    PipelineResult,
    PipelineError,
)

from .feed_analysis import FeedAnalysisPipeline
from .weekly_thresholds import WeeklyThresholdsPipeline
from .engagement_analysis import EngagementAnalysisPipeline

__all__ = [
    # Base classes
    "BaseResearchPipeline",
    "BaseFeedAnalysisPipeline",
    # Pipeline states and results
    "PipelineState",
    "PipelineResult",
    "PipelineError",
    # Concrete implementations
    "FeedAnalysisPipeline",
    "WeeklyThresholdsPipeline",
    "EngagementAnalysisPipeline",
]

__version__ = "1.0.0"
__author__ = "Analytics Team"
__description__ = "Simple pipeline framework for consistent analytics patterns"
