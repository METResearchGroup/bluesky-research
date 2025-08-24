"""Analytics analysis framework.

This package provides simple, organized analysis classes for analytics operations.
The focus is on direct method execution and shared utilities rather than complex
pipeline orchestration.
"""

from services.calculate_analytics.study_analytics.shared.pipelines.base import (
    BaseAnalyzer,
)
from services.calculate_analytics.study_analytics.shared.pipelines.feed_analysis import (
    FeedAnalyzer,
)
from services.calculate_analytics.study_analytics.shared.pipelines.engagement_analysis import (
    EngagementAnalyzer,
)
from services.calculate_analytics.study_analytics.shared.pipelines.weekly_thresholds import (
    WeeklyThresholdsAnalyzer,
)

__all__ = [
    "BaseAnalyzer",
    "FeedAnalyzer",
    "EngagementAnalyzer",
    "WeeklyThresholdsAnalyzer",
]

__version__ = "1.0.0"
__author__ = "Analytics Team"
__description__ = "Simple pipeline framework for consistent analytics patterns"
