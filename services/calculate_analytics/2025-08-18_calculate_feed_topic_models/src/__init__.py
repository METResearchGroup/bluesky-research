"""
Feed-Level Topic Analysis for Bluesky Research

This package provides the data loading infrastructure and pipeline integration
for topic modeling analysis of Bluesky feed data.

Author: AI Agent implementing MET-44
Date: 2025-08-22
"""

__version__ = "0.1.0"
__author__ = "AI Agent implementing MET-44"

from .data_loading.base import DataLoader, DataLoadingError, ValidationError
from .data_loading.config import DataLoaderConfig
from .data_loading.local import LocalDataLoader
from .pipeline.topic_modeling import TopicModelingPipeline

__all__ = [
    "DataLoader",
    "DataLoadingError",
    "ValidationError",
    "DataLoaderConfig",
    "LocalDataLoader",
    "TopicModelingPipeline",
]
