"""
Data Loading Module for Topic Modeling Pipeline

This module provides the data loading infrastructure for the topic modeling pipeline,
including abstract interfaces and concrete implementations for different data sources.

Author: AI Agent implementing MET-44
Date: 2025-08-22
"""

from .base import DataLoader, DataLoadingError, ValidationError
from .config import DataLoaderConfig
from .local import LocalDataLoader

__all__ = [
    "DataLoader",
    "DataLoadingError",
    "ValidationError",
    "DataLoaderConfig",
    "LocalDataLoader",
]
