"""
Topic Modeling Module for Bluesky Research

This module provides tools for topic modeling and analysis, including:
- BERTopic wrapper with YAML configuration
- Topic quality monitoring and coherence metrics
- GPU optimization for large-scale processing
- Generic pipeline for any text DataFrame

Author: AI Agent implementing MET-34
Date: 2025-01-20
"""

from .bertopic_wrapper import BERTopicWrapper

__all__ = ["BERTopicWrapper"]
__version__ = "0.1.0"
