"""Compatibility module for test_data_filter.py.

This module provides the old export_data.py interface for backward compatibility
with test_data_filter.py. It uses the new CachePathManager internally.
"""

from services.sync.stream.cache_management import CachePathManager
from services.sync.stream.types import Operation, RecordType, FollowStatus

# Create a path manager instance to access path maps
_path_manager = CachePathManager()

# Compatibility: raw_sync_relative_path_map
# This maps to study_user_activity_relative_path_map
raw_sync_relative_path_map = _path_manager.study_user_activity_relative_path_map

# Compatibility: raw_sync_root_local_path
# This maps to study_user_activity_root_local_path
raw_sync_root_local_path = _path_manager.study_user_activity_root_local_path

