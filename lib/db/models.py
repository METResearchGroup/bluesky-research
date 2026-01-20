"""Database models and type definitions."""

from enum import Enum


class StorageTier(str, Enum):
    """Storage tier for data partitioning.
    
    Data is partitioned into "cache" (older data) and "active" (recent data)
    based on lookback days configuration.
    """
    CACHE = "cache"
    ACTIVE = "active"
