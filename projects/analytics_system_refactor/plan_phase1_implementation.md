# Phase 1 Implementation Plan: Extract Shared Data Loading & Processing

> **Linear Issue**: [MET-39](https://linear.app/metresearch/issue/MET-39/phase-1-extract-shared-data-loading-and-processing)  
> **Status**: Implementation Planning  
> **Priority**: High  
> **Effort Estimate**: 1 week (1 developer)  
> **Dependencies**: None - this is the starting point

## Overview

This document outlines the detailed implementation plan for Phase 1 of the analytics system refactor. The goal is to extract shared data loading, processing, and configuration management modules from existing monolithic scripts to eliminate code duplication and improve maintainability.

## Current State Analysis

### Problems Identified
1. **Code Duplication**: Similar data loading logic repeated across multiple files
2. **Hardcoded Configuration**: Study dates, feature lists, and file paths scattered throughout code
3. **Monolithic Scripts**: Single large files (500+ lines) mixing multiple concerns
4. **Inconsistent Patterns**: Different files follow different architectural approaches
5. **Limited Reusability**: Functions cannot be easily imported and reused across scripts

### Key Files to Refactor
- `services/calculate_analytics/study_analytics/load_data/load_data.py` (349 lines)
- `services/calculate_analytics/study_analytics/calculate_analytics/feed_analytics.py` (540 lines)
- `services/calculate_analytics/study_analytics/calculate_analytics/calculate_weekly_thresholds_per_user.py` (517 lines)
- `services/calculate_analytics/study_analytics/load_data/load_labels.py`
- `services/calculate_analytics/study_analytics/load_data/load_feeds.py`

## Implementation Strategy

### Phase 1A: Foundation & Configuration (Days 1-2)
1. **Create shared configuration structure**
2. **Implement configuration loading utilities**
3. **Extract all hardcoded constants**

### Phase 1B: Data Loading Layer (Days 3-4)
1. **Create shared data loading modules**
2. **Implement unified data loading interfaces**
3. **Add error handling and validation**

### Phase 1C: Processing Logic Extraction (Days 5-6)
1. **Extract feature calculation logic**
2. **Extract threshold calculation logic**
3. **Create reusable processing functions**

### Phase 1D: Integration & Testing (Day 7)
1. **Update existing scripts to use shared modules**
2. **Verify all functionality preserved**
3. **Run comprehensive tests**

## Detailed Implementation Plan

### 1. Create Shared Configuration Structure

#### 1.1 Directory Structure
```
services/calculate_analytics/study_analytics/shared/
├── __init__.py
├── config/
│   ├── __init__.py
│   ├── loader.py              # Configuration loading utilities
│   ├── validator.py           # Configuration validation
│   ├── analytics.yaml         # Main analytics configuration
│   ├── studies/
│   │   ├── wave1.yaml        # Wave 1 study configuration
│   │   └── wave2.yaml        # Wave 2 study configuration
│   └── features/
│       ├── toxicity.yaml      # Toxicity feature configuration
│       ├── political.yaml     # Political feature configuration
│       └── ime.yaml           # IME feature configuration
```

#### 1.2 Configuration Files

**`analytics.yaml`** - Main configuration
```yaml
# Main analytics configuration
defaults:
  lookback_days: 7
  label_threshold: 0.5
  exclude_partition_dates: ["2024-10-08"]

studies:
  wave1:
    start_date: "2024-08-19"
    end_date: "2024-10-06"
    week_start_dates: ["2024-08-19", "2024-08-26", "2024-09-02", "2024-09-09", "2024-09-16", "2024-09-23", "2024-09-30", "2024-10-06"]
    week_end_dates: ["2024-08-25", "2024-09-01", "2024-09-08", "2024-09-15", "2024-09-22", "2024-09-29", "2024-10-05", "2024-10-06"]
  
  wave2:
    start_date: "2024-10-07"
    end_date: "2024-12-01"
    week_start_dates: ["2024-10-07", "2024-10-14", "2024-10-21", "2024-10-28", "2024-11-04", "2024-11-11", "2024-11-18", "2024-11-25"]
    week_end_dates: ["2024-10-13", "2024-10-20", "2024-10-27", "2024-11-03", "2024-11-10", "2024-11-17", "2024-11-24", "2024-12-01"]

features:
  toxicity:
    columns: ["prob_toxic", "prob_severe_toxic", "prob_identity_attack", "prob_insult", "prob_profanity", "prob_threat"]
    threshold: 0.5
  
  ime:
    columns: ["prob_affinity", "prob_compassion", "prob_constructive", "prob_curiosity", "prob_nuance", "prob_personal_story", "prob_reasoning", "prob_respect"]
    threshold: 0.5
  
  political:
    columns: ["is_sociopolitical", "political_ideology_label"]
    threshold: 0.5
  
  content:
    columns: ["prob_alienation", "prob_fearmongering", "prob_generalization", "prob_moral_outrage", "prob_scapegoating", "prob_sexually_explicit", "prob_flirtation", "prob_spam", "prob_emotion", "prob_intergroup", "prob_moral", "prob_other"]
    threshold: 0.5

data_loading:
  default_columns: ["uri", "text", "preprocessing_timestamp", "author_did", "author_handle"]
  invalid_author_sources:
    - "services/preprocess_raw_data/classify_nsfw_content/dids_to_exclude.csv"
    - "services/preprocess_raw_data/classify_nsfw_content/manual_excludelist.py"
```

**`wave1.yaml`** - Wave 1 specific configuration
```yaml
# Wave 1 study configuration
study_id: "wave1"
start_date: "2024-08-19"
end_date: "2024-10-06"
weeks:
  - number: 1
    start: "2024-08-19"
    end: "2024-08-25"
  - number: 2
    start: "2024-08-26"
    end: "2024-09-01"
  # ... continue for all 8 weeks
```

#### 1.3 Configuration Loading Utilities

**`shared/config/loader.py`**
```python
"""Configuration loading and management utilities."""

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from pydantic import BaseModel, ValidationError

from lib.log.logger import get_logger

logger = get_logger(__file__)


class ConfigLoader:
    """Loads and validates configuration from YAML files."""
    
    def __init__(self, config_dir: Optional[str] = None):
        if config_dir is None:
            config_dir = os.path.join(
                os.path.dirname(__file__), 
                "..", 
                "config"
            )
        self.config_dir = Path(config_dir)
        self._config_cache: Dict[str, Any] = {}
    
    def load_config(self, config_name: str) -> Dict[str, Any]:
        """Load a configuration file by name."""
        if config_name in self._config_cache:
            return self._config_cache[config_name]
        
        config_path = self.config_dir / f"{config_name}.yaml"
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            self._config_cache[config_name] = config
            logger.info(f"Loaded configuration: {config_name}")
            return config
        except Exception as e:
            logger.error(f"Failed to load configuration {config_name}: {e}")
            raise
    
    def get_study_config(self, study_id: str) -> Dict[str, Any]:
        """Get configuration for a specific study."""
        studies_config = self.load_config("analytics")
        return studies_config["studies"][study_id]
    
    def get_feature_config(self, feature_type: str) -> Dict[str, Any]:
        """Get configuration for a specific feature type."""
        analytics_config = self.load_config("analytics")
        return analytics_config["features"][feature_type]
    
    def get_default_config(self) -> Dict[str, Any]:
        """Get default configuration values."""
        analytics_config = self.load_config("analytics")
        return analytics_config["defaults"]


# Global configuration loader instance
config_loader = ConfigLoader()
```

**`shared/config/validator.py`**
```python
"""Configuration validation utilities."""

from typing import Any, Dict, List
from pydantic import BaseModel, Field, validator

from lib.log.logger import get_logger

logger = get_logger(__file__)


class StudyConfig(BaseModel):
    """Validation model for study configuration."""
    study_id: str
    start_date: str
    end_date: str
    weeks: List[Dict[str, Any]]
    
    @validator('start_date', 'end_date')
    def validate_date_format(cls, v):
        # Validate YYYY-MM-DD format
        from datetime import datetime
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError('Date must be in YYYY-MM-DD format')


class FeatureConfig(BaseModel):
    """Validation model for feature configuration."""
    columns: List[str]
    threshold: float = Field(ge=0.0, le=1.0)


class AnalyticsConfig(BaseModel):
    """Validation model for main analytics configuration."""
    defaults: Dict[str, Any]
    studies: Dict[str, StudyConfig]
    features: Dict[str, FeatureConfig]
    data_loading: Dict[str, Any]


def validate_config(config: Dict[str, Any]) -> bool:
    """Validate a configuration dictionary."""
    try:
        AnalyticsConfig(**config)
        logger.info("Configuration validation passed")
        return True
    except ValidationError as e:
        logger.error(f"Configuration validation failed: {e}")
        return False
```

### 2. Create Shared Data Loading Modules

#### 2.1 Directory Structure
```
services/calculate_analytics/study_analytics/shared/
├── data_loading/
│   ├── __init__.py
│   ├── base.py              # Base data loading classes
│   ├── post_loader.py       # Post loading logic
│   ├── label_loader.py      # Label loading logic
│   ├── feed_loader.py       # Feed loading logic
│   ├── user_loader.py       # User loading logic
│   └── filters.py           # Data filtering utilities
```

#### 2.2 Base Data Loading Classes

**`shared/data_loading/base.py`**
```python
"""Base classes for data loading operations."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from lib.log.logger import get_logger

logger = get_logger(__file__)


class DataLoader(ABC):
    """Abstract base class for data loaders."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = get_logger(self.__class__.__name__)
    
    @abstractmethod
    def load(self, **kwargs) -> pd.DataFrame:
        """Load data based on provided parameters."""
        pass
    
    def validate_data(self, data: pd.DataFrame) -> bool:
        """Validate loaded data meets requirements."""
        if data.empty:
            self.logger.warning("Loaded data is empty")
            return False
        return True
    
    def log_load_summary(self, data: pd.DataFrame, source: str):
        """Log summary of loaded data."""
        self.logger.info(f"Loaded {len(data)} records from {source}")
        if not data.empty:
            self.logger.info(f"Columns: {list(data.columns)}")
            self.logger.info(f"Date range: {data.get('date', pd.Series()).min()} to {data.get('date', pd.Series()).max()}")


class FilteredDataLoader(DataLoader):
    """Base class for data loaders that apply filters."""
    
    def __init__(self, config: Dict[str, Any], filters: Optional[List[str]] = None):
        super().__init__(config)
        self.filters = filters or []
    
    def apply_filters(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply configured filters to the data."""
        filtered_data = data.copy()
        
        for filter_name in self.filters:
            filter_func = getattr(self, f"filter_{filter_name}", None)
            if filter_func:
                filtered_data = filter_func(filtered_data)
                self.logger.info(f"Applied filter: {filter_name}")
        
        return filtered_data
    
    def filter_invalid_authors(self, data: pd.DataFrame) -> pd.DataFrame:
        """Filter out posts from invalid authors."""
        # Implementation will be specific to each loader
        return data
```

#### 2.3 Post Loading Module

**`shared/data_loading/post_loader.py`**
```python
"""Post data loading and processing utilities."""

from typing import Optional

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from services.backfill.posts_used_in_feeds.load_data import (
    calculate_start_end_date_for_lookback,
    load_preprocessed_posts_used_in_feeds_for_partition_date,
    default_num_days_lookback,
)
from services.preprocess_raw_data.classify_nsfw_content.manual_excludelist import (
    BSKY_HANDLES_TO_EXCLUDE,
)

from .base import FilteredDataLoader
from ..config.loader import config_loader


class PostLoader(FilteredDataLoader):
    """Loads and processes post data for analytics."""
    
    def __init__(self):
        config = config_loader.load_config("analytics")
        super().__init__(config, filters=["invalid_authors"])
        
        # Load invalid author lists
        self.invalid_dids = self._load_invalid_dids()
        self.invalid_handles = BSKY_HANDLES_TO_EXCLUDE
    
    def _load_invalid_dids(self) -> pd.DataFrame:
        """Load list of invalid DIDs to exclude."""
        from lib.constants import project_home_directory
        import os
        
        invalid_dids_path = os.path.join(
            project_home_directory,
            "services/preprocess_raw_data/classify_nsfw_content/dids_to_exclude.csv"
        )
        return pd.read_csv(invalid_dids_path)
    
    def load_filtered_posts(
        self,
        partition_date: str,
        lookback_start_date: str,
        lookback_end_date: str,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Load preprocessed posts with custom filters applied."""
        if columns is None:
            columns = self.config["data_loading"]["default_columns"]
        
        posts_df = load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date=partition_date,
            lookback_start_date=lookback_start_date,
            lookback_end_date=lookback_end_date,
            table_columns=columns,
        )
        
        self.logger.info(f"Loaded {len(posts_df)} posts for partition date {partition_date}")
        
        # Apply filters
        filtered_posts = self.apply_filters(posts_df)
        
        self.logger.info(f"Filtered to {len(filtered_posts)} posts after removing invalid authors")
        return filtered_posts
    
    def load_unfiltered_posts(
        self,
        partition_date: str,
        lookback_start_date: str,
        lookback_end_date: str
    ) -> pd.DataFrame:
        """Load preprocessed posts without custom filters."""
        posts_df = load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date=partition_date,
            lookback_start_date=lookback_start_date,
            lookback_end_date=lookback_end_date,
        )
        
        self.logger.info(f"Loaded {len(posts_df)} unfiltered posts for partition date {partition_date}")
        return posts_df
    
    def get_hydrated_posts(
        self,
        partition_date: str,
        posts_df: Optional[pd.DataFrame] = None,
        load_unfiltered_posts: bool = False
    ) -> pd.DataFrame:
        """Get hydrated posts with all features for a partition date."""
        lookback_start_date, lookback_end_date = calculate_start_end_date_for_lookback(
            partition_date=partition_date,
            num_days_lookback=default_num_days_lookback,
        )
        
        if posts_df is not None:
            self.logger.info(f"Using provided posts dataframe for partition date {partition_date}")
            df = posts_df
        else:
            if load_unfiltered_posts:
                df = self.load_unfiltered_posts(
                    partition_date, lookback_start_date, lookback_end_date
                )
            else:
                self.logger.info(f"Loading custom filtered preprocessed posts for partition date {partition_date}")
                df = self.load_filtered_posts(
                    partition_date, lookback_start_date, lookback_end_date
                )
        
        # Load labels and features
        hydrated_df = self._hydrate_posts_with_labels(df, partition_date)
        return hydrated_df
    
    def _hydrate_posts_with_labels(self, posts_df: pd.DataFrame, partition_date: str) -> pd.DataFrame:
        """Add label columns to posts dataframe."""
        # This will be implemented to load all the different label types
        # and merge them with the posts dataframe
        # Implementation details to be added based on existing logic
        return posts_df
    
    def filter_invalid_authors(self, data: pd.DataFrame) -> pd.DataFrame:
        """Filter out posts from invalid authors."""
        filtered_data = data[~data["author_did"].isin(self.invalid_dids["did"])]
        filtered_data = filtered_data[~filtered_data["author_handle"].isin(self.invalid_handles)]
        return filtered_data


# Global post loader instance
post_loader = PostLoader()
```

#### 2.4 Label Loading Module

**`shared/data_loading/label_loader.py`**
```python
"""Label loading and processing utilities."""

from typing import Dict, List, Optional

import pandas as pd

from lib.log.logger import get_logger
from services.calculate_analytics.study_analytics.load_data.load_labels import (
    get_ime_labels_for_posts,
    get_perspective_api_labels_for_posts,
    get_sociopolitical_labels_for_posts,
    get_valence_labels_for_posts,
)

from .base import DataLoader
from ..config.loader import config_loader


class LabelLoader(DataLoader):
    """Loads and processes various content classification labels."""
    
    def __init__(self):
        config = config_loader.load_config("analytics")
        super().__init__(config)
    
    def load_all_labels(
        self, 
        posts_df: pd.DataFrame, 
        partition_date: str
    ) -> pd.DataFrame:
        """Load all available labels for posts."""
        self.logger.info(f"Loading all labels for {len(posts_df)} posts")
        
        # Load each label type
        ime_labels = self.load_ime_labels(posts_df, partition_date)
        perspective_labels = self.load_perspective_labels(posts_df, partition_date)
        sociopolitical_labels = self.load_sociopolitical_labels(posts_df, partition_date)
        valence_labels = self.load_valence_labels(posts_df, partition_date)
        
        # Merge all labels
        labeled_posts = posts_df.copy()
        
        if not ime_labels.empty:
            labeled_posts = labeled_posts.merge(
                ime_labels, on="uri", how="left", suffixes=("", "_ime")
            )
        
        if not perspective_labels.empty:
            labeled_posts = labeled_posts.merge(
                perspective_labels, on="uri", how="left", suffixes=("", "_perspective")
            )
        
        if not sociopolitical_labels.empty:
            labeled_posts = labeled_posts.merge(
                sociopolitical_labels, on="uri", how="left", suffixes=("", "_sociopolitical")
            )
        
        if not valence_labels.empty:
            labeled_posts = labeled_posts.merge(
                valence_labels, on="uri", how="left", suffixes=("", "_valence")
            )
        
        self.logger.info(f"Successfully loaded labels for {len(labeled_posts)} posts")
        return labeled_posts
    
    def load_ime_labels(self, posts_df: pd.DataFrame, partition_date: str) -> pd.DataFrame:
        """Load IME labels for posts."""
        try:
            labels = get_ime_labels_for_posts(posts_df, partition_date)
            self.logger.info(f"Loaded IME labels for {len(labels)} posts")
            return labels
        except Exception as e:
            self.logger.error(f"Failed to load IME labels: {e}")
            return pd.DataFrame()
    
    def load_perspective_labels(self, posts_df: pd.DataFrame, partition_date: str) -> pd.DataFrame:
        """Load Perspective API labels for posts."""
        try:
            labels = get_perspective_api_labels_for_posts(posts_df, partition_date)
            self.logger.info(f"Loaded Perspective API labels for {len(labels)} posts")
            return labels
        except Exception as e:
            self.logger.error(f"Failed to load Perspective API labels: {e}")
            return pd.DataFrame()
    
    def load_sociopolitical_labels(self, posts_df: pd.DataFrame, partition_date: str) -> pd.DataFrame:
        """Load sociopolitical labels for posts."""
        try:
            labels = get_sociopolitical_labels_for_posts(posts_df, partition_date)
            self.logger.info(f"Loaded sociopolitical labels for {len(labels)} posts")
            return labels
        except Exception as e:
            self.logger.error(f"Failed to load sociopolitical labels: {e}")
            return pd.DataFrame()
    
    def load_valence_labels(self, posts_df: pd.DataFrame, partition_date: str) -> pd.DataFrame:
        """Load valence labels for posts."""
        try:
            labels = get_valence_labels_for_posts(posts_df, partition_date)
            self.logger.info(f"Loaded valence labels for {len(labels)} posts")
            return labels
        except Exception as e:
            self.logger.error(f"Failed to load valence labels: {e}")
            return pd.DataFrame()


# Global label loader instance
label_loader = LabelLoader()
```

### 3. Create Shared Processing Modules

#### 3.1 Directory Structure
```
services/calculate_analytics/study_analytics/shared/
├── processing/
│   ├── __init__.py
│   ├── base.py              # Base processing classes
│   ├── feature_calculator.py # Feature calculation logic
│   ├── threshold_calculator.py # Threshold calculation logic
│   └── aggregator.py        # Data aggregation utilities
```

#### 3.2 Feature Calculation Module

**`shared/processing/feature_calculator.py`**
```python
"""Feature calculation and processing utilities."""

from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from lib.log.logger import get_logger
from ..config.loader import config_loader


class FeatureCalculator:
    """Calculates various features from labeled post data."""
    
    def __init__(self):
        self.config = config_loader.load_config("analytics")
        self.logger = get_logger(__name__)
    
    def calculate_toxicity_features(self, posts_df: pd.DataFrame) -> Dict[str, float]:
        """Calculate toxicity-related features from posts."""
        feature_config = self.config["features"]["toxicity"]
        columns = feature_config["columns"]
        threshold = feature_config["threshold"]
        
        features = {}
        for col in columns:
            if col in posts_df.columns:
                avg_prob = posts_df[col].dropna().mean()
                features[f"avg_{col}"] = avg_prob if not pd.isna(avg_prob) else 0.0
            else:
                features[f"avg_{col}"] = 0.0
        
        return features
    
    def calculate_ime_features(self, posts_df: pd.DataFrame) -> Dict[str, float]:
        """Calculate IME-related features from posts."""
        feature_config = self.config["features"]["ime"]
        columns = feature_config["columns"]
        threshold = feature_config["threshold"]
        
        features = {}
        for col in columns:
            if col in posts_df.columns:
                avg_prob = posts_df[col].dropna().mean()
                features[f"avg_{col}"] = avg_prob if not pd.isna(avg_prob) else 0.0
            else:
                features[f"avg_{col}"] = 0.0
        
        return features
    
    def calculate_political_features(self, posts_df: pd.DataFrame) -> Dict[str, float]:
        """Calculate political-related features from posts."""
        feature_config = self.config["features"]["political"]
        columns = feature_config["columns"]
        threshold = feature_config["threshold"]
        
        features = {}
        total_rows = len(posts_df)
        
        if "is_sociopolitical" in posts_df.columns:
            avg_is_political = posts_df["is_sociopolitical"].dropna().mean()
            features["avg_is_political"] = avg_is_political if not pd.isna(avg_is_political) else 0.0
            features["avg_is_not_political"] = 1 - features["avg_is_political"]
        
        if "political_ideology_label" in posts_df.columns:
            for ideology in ["left", "right", "moderate"]:
                count = (posts_df["political_ideology_label"].fillna("").eq(ideology)).sum()
                features[f"avg_is_political_{ideology}"] = count / total_rows if total_rows > 0 else 0.0
        
        return features
    
    def calculate_content_features(self, posts_df: pd.DataFrame) -> Dict[str, float]:
        """Calculate content-related features from posts."""
        feature_config = self.config["features"]["content"]
        columns = feature_config["columns"]
        threshold = feature_config["threshold"]
        
        features = {}
        for col in columns:
            if col in posts_df.columns:
                avg_prob = posts_df[col].dropna().mean()
                features[f"avg_{col}"] = avg_prob if not pd.isna(avg_prob) else 0.0
            else:
                features[f"avg_{col}"] = 0.0
        
        return features
    
    def calculate_all_features(self, posts_df: pd.DataFrame) -> Dict[str, float]:
        """Calculate all available features from posts."""
        features = {}
        
        # Calculate each feature type
        features.update(self.calculate_toxicity_features(posts_df))
        features.update(self.calculate_ime_features(posts_df))
        features.update(self.calculate_political_features(posts_df))
        features.update(self.calculate_content_features(posts_df))
        
        return features


# Global feature calculator instance
feature_calculator = FeatureCalculator()
```

#### 3.3 Threshold Calculation Module

**`shared/processing/threshold_calculator.py`**
```python
"""Threshold calculation utilities for weekly analysis."""

from datetime import date
from typing import Dict, List, Optional

import pandas as pd

from lib.log.logger import get_logger
from ..config.loader import config_loader


class ThresholdCalculator:
    """Calculates weekly thresholds and assignments for users."""
    
    def __init__(self):
        self.config = config_loader.load_config("analytics")
        self.logger = get_logger(__name__)
    
    def map_date_to_static_week(self, partition_date: str, wave: int) -> int:
        """Map a partition date to a static week number based on user's wave."""
        study_config = self.config["studies"][f"wave{wave}"]
        week_end_dates = study_config["week_end_dates"]
        
        week = 1
        for end_date in week_end_dates:
            if partition_date <= end_date:
                break
            week += 1
        
        if week > len(week_end_dates):
            raise ValueError(f"Date {partition_date} is beyond wave {wave} study period")
        
        return week
    
    def get_week_thresholds_static(
        self, 
        user_handle_to_wave_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Get static week thresholds for each user."""
        if user_handle_to_wave_df.empty:
            return pd.DataFrame(columns=["bluesky_handle", "wave", "date", "week_static"])
        
        # Generate date range for each user based on their wave
        week_thresholds = []
        
        for _, user_row in user_handle_to_wave_df.iterrows():
            handle = user_row["bluesky_handle"]
            wave = user_row["wave"]
            
            study_config = self.config["studies"][f"wave{wave}"]
            start_date = study_config["start_date"]
            end_date = study_config["end_date"]
            
            # Generate daily dates for the user's study period
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            
            for date_val in date_range:
                date_str = date_val.strftime('%Y-%m-%d')
                week_num = self.map_date_to_static_week(date_str, wave)
                
                week_thresholds.append({
                    "bluesky_handle": handle,
                    "wave": wave,
                    "date": date_str,
                    "week_static": week_num
                })
        
        return pd.DataFrame(week_thresholds)
    
    def get_week_thresholds_dynamic(
        self, 
        user_handle_to_wave_df: pd.DataFrame,
        user_activity_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Get dynamic week thresholds based on user activity."""
        # This will implement the dynamic week calculation logic
        # based on when users actually started using the platform
        # Implementation details to be added based on existing logic
        return pd.DataFrame()


# Global threshold calculator instance
threshold_calculator = ThresholdCalculator()
```

### 4. Update Existing Scripts

#### 4.1 Update `load_data.py`

**Key Changes:**
- Import from shared modules instead of local functions
- Use configuration from shared config
- Maintain same function signatures for backward compatibility

```python
"""Loads the data for a given partition date."""

import gc
import os
from typing import Literal, Optional

import pandas as pd

from lib.constants import project_home_directory
from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger
from services.backfill.posts_used_in_feeds.load_data import (
    calculate_start_end_date_for_lookback,
    load_posts_used_in_feeds,
    load_preprocessed_posts_used_in_feeds_for_partition_date,
    default_num_days_lookback,
)

# Import from shared modules
from .shared.data_loading.post_loader import post_loader
from .shared.data_loading.label_loader import label_loader
from .shared.config.loader import config_loader

# Load configuration
config = config_loader.load_config("analytics")

logger = get_logger(__file__)


def load_filtered_preprocessed_posts(
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
) -> pd.DataFrame:
    """Load preprocessed posts that have been filtered with additional
    custom filters for analysis."""
    return post_loader.load_filtered_posts(
        partition_date, lookback_start_date, lookback_end_date
    )


def get_hydrated_posts_for_partition_date(
    partition_date: str,
    posts_df: Optional[pd.DataFrame] = None,
    load_unfiltered_posts: bool = False,
) -> pd.DataFrame:
    """Hydrate each post and create a wide table of post features."""
    return post_loader.get_hydrated_posts(
        partition_date, posts_df, load_unfiltered_posts
    )


# ... rest of the functions updated to use shared modules
```

#### 4.2 Update `feed_analytics.py`

**Key Changes:**
- Import feature calculation from shared modules
- Use configuration from shared config
- Maintain same function signatures for backward compatibility

```python
"""Calculates the average feed content for each user."""

from typing import Optional

import numpy as np
import pandas as pd

from lib.log.logger import get_logger
from lib.helper import get_partition_dates

# Import from shared modules
from .shared.processing.feature_calculator import feature_calculator
from .shared.config.loader import config_loader

# Load configuration
config = config_loader.load_config("analytics")
studies_config = config["studies"]

start_date_inclusive = studies_config["wave1"]["start_date"]
end_date_inclusive = studies_config["wave2"]["end_date"]
exclude_partition_dates = config["defaults"]["exclude_partition_dates"]
default_label_threshold = config["defaults"]["label_threshold"]

logger = get_logger(__file__)


def get_per_user_feed_averages_for_partition_date(
    partition_date: str, load_unfiltered_posts: bool = True
) -> pd.DataFrame:
    """For each user, calculates the average feed content for a given partition date."""
    # ... existing logic for loading data ...
    
    # Use shared feature calculator
    for user, posts_df in map_user_to_posts_df.items():
        averages = {
            "user": user,
            "user_did": user,
        }
        
        # Calculate features using shared calculator
        features = feature_calculator.calculate_all_features(posts_df)
        averages.update(features)
        
        user_averages.append(averages)
    
    return pd.DataFrame(user_averages)


# ... rest of the functions updated to use shared modules
```

#### 4.3 Update `calculate_weekly_thresholds_per_user.py`

**Key Changes:**
- Import threshold calculation from shared modules
- Use configuration from shared config
- Maintain same function signatures for backward compatibility

```python
"""One-off script to calculate the week thresholds for each user."""

from datetime import date
import os
from typing import Optional

import pandas as pd

from lib.helper import get_partition_dates
from lib.log.logger import get_logger

# Import from shared modules
from .shared.processing.threshold_calculator import threshold_calculator
from .shared.config.loader import config_loader

# Load configuration
config = config_loader.load_config("analytics")
studies_config = config["studies"]

start_date_inclusive = studies_config["wave1"]["start_date"]
end_date_inclusive = studies_config["wave2"]["end_date"]
exclude_partition_dates = config["defaults"]["exclude_partition_dates"]

logger = get_logger(__file__)


def map_date_to_static_week(partition_date: str, wave: int) -> int:
    """Map a partition date to a static week number, based on the user's wave."""
    return threshold_calculator.map_date_to_static_week(partition_date, wave)


def get_week_thresholds_per_user_static(
    user_handle_to_wave_df: pd.DataFrame,
) -> pd.DataFrame:
    """Get the week thresholds for each user, based on a Monday -> Monday week schedule."""
    return threshold_calculator.get_week_thresholds_static(user_handle_to_wave_df)


# ... rest of the functions updated to use shared modules
```

### 5. Testing Strategy

#### 5.1 Unit Tests
- Test each shared module independently
- Mock dependencies for isolated testing
- Test configuration loading and validation
- Test error handling scenarios

#### 5.2 Integration Tests
- Test that existing scripts still work
- Verify same outputs are produced
- Test configuration changes propagate correctly
- Test error handling in real scenarios

#### 5.3 Test Structure
```
services/calculate_analytics/study_analytics/shared/tests/
├── __init__.py
├── test_config/
│   ├── test_loader.py
│   └── test_validator.py
├── test_data_loading/
│   ├── test_post_loader.py
│   ├── test_label_loader.py
│   └── test_filters.py
└── test_processing/
    ├── test_feature_calculator.py
    └── test_threshold_calculator.py
```

### 6. Implementation Checklist

#### Phase 1A: Foundation & Configuration (Days 1-2)
- [ ] Create shared configuration directory structure
- [ ] Implement configuration loading utilities
- [ ] Create YAML configuration files
- [ ] Implement configuration validation
- [ ] Test configuration loading and validation

#### Phase 1B: Data Loading Layer (Days 3-4)
- [ ] Create base data loading classes
- [ ] Implement post loader module
- [ ] Implement label loader module
- [ ] Implement feed loader module
- [ ] Implement user loader module
- [ ] Add data filtering utilities
- [ ] Test all data loading modules

#### Phase 1C: Processing Logic Extraction (Days 5-6)
- [ ] Create base processing classes
- [ ] Implement feature calculator module
- [ ] Implement threshold calculator module
- [ ] Create data aggregation utilities
- [ ] Test all processing modules

#### Phase 1D: Integration & Testing (Day 7)
- [ ] Update `load_data.py` to use shared modules
- [ ] Update `feed_analytics.py` to use shared modules
- [ ] Update `calculate_weekly_thresholds_per_user.py` to use shared modules
- [ ] Verify all existing functionality preserved
- [ ] Run comprehensive test suite
- [ ] Update documentation

### 7. Success Criteria Validation

#### Functional Requirements
- [ ] All existing scripts still work after refactoring
- [ ] Same outputs produced for same inputs
- [ ] Configuration changes propagate correctly
- [ ] Error handling works as expected

#### Code Quality Requirements
- [ ] No code duplication between shared modules
- [ ] Configuration externalized from hardcoded values
- [ ] Clear separation of concerns
- [ ] Comprehensive error handling
- [ ] Proper logging throughout

#### Testing Requirements
- [ ] Unit tests for all shared modules
- [ ] Integration tests for updated scripts
- [ ] Configuration validation tests
- [ ] Error handling tests
- [ ] Performance regression tests

### 8. Risk Mitigation

#### Technical Risks
- **Breaking Changes**: Maintain backward compatibility by keeping same function signatures
- **Performance Regression**: Profile before/after to ensure no performance degradation
- **Configuration Complexity**: Start with simple configuration and gradually add complexity

#### Process Risks
- **Scope Creep**: Stick to Phase 1 scope, defer advanced features to later phases
- **Testing Gaps**: Comprehensive testing strategy to catch issues early
- **Integration Issues**: Incremental updates with testing at each step

### 9. Post-Implementation Tasks

#### Documentation Updates
- [ ] Update README files with new architecture
- [ ] Document shared module APIs
- [ ] Create usage examples
- [ ] Update configuration documentation

#### Knowledge Transfer
- [ ] Document lessons learned
- [ ] Share implementation insights with team
- [ ] Update coding standards if needed
- [ ] Plan Phase 2 implementation

#### Monitoring & Maintenance
- [ ] Monitor for any issues in production
- [ ] Collect feedback on new architecture
- [ ] Plan performance optimizations
- [ ] Prepare for Phase 2 implementation

## Conclusion

This implementation plan provides a structured approach to Phase 1 of the analytics system refactor. By focusing on extracting shared functionality into reusable modules while maintaining backward compatibility, we can significantly improve code quality and maintainability without disrupting existing workflows.

The plan follows a phased approach that allows for incremental validation and reduces risk. Each phase builds upon the previous one, ensuring that the system remains functional throughout the refactoring process.

**Next Steps:**
1. Review and approve this implementation plan
2. Begin Phase 1A implementation (Foundation & Configuration)
3. Set up testing infrastructure
4. Start implementing shared modules incrementally

**Estimated Timeline**: 1 week (5 business days)  
**Resource Requirements**: 1 developer  
**Dependencies**: None - this is the starting point for the refactoring effort
