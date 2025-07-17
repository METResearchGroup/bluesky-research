"""Data loading integration for the Bluesky Post Explorer Backend.

This module provides data loading functionality for preprocessed posts,
implementing the integration required for MET-18.
"""

import sys
import os
from typing import Optional, List, Dict, Any, Literal
import pandas as pd
from pathlib import Path

# Add the project root to the Python path for imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the load_preprocessed_posts function
try:
    from services.backfill.posts.load_data import load_preprocessed_posts
except ImportError:
    # Fallback for development/testing when services module is not available
    def load_preprocessed_posts(
        start_date: str = "",
        end_date: str = "",
        sorted_by_partition_date: bool = False,
        ascending: bool = False,
        table_columns: Optional[List[str]] = None,
        output_format: Literal["list", "df"] = "list",
        convert_ts_fields: bool = False,
    ) -> List[Dict] | pd.DataFrame:
        """Fallback function for development when services module is not available.
        
        Returns mock data for development and testing purposes.
        """
        mock_data = [
            {
                'uri': 'at://example.com/app.bsky.feed.post/123',
                'text': 'Sample post content for testing',
                'preprocessing_timestamp': '2024-01-01T00:00:00Z'
            },
            {
                'uri': 'at://example.com/app.bsky.feed.post/124',
                'text': 'Another sample post for testing data loading',
                'preprocessing_timestamp': '2024-01-01T01:00:00Z'
            }
        ]
        
        if output_format == "df":
            return pd.DataFrame(mock_data)
        return mock_data


class DataLoader:
    """Data loader class for preprocessed posts integration.
    
    This class provides a clean interface for loading preprocessed posts
    and handles various data loading scenarios for the backend.
    """
    
    def __init__(self):
        """Initialize the DataLoader."""
        self.available = self._check_data_availability()
    
    def _check_data_availability(self) -> bool:
        """Check if the data loading service is available.
        
        Returns:
            bool: True if data loading service is available, False otherwise
        """
        try:
            # Try to import and check if we can access the data loading functionality
            from services.backfill.posts.load_data import load_preprocessed_posts
            return True
        except ImportError:
            return False
    
    def load_posts(
        self,
        start_date: str,
        end_date: str,
        sorted_by_partition_date: bool = True,
        ascending: bool = False,
        table_columns: Optional[List[str]] = None,
        output_format: Literal["list", "df"] = "df",
        convert_ts_fields: bool = False,
    ) -> List[Dict] | pd.DataFrame:
        """Load preprocessed posts with the specified parameters.
        
        Args:
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)
            sorted_by_partition_date: Whether to sort the posts by partition date
            ascending: Whether to sort the posts in ascending order
            table_columns: Columns to load from the table
            output_format: Format to return the data in ("list" or "df")
            convert_ts_fields: Whether to convert timestamp fields
            
        Returns:
            List[Dict] | pd.DataFrame: The loaded posts data
            
        Raises:
            ValueError: If start_date or end_date are not provided
            RuntimeError: If data loading fails
        """
        if not start_date or not end_date:
            raise ValueError("start_date and end_date must be provided")
        
        try:
            return load_preprocessed_posts(
                start_date=start_date,
                end_date=end_date,
                sorted_by_partition_date=sorted_by_partition_date,
                ascending=ascending,
                table_columns=table_columns,
                output_format=output_format,
                convert_ts_fields=convert_ts_fields,
            )
        except Exception as e:
            raise RuntimeError(f"Failed to load preprocessed posts: {str(e)}")
    
    def filter_posts(
        self, 
        posts_df: pd.DataFrame, 
        query: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Filter posts DataFrame based on query and limit.
        
        Args:
            posts_df: DataFrame containing posts
            query: Optional text search query
            limit: Optional limit on number of posts
            
        Returns:
            pd.DataFrame: Filtered posts DataFrame
        """
        filtered_df = posts_df.copy()
        
        # Apply text search filter if query provided
        if query and 'text' in filtered_df.columns:
            filtered_df = filtered_df[
                filtered_df['text'].str.contains(query, case=False, na=False)
            ]
        
        # Apply limit if provided
        if limit:
            filtered_df = filtered_df.head(limit)
        
        return filtered_df
    
    def get_posts_summary(self, posts_df: pd.DataFrame) -> Dict[str, Any]:
        """Get summary statistics for posts DataFrame.
        
        Args:
            posts_df: DataFrame containing posts
            
        Returns:
            Dict[str, Any]: Summary statistics
        """
        summary = {
            "total_count": len(posts_df),
            "columns": list(posts_df.columns),
            "memory_usage": posts_df.memory_usage(deep=True).sum(),
        }
        
        if 'preprocessing_timestamp' in posts_df.columns:
            summary["date_range"] = {
                "earliest": posts_df['preprocessing_timestamp'].min(),
                "latest": posts_df['preprocessing_timestamp'].max()
            }
        
        return summary
    
    def validate_date_format(self, date_string: str) -> bool:
        """Validate that date string is in YYYY-MM-DD format.
        
        Args:
            date_string: Date string to validate
            
        Returns:
            bool: True if valid format, False otherwise
        """
        import re
        
        # Check if the string matches the exact YYYY-MM-DD pattern
        pattern = r'^\d{4}-\d{2}-\d{2}$'
        if not re.match(pattern, date_string):
            return False
        
        try:
            # Parse with strict format checking
            pd.to_datetime(date_string, format='%Y-%m-%d', exact=True)
            return True
        except ValueError:
            return False


# Create a singleton instance for use across the application
data_loader = DataLoader()