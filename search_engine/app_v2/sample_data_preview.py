from typing import List, Dict, Any
import re


def filter_and_preview_sample_data(
    filters: Dict[str, Any], data: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Filters the sample data according to the provided filters and returns the top 5 matching rows.
    Supports filtering by keywords, hashtags, date range, and user handles.

    Args:
        filters: Nested dict of filters (e.g., {'Content': {'keywords': [...]}, ...})
        data: List of post dicts to filter.
    Returns:
        List of up to 5 filtered post dicts.
    """

    def row_matches(row: Dict[str, Any]) -> bool:
        # Content: keywords
        keywords = filters.get("Content", {}).get("keywords", [])
        if keywords and not any(kw.lower() in row["text"].lower() for kw in keywords):
            return False
        # Content: hashtags
        hashtags = filters.get("Content", {}).get("hashtags", [])
        if hashtags and not any(tag in row["hashtags"] for tag in hashtags):
            return False
        # Temporal: date_range
        date_range = filters.get("Temporal", {}).get("date_range")
        if date_range:
            try:
                # Accepts 'YYYY-MM-DD to YYYY-MM-DD'
                parts = re.split(r"\s*to\s*", date_range)
                start, end = parts[0].strip(), parts[-1].strip()
                if not (start <= row["date"] <= end):
                    return False
            except Exception:
                return False
        # User: handles
        handles = filters.get("User", {}).get("handles", [])
        if handles and row["user"] not in handles:
            return False
        return True

    result = [row for row in data if row_matches(row)]
    return result[:5]
