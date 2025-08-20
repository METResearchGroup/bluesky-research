from typing import List, Dict, Any
import re


def filter_and_preview_sample_data(
    filters: Dict[str, Any], data: List[Dict[str, Any]], preview: bool = True
) -> List[Dict[str, Any]]:
    """
    Filters the sample data according to the provided filters and returns the top N matching rows.
    Supports filtering by keywords, hashtags, date range, user handles, valence, toxic, political, and slant.

    Args:
        filters: Nested dict of filters (e.g., {'Content': {'keywords': [...]}, ...})
        data: List of post dicts to filter.
        preview: If True, return only the first 5 rows. If False, return all up to max_results.
    Returns:
        List of filtered post dicts (up to 5 for preview, or up to max_results for export).
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
        # Sentiment: valence
        valence = filters.get("Sentiment", {}).get("valence")
        if valence and row.get("valence") != valence:
            return False
        # Sentiment: toxic
        toxicity = filters.get("Sentiment", {}).get("toxicity")
        if toxicity:
            if toxicity == "Toxic" and row.get("toxic") is not True:
                return False
            if toxicity == "Not Toxic" and row.get("toxic") is not False:
                return False
            if toxicity == "Uncertain" and row.get("toxic") is not None:
                return False
        # Political: political
        political = filters.get("Political", {}).get("political")
        if political:
            if political == "Yes" and row.get("political") is not True:
                return False
            if political == "No" and row.get("political") is not False:
                return False
        # Political: slant
        slant = filters.get("Political", {}).get("slant")
        if slant and row.get("slant") != slant:
            return False
        return True

    result = [row for row in data if row_matches(row)]
    max_results = filters.get("General", {}).get("max_results", 1000)
    result = result[:max_results]
    if preview:
        return result[:5]
    return result
