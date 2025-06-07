import os
import json
from typing import List, Dict, Any


def get_sample_posts(scale: int = 1) -> List[Dict[str, Any]]:
    """
    Loads the sample posts from sample_posts.json and returns as a list of dicts.
    Optionally repeats the data 'scale' times for demo purposes.
    Args:
        scale: The number of times to repeat the sample data (default 1).
    Returns:
        List of sample post dicts, repeated as specified.
    """
    data_path = os.path.join(os.path.dirname(__file__), "sample_posts.json")
    with open(data_path, "r") as f:
        data = json.load(f)
    if scale > 1:
        data = data * scale
    print(f"[DIAG] get_sample_posts: scale={scale}, returned={len(data)} records")
    return data
