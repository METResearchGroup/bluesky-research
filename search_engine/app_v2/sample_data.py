import os
import json
from typing import List, Dict, Any


def get_sample_posts() -> List[Dict[str, Any]]:
    """
    Loads the sample posts from sample_posts.json and returns as a list of dicts.
    """
    data_path = os.path.join(os.path.dirname(__file__), "sample_posts.json")
    with open(data_path, "r") as f:
        return json.load(f)
