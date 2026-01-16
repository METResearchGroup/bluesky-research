import os

import pandas as pd

from lib.constants import project_home_directory


def load_uris_to_filter(path: str) -> set[str]:
    """Loads URIs to filter out."""
    fp = os.path.join(project_home_directory, path)
    df = pd.read_parquet(fp)
    return set(df["uri"])
