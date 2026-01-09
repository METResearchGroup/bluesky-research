"""Generic class for implementing caches.

Under the hood, uses SQLite to implement caches.

Each cache will have their own SQLite instance in order to scale
each cache independently.
"""

import os

from lib.load_env_vars import EnvVarsContainer
from lib.log.logger import get_logger


logger = get_logger(__file__)

bsky_data_dir = EnvVarsContainer.get_env_var("BSKY_DATA_DIR")
if not bsky_data_dir:
    raise ValueError("BSKY_DATA_DIR must be set to use lib.db.cache")

root_db_path = os.path.join(bsky_data_dir, "cache")
if not os.path.exists(root_db_path):
    logger.info(f"Creating new directory for cache data at {root_db_path}...")
    # exist_ok=True handles TOCTOU race condition when parallel test workers
    # concurrently import this module and attempt to create the directory
    os.makedirs(root_db_path, exist_ok=True)

existing_sqlite_dbs = [
    file for file in os.listdir(root_db_path) if file.endswith(".db")
]


class CacheItem:
    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value


class Cache:
    def __init__(self, cache_name: str):
        self.cache_name = cache_name
        self.cache_db_path = os.path.join(root_db_path, f"{cache_name}.db")
        self.cache_table_name = f"{cache_name}_cache"


if __name__ == "__main__":
    cache = Cache(cache_name="test_cache")
    cache.set("test_key", "test_value")
    print(cache.get("test_key"))
