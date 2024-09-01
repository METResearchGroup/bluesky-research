import os

from lib.constants import root_local_data_directory

cache_key = "__cache_sociopolitical__"
sociopolitical_root_s3_key = "ml_inference_sociopolitical"

# if in lambda container, create cache path in /tmp
# NOTE: this only has 512MB of memory.
if os.path.exists("/app"):
    root_cache_path = os.path.join("/tmp", cache_key)
else:
    root_cache_path = os.path.join(root_local_data_directory, cache_key)
