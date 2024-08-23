import os

from lib.constants import root_local_data_directory

cache_key = "__cache_sociopolitical__"
sociopolitical_root_s3_key = "ml_inference_sociopolitical"
root_cache_path = os.path.join(root_local_data_directory, cache_key)
