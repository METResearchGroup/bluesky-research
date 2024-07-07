import os

from lib.constants import root_local_data_directory

cache_key = "__cache_perspective_api__"
perspective_api_root_s3_key = "ml_inference_perspective_api"
previously_classified_post_uris_filename = "previously_classified_post_uris.json"  # noqa

root_cache_path = os.path.join(root_local_data_directory, cache_key)
