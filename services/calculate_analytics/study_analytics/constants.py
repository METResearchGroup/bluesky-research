import os

from lib.constants import root_local_data_directory

raw_data_root_path = os.path.join(root_local_data_directory, "analytics", "raw")
consolidated_data_root_path = os.path.join(
    root_local_data_directory, "analytics", "consolidated"
)
