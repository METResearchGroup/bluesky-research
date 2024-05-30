"""Collect the pilot data from the database and store as .csv and .jsonl files.

These posts, which have passed filtering and preprocessing, are already stored
in the database. Since we want to know at all times what the data is that we
are analyzing and that this dataset needs to be made openly available, we
need to fetch the most recent data and store it as a .csv and .jsonl file.
"""
import json
import os

import pandas as pd

from lib.constants import current_datetime_str
from lib.db.sql.preprocessing_database import get_filtered_posts

current_file_directory = os.path.dirname(os.path.abspath(__file__))

pilot_data_filename_csv = f"pilot_data_{current_datetime_str}.csv"
pilot_data_filename_jsonl = f"pilot_data_{current_datetime_str}.jsonl"

if __name__ == "__main__":
    post_dicts: list[dict] = get_filtered_posts(export_format="dict")
    df = pd.DataFrame(post_dicts)
    df.to_csv(
        os.path.join(current_file_directory, pilot_data_filename_csv),
        index=False
    )
    with open(pilot_data_filename_jsonl, 'w') as f:
        df_dicts = df.to_dict(orient="records")
        for df_dict in df_dicts:
            df_dict_str = json.dumps(df_dict)
            f.write(df_dict_str + '\n')
