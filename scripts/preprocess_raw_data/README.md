# Scripts for preprocessing raw data

Steps:

1. Run `load_raw_data_for_preprocessing.py` for a date range, to push
those records to the queue for `preprocess_raw_data`.
2. Run `trigger_preprocess_raw_data.py` to trigger the `preprocess_raw_data` pipeline.
3. Run `write_preprocessed_posts_to_parquet.py` to trigger the `write_cache_buffers_to_db` service and persist the preprocessed posts, which will be in `output_preprocess_raw_data`, to permanent parquet storage.
