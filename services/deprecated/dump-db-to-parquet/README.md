# Dump DB to Parquet

This is a Rust service that dumps our SQLite DB

Some possible updates that could be made:
- We could make this more generic conversions across data types (e.g., .jsonl to .parquet).
- We are starting with converting raw firehose data, but we can make conversions in other parts of the pipeline.