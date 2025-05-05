"""Clears the SQLite caches for the preprocessed posts and writes
them to parquet files."""

from services.write_cache_buffers_to_db.helper import write_cache_buffer_queue_to_db


def main():
    """Clears the SQLite caches for the preprocessed posts and writes
    them to parquet files."""
    service = "preprocess_raw_data"
    clear_queue = True
    write_cache_buffer_queue_to_db(service=service, clear_queue=clear_queue)


if __name__ == "__main__":
    main()
