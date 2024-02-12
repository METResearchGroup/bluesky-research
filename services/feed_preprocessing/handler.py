"""Handler for managing preprocessing of raw data before passing to ranking
and recommendation algorithms."""
from services.feed_preprocessing.helper import preprocess_raw_data, preprocess_raw_data_polars


def main(event: dict, context: dict) -> int:
    #preprocess_raw_data(chunk_size=event.get("chunk_size", 5))
    preprocess_raw_data_polars()
    return 0


if __name__ == "__main__":
    # each chunk = 1 .jsonl file, each .jsonl file ~ 100 posts
    event = {"chunk_size": 5}
    context = {}
    main(event=event, context=context)
