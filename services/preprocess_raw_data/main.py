"""Ingests raw firehose data and runs appropriate filters."""
from services.preprocess_raw_data.helper import preprocess_raw_data

def main() -> None:
    """Runs the main function."""
    preprocess_raw_data()


if __name__ == "__main__":
    main()
