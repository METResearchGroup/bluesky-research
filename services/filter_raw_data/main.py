"""Ingests raw firehose data and runs appropriate filters."""
from services.filter_raw_data.helper import filter_latest_raw_data

def main() -> None:
    """Runs the main function."""
    filter_latest_raw_data(1000)


if __name__ == "__main__":
    main()
