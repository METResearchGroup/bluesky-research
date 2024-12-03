from datetime import datetime, timedelta


def get_partition_dates(start_date: str, end_date: str) -> list[str]:
    """Returns a list of dates between start_date and end_date, inclusive.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        List of dates in YYYY-MM-DD format
    """
    partition_dates = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_timestamp = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_timestamp:
        partition_dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    return partition_dates
