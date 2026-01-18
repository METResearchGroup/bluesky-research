"""Tooling for discovering files based on a timestamp."""

import os


def _fetch_files_in_directory(directory_path: str) -> list[str]:
    """Fetch files in a directory."""
    files_list: list[str] = []
    for root, _, files in os.walk(directory_path):
        for file in files:
            files_list.append(os.path.join(root, file))
    return files_list


# NOTE: _fetch_files_after_* are just very thin layers over the
# _fetch_files_in_directory function. However, this is intentional
# as when they're used in find_files_after_timestamp, it makes it much
# easier to read and know what's the reason why we're fetching those files.
def _fetch_files_after_timestamp_year(year_dir_path: str) -> list[str]:
    """Fetch files after the timestamp year."""
    return _fetch_files_in_directory(year_dir_path)


def _fetch_files_after_timestamp_month(month_dir_path: str) -> list[str]:
    """Fetch files after the timestamp month."""
    return _fetch_files_in_directory(month_dir_path)


def _fetch_files_after_timestamp_day(day_dir_path: str) -> list[str]:
    """Fetch files after the timestamp day."""
    return _fetch_files_in_directory(day_dir_path)


def _fetch_files_after_timestamp_hour(hour_dir_path: str) -> list[str]:
    """Fetch files after the timestamp hour."""
    return _fetch_files_in_directory(hour_dir_path)


def _fetch_files_after_timestamp_minute(minute_dir_path: str) -> list[str]:
    """Fetch files after the timestamp minute."""
    return _fetch_files_in_directory(minute_dir_path)


def _fetch_all_files_after_timestamp_year(
    base_path: str,
    timestamp_year: str,
) -> list[str]:
    """For all the year-by-year files in the base path, fetch all
    files after the timestamp year.

    Our current setup assumes a year/month/day/hour/minute directory structure,
    so we can check at the top-level "year" directory and if the year is greater
    than the timestamp year, we crawl all the files that are more recent than the
    timestamp year.
    """
    files_list: list[str] = []
    year_level_paths = os.listdir(base_path)
    for year_dir in year_level_paths:
        if year_dir > timestamp_year:
            future_year_files: list[str] = _fetch_files_after_timestamp_year(
                year_dir_path=os.path.join(
                    base_path,
                    year_dir,
                )
            )
            files_list.extend(future_year_files)
    return files_list


def _fetch_all_files_after_timestamp_month(
    year_dir_path: str,
    timestamp_month: str,
) -> list[str]:
    """For all the month-by-month files in the year of interest, fetch all
    files after the timestamp month.

    At this level, if we have the same year, then we crawl all the files
    that are more recent than the timestamp month.
    """
    files_list: list[str] = []
    month_level_paths = os.listdir(year_dir_path)
    for month_dir in month_level_paths:
        if month_dir > timestamp_month:
            future_month_files: list[str] = _fetch_files_after_timestamp_month(
                month_dir_path=os.path.join(
                    year_dir_path,
                    month_dir,
                )
            )
            files_list.extend(future_month_files)

    return files_list


def _fetch_all_files_after_timestamp_day(
    month_dir_path: str,
    timestamp_day: str,
) -> list[str]:
    """For all the day-by-day files in the month of interest, fetch all
    files after the timestamp day.

    At this level, if we have the same year + month, then
    we crawl all the files that are more recent than the timestamp day.
    """
    files_list: list[str] = []
    day_level_paths = os.listdir(month_dir_path)
    for day_dir in day_level_paths:
        if day_dir > timestamp_day:
            future_day_files: list[str] = _fetch_files_after_timestamp_day(
                day_dir_path=os.path.join(
                    month_dir_path,
                    day_dir,
                )
            )
            files_list.extend(future_day_files)

    return files_list


def _fetch_all_files_after_timestamp_hour(
    day_dir_path: str,
    timestamp_hour: str,
) -> list[str]:
    """For all the hour-by-hour files in the day of interest, fetch all
    files after the timestamp hour.

    At this level, if we have the same year + month + day, then
    we crawl all the files that are more recent than the timestamp hour.
    """
    files_list: list[str] = []
    hour_level_paths = os.listdir(day_dir_path)
    for hour_dir in hour_level_paths:
        if hour_dir > timestamp_hour:
            future_minute_files: list[str] = _fetch_files_after_timestamp_hour(
                hour_dir_path=os.path.join(
                    day_dir_path,
                    hour_dir,
                )
            )
            files_list.extend(future_minute_files)

    return files_list


def _fetch_all_files_after_timestamp_minute(
    hour_dir_path: str,
    timestamp_minute: str,
) -> list[str]:
    """For all the minute-by-minute files in the hour of interest, fetch all
    files after the timestamp minute.

    At this level, if we have the same year + month + day + hour, then
    we crawl all the files that are more recent than the timestamp minute.
    """
    files_list: list[str] = []
    minute_level_paths = os.listdir(hour_dir_path)
    for minute_dir in minute_level_paths:
        if minute_dir > timestamp_minute:
            future_minute_files: list[str] = _fetch_files_after_timestamp_minute(
                minute_dir_path=os.path.join(
                    hour_dir_path,
                    minute_dir,
                )
            )
            files_list.extend(future_minute_files)

    return files_list


def find_files_after_timestamp(base_path: str, target_timestamp_path: str) -> list[str]:
    """Find files after a given timestamp.

    We crawl the directory structure in a breadth-first manner, starting at the
    top-level "year" directory and working our way down to the "minute" directory.
    For each level, we crawl all the files that are more recent than the timestamp
    at that level.

    This assumes a year/month/day/hour/minute directory structure, which is what we
    currently use for our setups.
    """
    if not target_timestamp_path or not isinstance(target_timestamp_path, str):
        raise ValueError(
            f"target_timestamp_path must be a non-empty string, got: {target_timestamp_path!r}"
        )
    timestamp_parts = target_timestamp_path.split("/")
    if len(timestamp_parts) != 5:
        raise ValueError(
            f"target_timestamp_path must be 'year/month/day/hour/minute', got: {target_timestamp_path!r}"
        )
    target_year, target_month, target_day, target_hour, target_minute = timestamp_parts
    files_list: list[str] = []

    # grab files from future years
    future_year_files: list[str] = _fetch_all_files_after_timestamp_year(
        base_path=base_path,
        timestamp_year=target_year,
    )

    # grab files from the same year
    # but more recent than the timestamp month.
    future_month_files: list[str] = _fetch_all_files_after_timestamp_month(
        year_dir_path=os.path.join(base_path, target_year),
        timestamp_month=target_month,
    )

    # grab files from the same year + month
    # but more recent than the timestamp day
    future_day_files: list[str] = _fetch_all_files_after_timestamp_day(
        month_dir_path=os.path.join(base_path, target_year, target_month),
        timestamp_day=target_day,
    )

    # grab files from the same year + month + day,
    # but more recent than the timestamp hour.
    future_hour_files: list[str] = _fetch_all_files_after_timestamp_hour(
        day_dir_path=os.path.join(base_path, target_year, target_month, target_day),
        timestamp_hour=target_hour,
    )

    # grab files from the same year + month + day + hour,
    # but more recent than the timestamp minute.
    future_minute_files: list[str] = _fetch_all_files_after_timestamp_minute(
        hour_dir_path=os.path.join(
            base_path,
            target_year,
            target_month,
            target_day,
            target_hour,
        ),
        timestamp_minute=target_minute,
    )

    # append all the files to the list
    files_list.extend(future_year_files)
    files_list.extend(future_month_files)
    files_list.extend(future_day_files)
    files_list.extend(future_hour_files)
    files_list.extend(future_minute_files)

    return files_list
