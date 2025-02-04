"""Loads data from S3 into local storage.

This loads the (1) feeds and (2) user session logs into local storage.
"""

from services.calculate_analytics.study_analytics.load_data.get_all_feeds import (
    main as get_all_feeds,
)
from services.calculate_analytics.study_analytics.load_data.get_all_user_session_logs import (
    main as get_all_user_session_logs,
)


def main():
    get_all_feeds()
    get_all_user_session_logs()


if __name__ == "__main__":
    main()
