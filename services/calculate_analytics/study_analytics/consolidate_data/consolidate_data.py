from services.calculate_analytics.study_analytics.consolidate_data.consolidate_feeds import (
    main as consolidate_feeds,
)
from services.calculate_analytics.study_analytics.consolidate_data.consolidate_user_session_logs import (
    main as consolidate_user_session_logs,
)


def main():
    consolidate_feeds()
    consolidate_user_session_logs()


if __name__ == "__main__":
    main()
