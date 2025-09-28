from services.calculate_analytics.shared.constants import (
    STUDY_END_DATE,
    STUDY_START_DATE,
    exclude_partition_dates,
)
from lib.helper import get_partition_dates
from services.get_author_to_average_toxicity_outrage.helper import (
    get_and_export_daily_author_to_average_toxicity_outrage,
)


def main():
    partition_dates: list[str] = get_partition_dates(
        start_date=STUDY_START_DATE,
        end_date=STUDY_END_DATE,
        exclude_partition_dates=exclude_partition_dates,
    )
    get_and_export_daily_author_to_average_toxicity_outrage(partition_dates)


if __name__ == "__main__":
    main()
