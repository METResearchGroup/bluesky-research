"""Gets the total number of users in the study."""

import os

import pandas as pd

from services.calculate_analytics.study_analytics.calculate_analytics.calculate_weekly_thresholds_per_user import (
    load_user_demographic_info,
)

current_filedir = os.path.dirname(os.path.abspath(__file__))


def main():
    study_users: pd.DataFrame = load_user_demographic_info()
    print(f"Total number of users in the study: {len(study_users)}")
    study_users = study_users[["bluesky_handle", "condition"]]
    fp = os.path.join(current_filedir, "study_users.csv")
    study_users.to_csv(fp, index=False)


if __name__ == "__main__":
    main()
