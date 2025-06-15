"""Gets aggregate metrics for user engagement."""

import os
import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.helper import get_partition_dates, generate_current_datetime_str
from services.calculate_analytics.study_analytics.generate_reports.weekly_user_logins import (
    load_user_date_to_week_df,
)
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel


current_dir = os.path.dirname(os.path.abspath(__file__))
current_datetime_str = generate_current_datetime_str()


def get_num_records_per_user_per_day(record_type: str) -> dict:
    """Get all records of a specific type for study users.

    Args:
        record_type: The type of record to get (like, post, follow, repost)

    Returns a dict with the following structure:

    {
        "did": {
            "2024-10-01": 10,
            "2024-10-02": 20,
            ...
        },
        ...
    }

    The dates returned for a user are only the dates for which there are records.
    """
    custom_args = {"record_type": record_type}
    df = load_data_from_local_storage(
        service="raw_sync",
        directory="cache",
        start_partition_date="2024-09-30",
        end_partition_date="2024-12-01",
        custom_args=custom_args,
    )
    df = df.drop_duplicates(subset=["uri"])
    df["partition_date"] = pd.to_datetime(df["synctimestamp"]).dt.date

    # Group by author and partition_date to get the number of records per user per day
    column_name = f"num_{record_type}s"
    records_per_user_per_day: list[dict] = (
        df.groupby(["author", "partition_date"])
        .size()
        .reset_index(name=column_name)
        .to_dict(orient="records")
    )

    result = {}

    for row in records_per_user_per_day:
        author = row["author"]
        date = row["partition_date"].strftime("%Y-%m-%d")
        num_records = row[column_name]

        if author not in result:
            result[author] = {}

        result[author][date] = num_records

    return result


def aggregate_metrics_per_user_per_day(users: list[dict], partition_dates: list[str]):
    """Get all engagement metrics for users per day.

    Returns a dict with the following structure:

    {
        "<handle>": {
            "2024-10-01": {
                "num_likes": 10,
                "num_posts": 20,
                "num_follows": 30,
                "num_reposts": 40,
            },
            ...
        },
        ...
    }

    Imputes zeros where relevant.
    """
    num_likes_per_user_per_day = get_num_records_per_user_per_day("like")
    num_posts_per_user_per_day = get_num_records_per_user_per_day("post")
    num_follows_per_user_per_day = get_num_records_per_user_per_day("follow")
    num_reposts_per_user_per_day = get_num_records_per_user_per_day("repost")

    aggregated_metrics_per_user_per_day: dict = {}

    print(f"Calculating engagement metrics for {len(users)} users")

    for user in users:
        did: str = user["bluesky_user_did"]
        handle: str = user["bluesky_handle"]
        num_likes_per_day: dict = num_likes_per_user_per_day.get(did, {})
        num_posts_per_day: dict = num_posts_per_user_per_day.get(did, {})
        num_follows_per_day: dict = num_follows_per_user_per_day.get(did, {})
        num_reposts_per_day: dict = num_reposts_per_user_per_day.get(did, {})

        if handle not in aggregated_metrics_per_user_per_day:
            aggregated_metrics_per_user_per_day[handle] = {}

        for partition_date in partition_dates:
            num_likes = num_likes_per_day.get(partition_date, 0)
            num_posts = num_posts_per_day.get(partition_date, 0)
            num_follows = num_follows_per_day.get(partition_date, 0)
            num_reposts = num_reposts_per_day.get(partition_date, 0)

            aggregated_metrics_per_user_per_day[handle][partition_date] = {
                "num_likes": num_likes,
                "num_posts": num_posts,
                "num_follows": num_follows,
                "num_reposts": num_reposts,
            }

    return aggregated_metrics_per_user_per_day


def aggregate_metrics_per_user_per_week(
    aggregated_metrics_per_user_per_day: dict,
    user_date_to_week_df: pd.DataFrame,
):
    """Get all engagement metrics for a user per week.

    Returns a dict with the following structure:

    {
        "<handle>": {
            "week": {
                "num_likes": 10,
                "num_posts": 20,
                "num_follows": 30,
                "num_reposts": 40,
            },
            ...
        },
        ...
    }
    """
    user_to_weekly_engagement_metrics: dict = {}
    for user, dates_to_metrics_map in aggregated_metrics_per_user_per_day.items():
        subset_user_date_to_week_df = user_date_to_week_df[
            user_date_to_week_df["bluesky_handle"] == user
        ]
        subset_map_date_to_week = dict(
            zip(
                subset_user_date_to_week_df["date"],
                subset_user_date_to_week_df["week"],
            )
        )
        total_engagement_metric_per_week: dict[str, dict[str, int]] = {}
        for date, metrics in dates_to_metrics_map.items():
            week = subset_map_date_to_week[date]
            if week not in total_engagement_metric_per_week:
                total_engagement_metric_per_week[week] = {
                    "num_likes": 0,
                    "num_posts": 0,
                    "num_follows": 0,
                    "num_reposts": 0,
                }
            total_engagement_metric_per_week[week] = {
                "num_likes": total_engagement_metric_per_week[week]["num_likes"]
                + metrics["num_likes"],
                "num_posts": total_engagement_metric_per_week[week]["num_posts"]
                + metrics["num_posts"],
                "num_follows": total_engagement_metric_per_week[week]["num_follows"]
                + metrics["num_follows"],
                "num_reposts": total_engagement_metric_per_week[week]["num_reposts"]
                + metrics["num_reposts"],
            }
        user_to_weekly_engagement_metrics[user] = total_engagement_metric_per_week

    return user_to_weekly_engagement_metrics


def transform_aggregated_metrics_per_user_per_day(
    aggregated_metrics_per_user_per_day: dict,
    users: list[dict],
) -> pd.DataFrame:
    """Transform the aggregated metrics per user per day into a DataFrame.

    Flattens the data into a single dataframe.

    Input:
        {
            "<handle>": {
                "2024-10-01": {
                    "num_likes": 10,
                    "num_posts": 20,
                    "num_follows": 30,
                    "num_reposts": 40,
                },
                ...
            },
            ...
        }

    Output:
        pd.DataFrame with the following columns:
        ```
        {
            "handle": "<handle>",
            "condition": "<condition>",
            "date": "<date>",
            "num_likes": 10,
            "num_posts": 20,
            "num_follows": 30,
            "num_reposts": 40,
        },
        ...
        ```
    """
    records = []

    for user in users:
        user_handle = user["bluesky_handle"]
        user_condition = user["condition"]

        metrics = aggregated_metrics_per_user_per_day[user_handle]

        for date, metrics in metrics.items():
            default_fields = {
                "handle": user_handle,
                "condition": user_condition,
                "date": date,
            }
            records.append({**default_fields, **metrics})

    return pd.DataFrame(records).sort_values(
        by=["handle", "date"], inplace=True, ascending=[True, True]
    )


def transform_aggregated_metrics_per_user_per_week(
    aggregated_metrics_per_user_per_week: dict,
    users: list[dict],
    user_date_to_week_df: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the aggregated metrics per user per week into a DataFrame.

    Iterates through each of the users and gets their data. Imputes default
    values when:
    - The user doesn't have data for the given week.
    - The user doesn't have engagement data at all.
    """
    flattened_metrics_per_user_per_week: list[dict] = []

    default_metrics = {
        "num_likes": 0,
        "num_posts": 0,
        "num_follows": 0,
        "num_reposts": 0,
    }

    for user in users:
        user_handle = user["bluesky_handle"]
        user_condition = user["condition"]

        subset_user_date_to_week_df = user_date_to_week_df[
            user_date_to_week_df["bluesky_handle"] == user_handle
        ]

        weeks = sorted(subset_user_date_to_week_df["week"].unique().tolist(), key=str)

        weeks_to_metrics_map = aggregated_metrics_per_user_per_week[user_handle]

        for week in weeks:
            if not week or pd.isna(week):
                # sometimes there's a weird empty string or None or NaN. Skip this.
                continue
            default_fields = {
                "handle": user_handle,
                "condition": user_condition,
                "week": week,
            }
            metrics = weeks_to_metrics_map.get(week, default_metrics)
            flattened_metrics_per_user_per_week.append({**default_fields, **metrics})

    return pd.DataFrame(flattened_metrics_per_user_per_week)


def main():
    """Main function."""
    users: list[UserToBlueskyProfileModel] = get_all_users()
    user_df: pd.DataFrame = pd.DataFrame([user.model_dump() for user in users])
    # Drop rows where is_study_user is False to only keep actual study participants
    user_df = user_df[user_df["is_study_user"]]
    user_df = user_df[["bluesky_handle", "bluesky_user_did", "condition"]]
    users = user_df.to_dict(orient="records")

    partition_dates: list[str] = get_partition_dates(
        start_date="2024-09-30",
        end_date="2024-12-01",
        exclude_partition_dates=[],
    )

    user_date_to_week_df = load_user_date_to_week_df()

    # The list of users from Qualtrics is slightly different from the list of users in DynamoDB.
    # This is due to a variety of reasons (test users, people leaving the study, etc.),
    # and we treat the users from Qualtrics as the source of ground truth.
    # So, we filter the users in DynamoDB to only include those that are in the Qualtrics logs.
    users_from_qualtrics_logs = set(user_date_to_week_df["bluesky_handle"].unique())
    users: list[dict] = [
        user for user in users if user["bluesky_handle"] in users_from_qualtrics_logs
    ]

    # get daily engagement metrics
    aggregated_metrics_per_user_per_day: dict = aggregate_metrics_per_user_per_day(
        users=users,
        partition_dates=partition_dates,
    )

    transformed_metrics_per_user_per_day: pd.DataFrame = (
        transform_aggregated_metrics_per_user_per_day(
            aggregated_metrics_per_user_per_day=aggregated_metrics_per_user_per_day,
            users=users,
        )
    )

    daily_fp = os.path.join(
        current_dir,
        "per_user_engagement_metrics",
        f"daily_engagement_metrics_per_user_{current_datetime_str}.csv",
    )
    transformed_metrics_per_user_per_day.to_csv(daily_fp, index=False)

    # get weekly engagement metrics
    aggregated_metrics_per_user_per_week: dict = aggregate_metrics_per_user_per_week(
        aggregated_metrics_per_user_per_day=aggregated_metrics_per_user_per_day,
        user_date_to_week_df=user_date_to_week_df,
    )

    transformed_metrics_per_user_per_week: pd.DataFrame = (
        transform_aggregated_metrics_per_user_per_week(
            aggregated_metrics_per_user_per_week=aggregated_metrics_per_user_per_week,
            users=users,
            user_date_to_week_df=user_date_to_week_df,
        )
    )

    fp = os.path.join(
        current_dir,
        "per_user_engagement_metrics",
        f"weekly_engagement_metrics_per_user_{current_datetime_str}.csv",
    )

    transformed_metrics_per_user_per_week.to_csv(fp, index=False)


if __name__ == "__main__":
    main()
