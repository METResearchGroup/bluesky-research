"""Helper tooling for tracking and updating user engagement metrics."""
from atproto_client.models.app.bsky.actor.defs import ProfileViewDetailed

from lib.helper import client
from services.participant_data.helper import get_user_to_bluesky_profiles_as_list_dicts # noqa


def get_aggregate_user_profile_metrics(profile: ProfileViewDetailed) -> dict:
    return {
        "total_followers": profile.followers_count,
        "total_following": profile.follows_count,
        "total_posts_written": profile.posts_count
    }


def get_total_user_engagement_metrics() -> dict:
    return {
        "total_likes": 0,
        "total_reposts": 0,
    }


def get_updated_metrics_for_user(user: dict) -> dict:
    user_did = user["bluesky_user_did"]
    user_profile: ProfileViewDetailed = client.get_profile(user_did)
    user_metrics = get_aggregate_user_profile_metrics(user_profile)
    return user_metrics


def write_updated_metrics_to_db(metrics_list: list[dict]):
    pass


def get_updated_metrics_for_users() -> list[dict]:
    users: list[dict] = get_user_to_bluesky_profiles_as_list_dicts()
    metrics_list: list[dict] = []
    for user in users:
        metrics = get_updated_metrics_for_user(user)
        metrics_list.append(metrics)
    return metrics_list


def update_user_engagement_metrics():
    metrics_list: list[dict] = get_updated_metrics_for_users()
    write_updated_metrics_to_db(metrics_list)


if __name__ == "__main__":
    users: list[dict] = get_user_to_bluesky_profiles_as_list_dicts()
    metrics_list: list[dict] = get_updated_metrics_for_users()
    breakpoint()
