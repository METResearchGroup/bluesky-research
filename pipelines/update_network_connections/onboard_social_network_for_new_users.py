"""Onboards the list of users to the network.

Assumes that everyone who is being added has some follows/followers.
"""

from lib.aws.athena import Athena
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel

athena = Athena()


def load_users_with_connections() -> set[str]:
    """Queries the Athena table to get the list of user DIDs with connections."""  # noqa
    query = """
    SELECT DISTINCT study_user_did 
    FROM (
        SELECT follow_did AS study_user_did 
        FROM user_social_networks 
        WHERE relationship_to_study_user = 'follower'
        UNION ALL
        SELECT follower_did AS study_user_did 
        FROM user_social_networks 
        WHERE relationship_to_study_user = 'follow'
    ) AS combined
    """
    df = athena.query_results_as_df(query)
    df_dicts = df.to_dict(orient="records")
    df_dicts = athena.parse_converted_pandas_dicts(df_dicts)
    return set([row["study_user_did"] for row in df_dicts])


def main():
    users: list[UserToBlueskyProfileModel] = get_all_users()
    users_with_connections: set[str] = load_users_with_connections()
    users_not_synced: list[UserToBlueskyProfileModel] = [
        user for user in users if user.bluesky_user_did not in users_with_connections
    ]
    for user in users_not_synced:
        print(user.bluesky_user_did)


if __name__ == "__main__":
    main()
