"""After syncing the social network of users to S3, let's fetch those
users and add their data to the local storage for the scraped user
social networks.
"""

import os

import pandas as pd

from lib.aws.athena import Athena
from lib.db.manage_local_data import export_data_to_local_storage
from services.participant_data.helper import get_all_users

athena = Athena()


current_file_directory = os.path.dirname(os.path.abspath(__file__))
missing_users_filename = "missing_users.csv"
missing_users_filepath = os.path.join(current_file_directory, missing_users_filename)

study_users = get_all_users()


def main():
    # these users have now been added to DynamoDB. We can subset
    # those DynamoDB records to get the records for the missing users
    missing_users_df: pd.DataFrame = pd.read_csv(missing_users_filepath)
    missing_users = [
        user
        for user in study_users
        if user.bluesky_handle in missing_users_df["handle"].tolist()
    ]
    missing_user_dids = [user.bluesky_user_did for user in missing_users]
    missing_user_handles = [user.bluesky_handle for user in missing_users]
    missing_user_dids_string = ",".join([f"'{did}'" for did in missing_user_dids])
    missing_user_handles_string = ",".join(
        [f"'{handle}'" for handle in missing_user_handles]
    )
    query = f"""
    SELECT *
    FROM user_social_networks
    WHERE follower_did IN ({missing_user_dids_string})
    OR follower_handle IN ({missing_user_handles_string})
    """
    df = athena.query_results_as_df(query)
    print(f"Fetched {len(df)} results for follows to export.")
    export_data_to_local_storage(service="scraped_user_social_network", df=df)
    print(f"Fetched {len(df)} results for follows to export.")


if __name__ == "__main__":
    main()
