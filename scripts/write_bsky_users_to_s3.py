"""We keep the current study users in DynamoDB.

It would be nice to move them to S3 (and then process with Athena).

NOTE: the users will continue to be kept (and managed) in DynamoDB, but
this lets us, for example, join users with usage data.
"""

from datetime import datetime

import pandas as pd

from lib.aws.s3 import S3
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel


study_users: list[UserToBlueskyProfileModel] = get_all_users()
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
s3_key = f"exported_study_users/{timestamp}.json"

s3 = S3()


def main():
    study_users_df = pd.DataFrame(
        [user.dict() for user in study_users if user.is_study_user]
    )
    print(
        f"Writing {len(study_users_df)} in-study users (out of {len(study_users)} total users) to {s3_key}"
    )
    study_user_dicts = study_users_df.to_dict(orient="records")
    s3.write_dicts_jsonl_to_s3(data=study_user_dicts, key=s3_key)
    print(f"Done writing {len(study_user_dicts)} study users to {s3_key}")


if __name__ == "__main__":
    main()
