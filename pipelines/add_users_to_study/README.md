# Add Users to Study

This pipeline contains utilities for managing users in a Bluesky study. It includes:

- `app.py`: A Streamlit web application that provides a UI for:
  - Adding individual users to the study by their Bluesky handle
  - Assigning users to experimental conditions (Reverse Chronological, Engagement, or Representative Diversification)
  - Viewing existing users grouped by their assigned conditions

- `add_users_from_csv_file.py`: A script to bulk import users from a CSV file containing Bluesky handles and condition assignments

- `add_previously_missing_users.py`: A utility to identify and add users who were missed in previous onboarding runs

- `add_synced_social_network_to_local_store.py`: A script that:
  - Fetches social network data for study users from S3/Athena
  - Exports that data to local storage for analysis

The pipeline handles validation of Bluesky handles, mapping of users to conditions, and maintains a record of study participants in DynamoDB.

To add users to the study, we need to:

- Add them to the DynamoDB table. This can be done through the Streamlit app or the `add_users_from_csv_file.py` script.
- Assign them to a condition. This can be done through the Streamlit app.
- Geth their social network data. This is actually done in `get_existing_user_social_network/src/get_existing_user_social_network/helper.py`,
which gets the social network data (follows/followers) from the Bluesky API and
stores it in S3.
- Run compaction in Quest. This can be done with `services/compact_all_services/helper.py`, with the `compact_migrate_s3_data_to_local_storage()` function for the "scraped_user_social_network" service. This will store the data in local storage, and then subsequent compaction runs will compact and dedupe the data.
