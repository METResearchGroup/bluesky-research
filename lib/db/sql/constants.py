### Participant data ###
study_users_schema = """
    id INTEGER PRIMARY KEY,
    name TEXT
""" # to be updated later, just start with basic name and ID
study_questionnaire_data_schema = """ 
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    questionnaire_data TEXT,
    FOREIGN KEY(user_id) REFERENCES study_users(id)
"""# to be defined later.
user_to_bsky_profile_create_schema = """
    study_user_id TEXT,
    bluesky_handle TEXT,
    bluesky_user_did TEXT,
    PRIMARY KEY (study_user_id, bluesky_handle, bluesky_user_did)
"""


### all tables ###
TABLE_NAME_TO_SCHEMA_MAP = {
    "study_users": study_users_schema,
    "study_questionnaire_data": study_questionnaire_data_schema,
    "user_to_bsky_profile": user_to_bsky_profile_create_schema
}

TABLE_NAME_TO_KEYS_MAP = {}
