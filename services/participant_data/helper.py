"""Helper functions for participant data service."""
import os
import sqlite3

from lib.db.sql.helper import (
    check_if_table_exists, create_table, get_all_table_results_as_df,
    write_to_database
)

current_file_directory = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB_NAME = "study_participants.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()


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

map_table_to_schema = {
    "study_users": study_users_schema,
    "study_questionnaire_data": study_questionnaire_data_schema,
    "user_to_bsky_profile": user_to_bsky_profile_create_schema
}


def generate_create_table_query(table_name: str) -> str:
    """Generates a CREATE TABLE query for the given table name."""
    schema = map_table_to_schema.get(table_name)
    if schema is None:
        raise ValueError(f"Table name {table_name} not found in schema mapping.") # noqa
    return f"CREATE TABLE {table_name} ({schema});"


def test_insertion() -> None:
    """Tests if table creation is successful. Will fail if the test has already
    been run before, so should only be run at the start.
    """
    map_table_name = "user_to_bsky_profile"
    print(f"Testing insertion of a user into the {map_table_name} table...")
    user_id = 999
    bluesky_handle = "markptorres.bsky.social"
    bluesky_user_did = "did:plc:w5mjarupsl6ihdrzwgnzdh4y"
    data = {
        "study_user_id": user_id,
        "bluesky_handle": bluesky_handle,
        "bluesky_user_did": bluesky_user_did   
    }
    write_to_database(
        conn=conn, cursor=cursor, table_name=map_table_name, data=data
    )
    df = get_all_table_results_as_df(conn=conn, table_name=map_table_name)
    assert df.shape[0] == 1
    print(f"Successfully inserted a user into the {map_table_name} table.")


def initialize_study_tables() -> None:
    """Initializes the three tables needed to store participant data."""
    for table in map_table_to_schema.keys():
        create_table(conn=conn, cursor=cursor, table_name=table)
        check_if_table_exists(cursor=cursor, table_name=table)
        print(f"Table {table} created successfully.")
    test_insertion()
