import os
import sqlite3

import pandas as pd

from lib.db.sql.constants import (
    TABLE_NAME_TO_KEYS_MAP, TABLE_NAME_TO_SCHEMA_MAP
)
from lib.log.logger import Logger

logger = Logger(__name__)
current_file_directory = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB_NAME = "data.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)


def get_default_conn_cursor() -> tuple:
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()
    return conn, cursor


def generate_create_table_statement(table_name: str) -> str:
    schema = TABLE_NAME_TO_SCHEMA_MAP[table_name]
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema}
        )
    """


def create_table(
    conn: sqlite3.Connection, cursor: sqlite3.Cursor, table_name: str
) -> None:
    create_table_statement = generate_create_table_statement(table_name)
    cursor.execute(create_table_statement)
    conn.commit()


def check_if_table_exists(cursor: sqlite3.Cursor, table_name: str) -> bool:
    query = f"SELECT name FROM sqlite_master WHERE type='table' AND name=?"
    cursor.execute(query, (table_name,))
    result = cursor.fetchone()
    return result is not None


def write_to_database(
    conn: sqlite3.Connection, cursor: sqlite3.Cursor, table_name: str, data: dict
) -> None:
    columns = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    values = tuple(data.values())
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    cursor.execute(insert_query, values)
    conn.commit()


def get_column(cursor: sqlite3.Cursor, table_name: str, column: str) -> list:
    query = f"SELECT {column} FROM {table_name}"
    cursor.execute(query)
    results = cursor.fetchall()
    return [row[0] for row in results]


def single_row_insertion_is_valid(row_data: dict, table_name: str) -> bool:
    """Check if the data should be inserted into SQLite DB.

    Shouldn't be inserted if the PK of the data isn't unique in the DB.
    Function is generic enough if we want to add extra checks.

    Assumes that row_data is a dict corresponding to the data for one row.
    """
    table_pk = TABLE_NAME_TO_KEYS_MAP[table_name]["primary"][0]
    row_pk_value = row_data.get(table_pk, None)
    if row_pk_value is None:
        logger.warning(
            f"Insertion into {table_name} invalid: data lacks {table_pk} PK."
        )
        return False
    col = get_column(table_name=table_name, column=table_pk)
    return row_pk_value not in col  # only insert if PK is unique.


def get_all_table_results_as_df(conn: sqlite3.Connection, table_name: str) -> pd.DataFrame:
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        logger.info(f"Error getting all table results as df: {e}")
        logger.info("Returning empty df.")
        return pd.DataFrame()
