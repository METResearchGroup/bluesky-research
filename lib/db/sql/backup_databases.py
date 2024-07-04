"""Back up SQL databases.

Stores in a new directory, `bluesky_research_data_backups`, outside of Git
version control.

Requires pyarrow and fastparquet.

The resulting filesystem will look something like this.

    - bluesky_research_data_backups
        - <db_name>
            - <timestamp>
                - parquet
                    - <table_name>.parquet
                - json
                    - <table_name>.json
        - <db_name>
            - <timestamp>
                - parquet
                    - <table_name>.parquet
                - json
                    - <table_name>.json
        - <db_name>
            - <timestamp>
                - parquet
                    - <table_name>.parquet
                - json
                    - <table_name>.json
"""
import gzip
import os
import sqlite3

import pandas as pd

from lib.constants import current_datetime_str

current_file_directory = os.path.dirname(os.path.abspath(__file__))

root_directory = os.path.abspath(
    os.path.join(current_file_directory, '../../../..')
)
root_data_backups_directory_name = "bluesky_research_data_backups"
root_data_backups_directory = os.path.join(
    root_directory, root_data_backups_directory_name
)


def load_db_filepaths(directory: str = current_file_directory) -> dict:
    """Loads all DB filepaths in the directory. Looks for '.db' extension.

    Returns a dictionary whose key = db filename and whose value = db filepath.
    """
    res = {}
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".db"):
                res[file] = os.path.join(root, file)
    return res


def backup_db(
    db_filepath: str, export_formats: list[str], compressed: bool = True
):
    # strip ".db" from name (e.g., "foo.db" -> "foo")
    db_folder_name = os.path.basename(db_filepath)[:-3]

    # full folder path = <root backups directory>/<db folder name>/<timestamp>
    export_directory = os.path.join(
        root_data_backups_directory, db_folder_name, current_datetime_str
    )

    print(f"Exporting {db_filepath} as {' and '.join(export_formats)} to {export_directory}...")  # noqa
    os.makedirs(export_directory, exist_ok=True)

    conn = sqlite3.connect(db_filepath)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table_name in tables:
        table_name = table_name[0]
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        for export_format in export_formats:
            if export_format == "parquet":
                print(f"Exporting table {table_name} as parquet...")
                parquet_export_dir = os.path.join(export_directory, "parquet")
                parquet_filepath = os.path.join(
                    parquet_export_dir, f"{table_name}.parquet"
                )
                os.makedirs(parquet_export_dir, exist_ok=True)
                compression = 'gzip' if compressed else None
                df.to_parquet(parquet_filepath, compression=compression)
                print(f"Finished table {table_name} as parquet to {parquet_filepath}...")  # noqa
            elif export_format == "json":
                print(f"Exporting table {table_name} as JSON...")
                json_export_dir = os.path.join(export_directory, "json")
                json_filepath = os.path.join(
                    json_export_dir, f"{table_name}.json")
                os.makedirs(json_export_dir, exist_ok=True)
                json_str = df.to_json(orient='records', lines=True)
                if compressed:
                    with gzip.open(json_filepath + '.gz', 'wt', encoding='utf-8') as gz_file:
                        gz_file.write(json_str)
                else:
                    with open(json_filepath, 'w', encoding='utf-8') as json_file:
                        json_file.write(json_str)
                print(
                    f"Finished table {table_name} as JSON to {json_filepath}...")

    conn.close()


def load_from_backup(
    db_name: str, timestamp: str, table_name: str, export_format: str
) -> pd.DataFrame:
    """Loads a table from a backup.

    Args:
        db_name: The name of the database.
        timestamp: The timestamp of the backup.
        table_name: The name of the table.
        export_format: The format of the backup (e.g., "parquet", "json").

    Returns:
        A pandas DataFrame.
    """
    backup_directory = os.path.join(
        root_data_backups_directory, db_name, timestamp, export_format
    )
    if export_format == "parquet":
        return pd.read_parquet(os.path.join(backup_directory, f"{table_name}.parquet"))
    elif export_format == "json":
        with open(os.path.join(backup_directory, f"{table_name}.json"), 'r') as f:
            return pd.read_json(f, lines=True)
    else:
        raise ValueError(f"Unsupported export format: {export_format}")


def _test_load_data():
    """Tests loading data from a backup."""
    db_name = "filtered_posts"
    timestamp = "2024-05-29-09:14:42"
    table_name = "filteredpreprocessedposts"
    export_format = "parquet"
    df = load_from_backup(db_name, timestamp, table_name, export_format)
    print(df.head())
    assert df.shape[0] > 0


def main(export_formats: list[str] = ["parquet", "json"]):
    db_filepaths: dict = load_db_filepaths()
    for db_filename, db_filepath in db_filepaths.items():
        print(f"Backing up {db_filename}...")
        backup_db(db_filepath=db_filepath, export_formats=export_formats)
        print(f"Backed up {db_filename}.")
    print("Completed backing up all SQL databases.")


if __name__ == "__main__":
    main()
