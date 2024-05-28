"""Back up SQL databases."""
import os

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


def backup_db_as_parquet(
    db_filepath: str,
    export_directory: str,
    compressed: bool = True
):
    # NOTE: we specify an export directory (probably timestamp) and then
    # just write a bunch of smaller compressed files to there.
    print(f"Exporting {db_filepath} as parquet to {export_directory}...")
    pass


def backup_db_as_json(
    db_filepath: str,
    export_directory: str,
    compressed: bool = True
):
    print(f"Exporting {db_filepath} as JSON to {export_directory}...")
    pass


def backup_db(
    db_filepath: str,
    formats: list[str],
    compressed: bool = True
):
    # strip ".db" from name (e.g., "foo.db" -> "foo")
    db_folder_name = os.path.basename(db_filepath)[:-3]

    # full folder path = <root backups directory>/<db folder name>/<timestamp>
    export_directory = os.path.join(
        root_data_backups_directory, db_folder_name, current_datetime_str
    )

    for format in formats:
        if format == "parquet":
            backup_db_as_parquet(
                db_filepath=db_filepath,
                export_directory=export_directory,
                compressed=compressed
            )
        elif format == "json":
            backup_db_as_json(
                db_filepath=db_filepath,
                export_directory=export_directory,
                compressed=compressed
            )


def main(
    formats: list[str] = ["parquet", "json"]
):
    db_filepaths: dict = load_db_filepaths()
    for db_filename, db_filepath in db_filepaths.items():
        print(f"Backing up {db_filename}...")
        backup_db(db_filepath=db_filepath, formats=formats)
        print(f"Backed up {db_filename}.")


if __name__ == "__main__":
    main()
