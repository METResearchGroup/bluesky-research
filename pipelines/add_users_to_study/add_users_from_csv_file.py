"""Adds users from the provided .csv file."""

# 1. Add users from users.csv
# 2. Delete users from spam_users.csv, if they exist.


def add_users_from_csv_file(csv_file_path: str):
    """Adds users from the provided .csv file."""
    pass


def delete_users_from_csv_file(csv_file_path: str):
    """Deletes users from the provided .csv file."""
    pass


if __name__ == "__main__":
    csv_files_of_users_to_add = ["users_1.csv"]
    csv_files_of_users_to_delete = ["spam_users_1.csv"]

    print(f"Adding users from {csv_files_of_users_to_add}")
    for csv_file_path in csv_files_of_users_to_add:
        add_users_from_csv_file(csv_file_path)

    print(f"Deleting users from {csv_files_of_users_to_delete}")
    for csv_file_path in csv_files_of_users_to_delete:
        delete_users_from_csv_file(csv_file_path)
