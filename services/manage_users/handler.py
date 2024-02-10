"""Service for managing user CRUD operations, such as adding new study
participants to the experiment.
"""

from services.manage_users.helper import create_and_insert_new_users


def main(event: dict, context: dict) -> int:
    """Manage all user-related CRUD operations."""
    operation = event["operation"]
    if operation == "insert":
        # insert new users from .csv
        filepath = event["filepath"]
        create_and_insert_new_users(filepath=filepath)


if __name__ == "__main__":
    event = {"operation": "insert", "filepath": "path/to/.csv"}
    context = {}
    main(event=event, context=context)
