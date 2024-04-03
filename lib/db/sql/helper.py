import peewee
from peewee import TextField
from playhouse.migrate import migrate, SqliteMigrator


# table_name comes from [class]._meta.table_name (e.g., FirehosePost._meta.table_name)
def add_new_column_to_table(
    cls,
    cursor,
    db: peewee.SqliteDatabase,
    colname: str
) -> None:
    """Adds a new column to the existing firehosepost table and backfills
    existing records with a null default value.

    Assumes TextField type (for now, though this can be changed).
    """
    table_name = cls._meta.table_name
    print(f"Adding new column {colname} to {table_name} table")
    migrator = SqliteMigrator(db)
    migrate(
        migrator.add_column(table_name, colname, TextField(null=True))
    )
    print(f"Added new column {colname} to {table_name} table")
    current_table_cols = [
        col[1] for col in cursor.execute(f"PRAGMA table_info({table_name})")
    ]
    print(f"Current columns in {table_name} table: {current_table_cols}")
