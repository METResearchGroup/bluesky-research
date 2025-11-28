"""Migrate research data to S3."""

PREFIXES_TO_MIGRATE = []


def main():
    """Migrate research data to S3.

    Basic algorithm setup:
    1. Define the prefixes to migrate.
    2. Set up a SQLite DB to track the migration status of each
    prefix.
    3. For each prefix, (a) migrate to S3 and (b) update the SQLite
    DB with the migration status.
    4. Iterate until all prefixes are migrated.
    """
    pass


if __name__ == "__main__":
    pass
