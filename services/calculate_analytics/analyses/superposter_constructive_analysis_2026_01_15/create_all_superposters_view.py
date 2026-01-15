"""
Create (or replace) an Athena view containing all superposter author DIDs.
"""

from __future__ import annotations

from lib.aws.athena import Athena


DEFAULT_DB_NAME = "default_db"
DEFAULT_WORKGROUP = "primary"
DEFAULT_OUTPUT_LOCATION = "s3://bluesky-research/athena-results/"

DEFAULT_VIEW_NAME = "superposter_author_dids"
SOURCE_TABLE = "archive_daily_superposters"


def build_view_ddl(db_name: str, view_name: str) -> str:
    return f"""
CREATE OR REPLACE VIEW {db_name}.{view_name} AS
WITH parsed AS (
  SELECT
    partition_date,
    COALESCE(
      TRY(CAST(json_parse(superposters) AS ARRAY(ROW(author_did VARCHAR, count BIGINT)))),
      CAST(ARRAY[] AS ARRAY(ROW(author_did VARCHAR, count BIGINT)))
    ) AS sp
  FROM {db_name}.{SOURCE_TABLE}
)
SELECT DISTINCT
  parsed.partition_date,
  x.author_did
FROM parsed
CROSS JOIN UNNEST(sp) AS u(x)
WHERE x.author_did IS NOT NULL
  AND parsed.partition_date IS NOT NULL
""".strip()


def main() -> None:
    ddl = build_view_ddl(db_name=DEFAULT_DB_NAME, view_name=DEFAULT_VIEW_NAME)

    athena = Athena()
    athena.run_query(
        query=ddl,
        db_name=DEFAULT_DB_NAME,
        workgroup=DEFAULT_WORKGROUP,
        output_location=DEFAULT_OUTPUT_LOCATION,
    )

    print(f"Created/updated view: {DEFAULT_DB_NAME}.{DEFAULT_VIEW_NAME}")


if __name__ == "__main__":
    main()
