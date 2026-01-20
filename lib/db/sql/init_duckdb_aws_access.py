"""One-off script to run for enabling AWS access in DuckDB."""

import duckdb


def set_aws_access_in_duckdb():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")

    con.execute("""
    CREATE PERSISTENT SECRET aws_default_s3 (
        TYPE s3,
        PROVIDER credential_chain
    );
    """)
