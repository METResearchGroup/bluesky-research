# Backfill study users

Backfill data from study users.

1. Load study users. Write to SQLite. Defined in `load_study_users.py`. Creates a SQLite DB where each row has a list of DIDs.
2. Run backfill for the user's engagement activity. First, runs `query_plc_endpoint.py` to get the PLC docs for each DID and then saves them. Then, runs `manager.py` to run the PDS backfills for each DID.
3. Write to `/raw_sync`.
