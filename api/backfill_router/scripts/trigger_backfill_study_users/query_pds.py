import sys
from api.backfill_router.api import run_pds_backfill_api

if __name__ == "__main__":
    config_path = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "api/backfill_router/config/examples/backfill_study_users.yaml"
    )
    run_pds_backfill_api(config_path)
