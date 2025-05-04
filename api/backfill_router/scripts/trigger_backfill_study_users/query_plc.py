import sys
from api.backfill_router.api import run_query_plc

if __name__ == "__main__":
    config_path = (
        sys.argv[1]
        if len(sys.argv) > 1
        # else "api/backfill_router/config/examples/backfill_study_users.yaml"
        else "api/backfill_router/config/examples/backfill_posts_liked_by_study_users.yaml"
    )
    run_query_plc(config_path)
