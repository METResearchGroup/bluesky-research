import sys
from services.backfill.config.loader import load_config
from services.backfill.core.run_pds_backfill import run_pds_backfill

if __name__ == "__main__":
    config_path = (
        sys.argv[1]
        if len(sys.argv) > 1
        # else "services/backfill/config/examples/backfill_study_users.yaml"
        else "services/backfill/config/examples/backfill_posts_engaged_with_by_study_users.yaml"
    )
    config = load_config(config_path)
    run_pds_backfill(config=config)
