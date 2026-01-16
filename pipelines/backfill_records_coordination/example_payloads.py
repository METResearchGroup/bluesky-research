"""Example payloads to use for backfills.

Three types of backfills:
1. Enqueue records only, skip running integrations
2. Run integrations only (process existing queued records)
3. Write cache buffers to database for one or more services

These can be done one at a time for a given service, to (1) enqueue records,
(2) run the integration, and then (3) write cache buffers to the database.
"""

payloads = {
    "backfill_posts_used_in_feeds_enqueue_only": {
        "description": """
            Enqueue records only, skip running integrations. Determine which posts
            were used in feeds for a given date range, and enqueue those posts for
            backfill.
        """,
        "command": """
            python app.py --record-type posts_used_in_feeds --add-to-queue
            --start-date 2024-01-01 --end-date 2024-01-31
        """,
    },
    "perspective_api_enqueue_only": {
        "description": "Enqueue records only, skip running integrations",
        "command": "python app.py --record-type posts --integrations ml_inference_perspective_api --add-to-queue",
    },
    "perspective_api_run_integration_only": {
        "description": "Run integrations only (process existing queued records)",
        "command": "python app.py --record-type posts --integrations ml_inference_perspective_api --run-integrations",
    },
    "perspective_api_run_integration_only_no_record_type": {
        "description": "Run integrations only (process existing queued records). Works the same with or without the `--record-type` flag.",
        "command": "python app.py --integrations ml_inference_perspective_api --run-integrations",
    },
    "perspective_api_trigger_write_cache_buffers_to_db": {
        "description": "Write cache buffers to database for Perspective API",
        "command": "python app.py --record-type posts --write-cache-buffer-to-storage --service-source-buffer ml_inference_perspective_api",
    },
    "sociopolitical_enqueue_only": {
        "description": "Enqueue records only, skip running integrations",
        "command": "python app.py --record-type posts --integrations ml_inference_sociopolitical --add-to-queue",
    },
    "sociopolitical_run_integration_only": {
        "description": "Run integrations only (process existing queued records)",
        "command": "python app.py --record-type posts --integrations ml_inference_sociopolitical --run-integrations",
    },
    "sociopolitical_trigger_write_cache_buffers_to_db": {
        "description": "Write cache buffers to database for Sociopolitical",
        "command": "python app.py --record-type posts --write-cache-buffer-to-storage --service-source-buffer ml_inference_sociopolitical",
    },
    "ime_enqueue_only": {
        "description": "Enqueue records only, skip running integrations",
        "command": "python app.py --record-type posts --integrations ml_inference_ime --add-to-queue",
    },
    "ime_run_integration_only": {
        "description": "Run integrations only (process existing queued records)",
        "command": "python app.py --record-type posts --integrations ml_inference_ime --run-integrations",
    },
    "ime_trigger_write_cache_buffers_to_db": {
        "description": "Write cache buffers to database for IME",
        "command": "python app.py --record-type posts --write-cache-buffer-to-storage --service-source-buffer ml_inference_ime",
    },
    "all_trigger_write_cache_buffers_to_db": {
        "description": "Write cache buffers to database for all services",
        "command": "python app.py --record-type posts --write-cache-buffer-to-storage --service-source-buffer all",
    },
}
