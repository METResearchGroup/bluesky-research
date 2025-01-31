"""Example payloads to use for backfills.

Three types of backfills:
1. Insert dummy records only, skip the run_integration step
2. Run the integration only, skip the insert_dummy_records step
3. Write cache buffers to database for all services

Can be done one at a time for a given service, to (1) insert dummy records,
(2) run the integration, and then (3) write cache buffers to database, in
order to mimick the full prod workflow for the integrations.
"""

payloads = {
    "perspective_api_insert_dummy_records_only": {
        "description": "Insert dummy records only, skip the run_integration step",
        "command": "python app.py --record-type posts --integration ml_inference_perspective_api --add-to-queue",
    },
    "perspective_api_run_integration_only": {
        "description": "Run the integration only, skip the insert_dummy_records step",
        "command": "python app.py --record-type posts --integration ml_inference_perspective_api --run-integrations",
    },
    "perspective_api_trigger_write_cache_buffers_to_db": {
        "description": "Write cache buffers to database for Perspective API",
        "command": "python app.py --record-type posts --write-cache ml_inference_perspective_api",
    },
    "sociopolitical_insert_dummy_records_only": {
        "description": "Insert dummy records only, skip the run_integration step",
        "command": "python app.py --record-type posts --integration ml_inference_sociopolitical --add-to-queue",
    },
    "sociopolitical_run_integration_only": {
        "description": "Run the integration only, skip the insert_dummy_records step",
        "command": "python app.py --record-type posts --integration ml_inference_sociopolitical --run-integrations",
    },
    "sociopolitical_trigger_write_cache_buffers_to_db": {
        "description": "Write cache buffers to database for Sociopolitical",
        "command": "python app.py --record-type posts --write-cache ml_inference_sociopolitical",
    },
    "ime_insert_dummy_records_only": {
        "description": "Insert dummy records only, skip the run_integration step",
        "command": "python app.py --record-type posts --integration ml_inference_ime --add-to-queue",
    },
    "ime_run_integration_only": {
        "description": "Run the integration only, skip the insert_dummy_records step",
        "command": "python app.py --record-type posts --integration ml_inference_ime --run-integrations",
    },
    "ime_trigger_write_cache_buffers_to_db": {
        "description": "Write cache buffers to database for IME",
        "command": "python app.py --record-type posts --write-cache ml_inference_ime",
    },
    "all_trigger_write_cache_buffers_to_db": {
        "description": "Write cache buffers to database for all services",
        "command": "python app.py --record-type posts --write-cache all",
    },
}
