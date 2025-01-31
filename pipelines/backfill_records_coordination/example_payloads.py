"""Example payloads to use for backfills."""

payloads = {
    "perspective_api_insert_dummy_records_only": {
        "description": "Insert dummy records only, skip the run_integration step",
        "command": "python app.py --record-type posts --integration ml_inference_perspective_api --add-to-queue --no-run-integrations",
    },
    "perspective_api_run_integration_only": {
        "description": "Run the integration only, skip the insert_dummy_records step",
        "command": "python app.py --record-type posts --integration ml_inference_perspective_api --no-add-to-queue --run-integrations",
    },
    "sociopolitical_insert_dummy_records_only": {
        "description": "Insert dummy records only, skip the run_integration step",
        "command": "python app.py --record-type posts --integration ml_inference_sociopolitical --add-to-queue --no-run-integrations",
    },
    "sociopolitical_run_integration_only": {
        "description": "Run the integration only, skip the insert_dummy_records step",
        "command": "python app.py --record-type posts --integration ml_inference_sociopolitical --no-add-to-queue --run-integrations",
    },
    "ime_insert_dummy_records_only": {
        "description": "Insert dummy records only, skip the run_integration step",
        "command": "python app.py --record-type posts --integration ml_inference_ime --add-to-queue --no-run-integrations",
    },
    "ime_run_integration_only": {
        "description": "Run the integration only, skip the insert_dummy_records step",
        "command": "python app.py --record-type posts --integration ml_inference_ime --no-add-to-queue --run-integrations",
    },
}
