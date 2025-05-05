# Naming convention:
# - input_<service_name> = queue for input data to the service (waiting for ingestion by service)
# - output_<service_name> = queue for output data from the service (waiting for write to DB).
# Creating a map so the queue names can be easily changed on the backend with minimal
# changes to the code.

NAME_TO_QUEUE_NAME_MAP = {
    "input_ml_inference_perspective_api": "input_ml_inference_perspective_api",
    "output_ml_inference_perspective_api": "output_ml_inference_perspective_api",
    "input_ml_inference_sociopolitical": "input_ml_inference_sociopolitical",
    "output_ml_inference_sociopolitical": "output_ml_inference_sociopolitical",
    "input_ml_inference_ime": "input_ml_inference_ime",
    "output_ml_inference_ime": "output_ml_inference_ime",
    "jetstream_sync": "jetstream_sync",
    "output_backfill_sync": "output_backfill_sync",
    "output_backfill_sync_block": "output_backfill_sync_block",
    "output_backfill_sync_follow": "output_backfill_sync_follow",
    "output_backfill_sync_like": "output_backfill_sync_like",
    "output_backfill_sync_post": "output_backfill_sync_post",
    "output_backfill_sync_reply": "output_backfill_sync_reply",
    "output_backfill_sync_repost": "output_backfill_sync_repost",
    "input_backfill_sync": "input_backfill_sync",
    "input_preprocess_raw_data": "input_preprocess_raw_data",
    "output_preprocess_raw_data": "output_preprocess_raw_data",
}
