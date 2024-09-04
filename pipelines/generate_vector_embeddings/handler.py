import json
import traceback

from lib.log.logger import get_logger
from services.generate_vector_embeddings.helper import do_vector_embeddings

logger = get_logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info("Starting vector embedding generation.")
        do_vector_embeddings()
        logger.info("Completed vector embedding generation.")
        return {
            "statusCode": 200,
            "body": json.dumps("Vector generation completed successfully"),
        }
    except Exception as e:
        logger.error(f"Error in vector generation pipeline: {e}")
        logger.error(traceback.format_exc())
        logger.error(
            json.dumps(
                {
                    "statusCode": 500,
                    "body": json.dumps(
                        f"Error in vector generation pipeline: {str(e)}"
                    ),
                }
            )
        )
        raise


if __name__ == "__main__":
    lambda_handler(None, None)
    # import pyarrow.parquet as pq
    # import s3fs

    # # Initialize S3 filesystem
    # s3 = s3fs.S3FileSystem()

    # # Path to your Parquet file
    # parquet_file_path = 's3://bluesky-research/vector_embeddings/in_network_post_embeddings/2024-08-25-02:37:11.parquet'

    # # Read the Parquet file
    # parquet_file = pq.ParquetFile(s3.open(parquet_file_path))

    # # Print the schema
    # print(parquet_file.schema)
    # breakpoint()
