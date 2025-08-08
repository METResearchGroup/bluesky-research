#!/usr/bin/env python3
"""
DataWriter Prefect Flow

This flow processes Redis Streams data into partitioned Parquet files.
It handles all 5 Bluesky data types: posts, likes, reposts, follows, blocks.

Author: AI Assistant
Date: 2025-08-08
"""

import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Any
from pathlib import Path

import redis
import polars as pl
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task(name="connect_to_redis", retries=3, retry_delay_seconds=5)
def connect_to_redis() -> redis.Redis:
    """Connect to Redis with retry logic"""
    logger = get_run_logger()
    logger.info("🔗 Connecting to Redis...")

    try:
        r = redis.Redis(
            host="localhost",
            port=6379,
            decode_responses=True,
            socket_connect_timeout=10,
            socket_timeout=10,
            retry_on_timeout=True,
        )
        r.ping()
        logger.info("✅ Redis connection successful")
        return r
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        raise


@task(name="get_stream_data", retries=2, retry_delay_seconds=3)
def get_stream_data(
    redis_client: redis.Redis,
    stream_name: str,
    batch_size: int = 1000,
    timeout_ms: int = 5000,
) -> List[Dict[str, Any]]:
    """Get data from Redis Stream with consumer group"""
    logger = get_run_logger()
    logger.info(f"📥 Reading from stream: {stream_name}")

    try:
        # Read from stream with consumer group
        messages = redis_client.xreadgroup(
            groupname="datawriter_group",
            consumername="datawriter_consumer",
            streams={stream_name: ">"},
            count=batch_size,
            block=timeout_ms,
        )

        if not messages:
            logger.info(f"📭 No messages in stream: {stream_name}")
            return []

        # Extract messages from the response
        stream_messages = messages[0][1] if messages else []
        parsed_messages = []

        for msg_id, fields in stream_messages:
            # Parse the message data
            message_data = json.loads(fields.get("data", "{}"))
            parsed_messages.append(
                {
                    "id": msg_id,
                    "data": message_data,
                    "timestamp": fields.get("timestamp", time.time()),
                }
            )

        logger.info(f"📊 Retrieved {len(parsed_messages)} messages from {stream_name}")
        return parsed_messages

    except Exception as e:
        logger.error(f"❌ Error reading from stream {stream_name}: {e}")
        raise


@task(name="process_messages_by_type")
def process_messages_by_type(
    messages: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Group messages by data type"""
    logger = get_run_logger()

    grouped_messages = {
        "app.bsky.feed.post": [],
        "app.bsky.feed.like": [],
        "app.bsky.feed.repost": [],
        "app.bsky.graph.follow": [],
        "app.bsky.graph.block": [],
    }

    for message in messages:
        data = message.get("data", {})
        collection = data.get("collection", "unknown")

        if collection in grouped_messages:
            grouped_messages[collection].append(message)
        else:
            logger.warning(f"⚠️ Unknown collection type: {collection}")

    # Log summary
    for collection, msgs in grouped_messages.items():
        if msgs:
            logger.info(f"📋 {collection}: {len(msgs)} messages")

    return grouped_messages


@task(name="write_parquet_files", retries=2, retry_delay_seconds=5)
def write_parquet_files(
    grouped_messages: Dict[str, List[Dict[str, Any]]], base_output_dir: str = "./data"
) -> Dict[str, str]:
    """Write messages to partitioned Parquet files"""
    logger = get_run_logger()

    written_files = {}
    current_time = datetime.now()

    # Create base output directory
    output_dir = Path(base_output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for collection, messages in grouped_messages.items():
        if not messages:
            continue

        logger.info(f"💾 Writing {len(messages)} messages for {collection}")

        # Create partitioned directory structure
        # Format: year/month/day/hour/type
        partition_path = (
            output_dir
            / str(current_time.year)
            / f"{current_time.month:02d}"
            / f"{current_time.day:02d}"
            / f"{current_time.hour:02d}"
            / collection.replace(".", "_")
        )
        partition_path.mkdir(parents=True, exist_ok=True)

        # Prepare data for Polars
        data_rows = []
        for message in messages:
            data = message.get("data", {})
            # Add metadata
            data["_message_id"] = message.get("id")
            data["_timestamp"] = message.get("timestamp")
            data["_processed_at"] = current_time.isoformat()
            data_rows.append(data)

        if data_rows:
            # Create Polars DataFrame
            df = pl.DataFrame(data_rows)

            # Generate filename with timestamp
            timestamp_str = current_time.strftime("%Y%m%d_%H%M%S")
            filename = f"data_{timestamp_str}_{collection.replace('.', '_')}.parquet"
            filepath = partition_path / filename

            # Write Parquet file
            df.write_parquet(str(filepath))

            written_files[collection] = str(filepath)
            logger.info(f"✅ Wrote {len(data_rows)} records to {filepath}")

    return written_files


@task(name="acknowledge_messages")
def acknowledge_messages(
    redis_client: redis.Redis, stream_name: str, message_ids: List[str]
) -> bool:
    """Acknowledge processed messages in Redis Stream"""
    logger = get_run_logger()

    if not message_ids:
        logger.info("📭 No messages to acknowledge")
        return True

    try:
        # Acknowledge messages
        redis_client.xack(stream_name, "datawriter_group", *message_ids)
        logger.info(f"✅ Acknowledged {len(message_ids)} messages in {stream_name}")
        return True
    except Exception as e:
        logger.error(f"❌ Error acknowledging messages: {e}")
        return False


@task(name="cleanup_old_messages")
def cleanup_old_messages(
    redis_client: redis.Redis, stream_name: str, max_messages: int = 100000
) -> bool:
    """Clean up old messages from Redis Stream"""
    logger = get_run_logger()

    try:
        # Get stream info
        stream_info = redis_client.xinfo_stream(stream_name)
        current_length = stream_info["length"]

        if current_length > max_messages:
            # Trim stream to keep only recent messages
            redis_client.xtrim(stream_name, maxlen=max_messages, approximate=True)
            logger.info(f"🧹 Trimmed {stream_name} to {max_messages} messages")
        else:
            logger.info(
                f"📊 {stream_name} has {current_length} messages (within limit)"
            )

        return True
    except Exception as e:
        logger.error(f"❌ Error cleaning up stream {stream_name}: {e}")
        return False


@task(name="create_processing_summary")
def create_processing_summary(
    processed_counts: Dict[str, int],
    written_files: Dict[str, str],
    processing_time: float,
) -> str:
    """Create a summary of the processing run"""
    logger = get_run_logger()

    summary = f"""
# DataWriter Processing Summary

## Processing Statistics
- **Total Processing Time**: {processing_time:.2f} seconds
- **Messages Processed**: {sum(processed_counts.values())}

## Messages by Type
"""

    for collection, count in processed_counts.items():
        summary += f"- **{collection}**: {count} messages\n"

    summary += "\n## Files Written\n"
    for collection, filepath in written_files.items():
        summary += f"- **{collection}**: {filepath}\n"

    summary += "\n## Performance Metrics\n"
    summary += f"- **Processing Rate**: {sum(processed_counts.values()) / processing_time:.2f} messages/sec\n"
    summary += f"- **Files Created**: {len(written_files)}\n"

    logger.info("📊 Processing summary created")
    return summary


@flow(
    name="datawriter-flow",
    description="Process Redis Streams to partitioned Parquet files",
)
def datawriter_flow(
    stream_names: List[str] = None,
    batch_size: int = 1000,
    output_dir: str = "./data",
    max_stream_length: int = 100000,
) -> Dict[str, Any]:
    """
    Main DataWriter flow that processes Redis Streams into partitioned Parquet files.

    Args:
        stream_names: List of Redis stream names to process
        batch_size: Number of messages to process per batch
        output_dir: Base directory for Parquet file output
        max_stream_length: Maximum number of messages to keep in streams

    Returns:
        Dictionary with processing results and statistics
    """
    logger = get_run_logger()
    start_time = time.time()

    logger.info("🚀 Starting DataWriter flow")

    # Default stream names if not provided
    if stream_names is None:
        stream_names = [
            "bluesky_posts",
            "bluesky_likes",
            "bluesky_reposts",
            "bluesky_follows",
            "bluesky_blocks",
        ]

    # Connect to Redis
    redis_client = connect_to_redis()

    total_processed = 0
    all_written_files = {}
    processing_results = {}

    try:
        for stream_name in stream_names:
            logger.info(f"🔄 Processing stream: {stream_name}")

            # Get messages from stream
            messages = get_stream_data(redis_client, stream_name, batch_size)

            if not messages:
                logger.info(f"📭 No messages in stream {stream_name}")
                continue

            # Process messages by type
            grouped_messages = process_messages_by_type(messages)

            # Write Parquet files
            written_files = write_parquet_files(grouped_messages, output_dir)

            # Acknowledge processed messages
            message_ids = [msg["id"] for msg in messages]
            ack_success = acknowledge_messages(redis_client, stream_name, message_ids)

            # Cleanup old messages
            cleanup_success = cleanup_old_messages(
                redis_client, stream_name, max_stream_length
            )

            # Track results
            stream_processed = len(messages)
            total_processed += stream_processed
            all_written_files.update(written_files)

            processing_results[stream_name] = {
                "messages_processed": stream_processed,
                "files_written": len(written_files),
                "acknowledged": ack_success,
                "cleanup_success": cleanup_success,
            }

            logger.info(
                f"✅ Completed processing {stream_name}: {stream_processed} messages"
            )

        # Calculate processing time
        processing_time = time.time() - start_time

        # Create summary
        summary = create_processing_summary(
            {
                stream: results["messages_processed"]
                for stream, results in processing_results.items()
            },
            all_written_files,
            processing_time,
        )

        # Create artifact
        create_markdown_artifact(
            key="datawriter-summary",
            markdown=summary,
            description="DataWriter processing summary",
        )

        logger.info("🎉 DataWriter flow completed successfully!")
        logger.info(f"📊 Total messages processed: {total_processed}")
        logger.info(f"⏱️ Processing time: {processing_time:.2f} seconds")
        logger.info(f"📁 Files written: {len(all_written_files)}")

        return {
            "success": True,
            "total_processed": total_processed,
            "processing_time": processing_time,
            "files_written": all_written_files,
            "stream_results": processing_results,
            "summary": summary,
        }

    except Exception as e:
        logger.error(f"❌ DataWriter flow failed: {e}")
        return {
            "success": False,
            "error": str(e),
            "total_processed": total_processed,
            "processing_time": time.time() - start_time,
        }

    finally:
        # Close Redis connection
        try:
            redis_client.close()
            logger.info("🔌 Redis connection closed")
        except Exception:
            pass


if __name__ == "__main__":
    # Run the flow directly for testing
    result = datawriter_flow()
    print(json.dumps(result, indent=2))
