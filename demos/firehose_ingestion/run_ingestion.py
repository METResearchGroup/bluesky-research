#!/usr/bin/env python
"""Bluesky Firehose Ingestion Demo Script.

This script demonstrates how to:
1. Connect to the Bluesky firehose
2. Ingest records into a SQLite database
3. Query and analyze the ingested data

Example usage:
    python -m demos.firehose_ingestion.run_ingestion --target_count 1000 --collections app.bsky.feed.post
"""

import argparse
import asyncio
import json
import time
from collections import Counter
from pprint import pprint
from typing import Dict, List, Set, Union

from demos.firehose_ingestion.db import FirehoseDB
from demos.firehose_ingestion.jetstream_connector import JetstreamConnector
from lib.log.logger import get_logger

logger = get_logger(__file__)


def analyze_record_types(db: FirehoseDB, limit: int = 1000) -> Dict:
    """Analyze the record types stored in the database.
    
    Args:
        db: FirehoseDB instance to query
        limit: Maximum number of records to analyze
        
    Returns:
        Dictionary with record type statistics
    """
    records = db.get_records(limit=limit)
    
    if not records:
        return {"error": "No records found"}
    
    # Analyze record types
    record_types = []
    operations = []
    
    for record in records:
        # Parse the JSON record
        record_data = json.loads(record.record)
        record_type = record_data.get("$type", "unknown")
        record_types.append(record_type)
        operations.append(record.operation)
    
    # Count frequencies
    type_counts = dict(Counter(record_types))
    operation_counts = dict(Counter(operations))
    
    return {
        "record_counts": {
            "total": len(records),
            "by_type": type_counts,
            "by_operation": operation_counts
        },
        "sample_record": json.loads(records[0].record) if records else None
    }


def analyze_post_contents(db: FirehoseDB, limit: int = 100) -> Dict:
    """Analyze post contents for text length, mentions, URLs, etc.
    
    Args:
        db: FirehoseDB instance to query
        limit: Maximum number of posts to analyze
        
    Returns:
        Dictionary with post content statistics
    """
    # Get posts specifically
    records = db.get_records(limit=limit)
    posts = []
    
    for record in records:
        record_data = json.loads(record.record)
        if record_data.get("$type") == "app.bsky.feed.post":
            posts.append(record_data)
    
    if not posts:
        return {"error": "No posts found"}
    
    # Analyze text lengths
    text_lengths = [len(post.get("text", "")) for post in posts]
    avg_length = sum(text_lengths) / len(text_lengths) if text_lengths else 0
    
    # Count mentions
    mention_counts = []
    for post in posts:
        # A simple estimate - actual parsing would be more complex
        mentions = post.get("text", "").count("@")
        mention_counts.append(mentions)
    
    # Check for media
    media_counts = []
    for post in posts:
        # Check for embed - this is a simplification
        has_media = "embed" in post
        media_counts.append(1 if has_media else 0)
    
    return {
        "post_stats": {
            "count": len(posts),
            "text_length": {
                "average": avg_length,
                "min": min(text_lengths) if text_lengths else 0,
                "max": max(text_lengths) if text_lengths else 0
            },
            "mentions": {
                "total": sum(mention_counts),
                "average": sum(mention_counts) / len(mention_counts) if mention_counts else 0
            },
            "media": {
                "count": sum(media_counts),
                "percentage": sum(media_counts) * 100 / len(posts) if posts else 0
            }
        },
        "sample_post": posts[0] if posts else None
    }


async def run_ingestion(
    db_name: str,
    instance: str,
    collections: Union[str, List[str]],
    target_count: int,
    max_time: int
) -> Dict:
    """Run the firehose ingestion process.
    
    Args:
        db_name: Name for the database
        instance: Jetstream instance to connect to
        collections: Collection(s) to subscribe to
        target_count: Target number of records to ingest
        max_time: Maximum time to run in seconds
        
    Returns:
        Dictionary with ingestion statistics
    """
    # Initialize the connector
    connector = JetstreamConnector(db_name=db_name, create_new_db=True)
    
    # Run the ingestion
    stats = await connector.listen_until_count(
        instance=instance,
        wanted_collections=collections,
        target_count=target_count,
        max_time=max_time,
    )
    
    return stats


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Run Bluesky firehose ingestion',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--db_name', type=str, 
                        help='Name for the database. If not provided, a timestamp will be used.')
    parser.add_argument('--instance', type=str, default="jetstream2.us-east.bsky.network",
                        help='Jetstream instance to connect to')
    parser.add_argument('--collections', type=str, default="app.bsky.feed.post",
                        help='Comma-separated list of collections to subscribe to')
    parser.add_argument('--target_count', type=int, default=1000,
                        help='Target number of records to ingest')
    parser.add_argument('--max_time', type=int, default=300,
                        help='Maximum time to run in seconds')
    parser.add_argument('--analysis', action='store_true',
                        help='Perform analysis on ingested data')
    
    args = parser.parse_args()
    
    # Split collections if comma-separated
    collections = args.collections.split(',') if ',' in args.collections else args.collections
    
    # Generate db_name if not provided
    db_name = args.db_name
    
    # Run the ingestion
    logger.info(f"Starting firehose ingestion with target count: {args.target_count}")
    loop = asyncio.get_event_loop()
    stats = loop.run_until_complete(
        run_ingestion(
            db_name=db_name,
            instance=args.instance,
            collections=collections,
            target_count=args.target_count,
            max_time=args.max_time
        )
    )
    
    logger.info("Ingestion complete. Statistics:")
    pprint(stats)
    
    # Perform analysis if requested
    if args.analysis and stats.get("records_stored", 0) > 0:
        db_name = db_name or f"firehose_{int(time.time())}"  # Fallback in case db_name is still None
        logger.info(f"Performing analysis on database: {db_name}")
        
        # Reconnect to the database
        db = FirehoseDB(db_name, create_new_db=False)
        
        # Analyze record types
        logger.info("Analyzing record types...")
        type_analysis = analyze_record_types(db)
        logger.info("Record type analysis:")
        pprint(type_analysis)
        
        # Analyze post contents (if we have posts)
        if "app.bsky.feed.post" in stats.get("record_types", []):
            logger.info("Analyzing post contents...")
            post_analysis = analyze_post_contents(db)
            logger.info("Post content analysis:")
            pprint(post_analysis)
    
    logger.info("All done!")


if __name__ == "__main__":
    main() 