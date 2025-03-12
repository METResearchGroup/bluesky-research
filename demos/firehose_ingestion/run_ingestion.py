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
from typing import Union, Optional

from demos.firehose_ingestion.db import FirehoseDB
from demos.firehose_ingestion.jetstream_connector import JetstreamConnector
from lib.log.logger import get_logger

logger = get_logger(__file__)


def analyze_collections(db: FirehoseDB, limit: int = 1000) -> dict:
    """Analyze the collections stored in the database.
    
    Args:
        db: FirehoseDB instance to query
        limit: Maximum number of records to analyze
        
    Returns:
        Dictionary with collection statistics
    """
    records = db.get_records(limit=limit)
    
    if not records:
        return {"error": "No records found"}
    
    # Analyze collections and types
    collections = []
    kinds = []
    dids = set()
    
    for record in records:
        collections.append(record.collection)
        kinds.append(record.kind)
        dids.add(record.did)
    
    # Count frequencies
    collection_counts = dict(Counter(collections))
    kind_counts = dict(Counter(kinds))
    
    # Extract a sample commit
    sample_commit = None
    if records:
        try:
            sample_commit = json.loads(records[0].commit)
        except json.JSONDecodeError:
            sample_commit = {"error": "Failed to parse commit JSON"}
    
    return {
        "record_counts": {
            "total": len(records),
            "by_collection": collection_counts,
            "by_kind": kind_counts,
            "unique_dids": len(dids)
        },
        "sample_commit": sample_commit
    }


def analyze_post_contents(db: FirehoseDB, limit: int = 100) -> dict:
    """Analyze post contents for text length, mentions, URLs, etc.
    
    Args:
        db: FirehoseDB instance to query
        limit: Maximum number of posts to analyze
        
    Returns:
        Dictionary with post content statistics
    """
    # Get posts specifically
    records = db.get_records(limit=limit, collection="app.bsky.feed.post")
    
    if not records:
        return {"error": "No post records found"}
    
    posts = []
    for record in records:
        try:
            commit_data = json.loads(record.commit)
            if "record" in commit_data and "$type" in commit_data["record"]:
                if commit_data["record"]["$type"] == "app.bsky.feed.post":
                    posts.append(commit_data["record"])
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Error parsing post record: {e}")
    
    if not posts:
        return {"error": "No valid posts found in records"}
    
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


def analyze_likes(db: FirehoseDB, limit: int = 100) -> dict:
    """Analyze like records to see which types of content are being liked.
    
    Args:
        db: FirehoseDB instance to query
        limit: Maximum number of like records to analyze
        
    Returns:
        Dictionary with like statistics
    """
    records = db.get_records(limit=limit, collection="app.bsky.feed.like")
    
    if not records:
        return {"error": "No like records found"}
    
    likes = []
    for record in records:
        try:
            commit_data = json.loads(record.commit)
            if "record" in commit_data and "$type" in commit_data["record"]:
                if commit_data["record"]["$type"] == "app.bsky.feed.like":
                    likes.append(commit_data["record"])
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Error parsing like record: {e}")
    
    if not likes:
        return {"error": "No valid likes found in records"}
    
    # Analyze which URIs are being liked
    uri_counts = {}
    for like in likes:
        if "subject" in like and "uri" in like["subject"]:
            uri = like["subject"]["uri"]
            # Extract the collection from the URI
            parts = uri.split("/")
            if len(parts) >= 5:
                collection = parts[4]
                uri_counts[collection] = uri_counts.get(collection, 0) + 1
    
    return {
        "like_stats": {
            "count": len(likes),
            "liked_collections": uri_counts
        },
        "sample_like": likes[0] if likes else None
    }


async def run_ingestion(
    db_name: str,
    instance: str,
    collections: Union[str, list[str]],
    target_count: int,
    max_time: int,
    cursor: Optional[str] = None,
    wanted_dids: Optional[Union[str, list[str]]] = None
) -> dict:
    """Run the firehose ingestion process.
    
    Args:
        db_name: Name for the database
        instance: Jetstream instance to connect to
        collections: Collection(s) to subscribe to
        target_count: Target number of records to ingest
        max_time: Maximum time to run in seconds
        cursor: Optional cursor for starting at a specific point
        wanted_dids: Optional specific DIDs to filter for
        
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
        cursor=cursor,
        wanted_dids=wanted_dids
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
    parser.add_argument('--cursor', type=str,
                        help='Cursor for starting at a specific point in the firehose')
    parser.add_argument('--wanted_dids', type=str,
                        help='Comma-separated list of DIDs to filter for')
    parser.add_argument('--analysis', action='store_true',
                        help='Perform analysis on ingested data')
    
    args = parser.parse_args()
    
    # Generate db_name if not provided
    db_name = args.db_name
    
    # Run the ingestion
    logger.info(f"Starting firehose ingestion with target count: {args.target_count}")
    loop = asyncio.get_event_loop()
    stats = loop.run_until_complete(
        run_ingestion(
            db_name=db_name,
            instance=args.instance,
            collections=args.collections,
            target_count=args.target_count,
            max_time=args.max_time,
            cursor=args.cursor,
            wanted_dids=args.wanted_dids
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
        
        # Analyze collections
        logger.info("Analyzing collections...")
        collection_analysis = analyze_collections(db)
        logger.info("Collection analysis:")
        pprint(collection_analysis)
        
        # Analyze post contents (if we have posts)
        if "app.bsky.feed.post" in stats.get("collections", []):
            logger.info("Analyzing post contents...")
            post_analysis = analyze_post_contents(db)
            logger.info("Post content analysis:")
            pprint(post_analysis)
            
        # Analyze likes (if we have likes)
        if "app.bsky.feed.like" in stats.get("collections", []):
            logger.info("Analyzing likes...")
            likes_analysis = analyze_likes(db)
            logger.info("Likes analysis:")
            pprint(likes_analysis)
    
    logger.info("All done!")
    
    if stats.get("latest_cursor"):
        logger.info(f"To resume from this point, use --cursor={stats['latest_cursor']}")


if __name__ == "__main__":
    main() 