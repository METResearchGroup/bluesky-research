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
import os
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from pprint import pprint
from typing import Dict, List, Optional, Set, Union

# Add the project root to sys.path
project_root = str(Path(os.path.abspath(__file__)).parent.parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from demos.firehose_ingestion.db import FirehoseDB, FirehoseRecord
from demos.firehose_ingestion.jetstream_connector import JetstreamConnector
from lib.config.app_config import APP_CONFIG
from lib.data.connections.jetstream import get_jetstream_connection
from lib.log.logger import get_logger

logger = get_logger(__file__)


def analyze_collections(db: FirehoseDB) -> Dict:
    """Analyze the collections stored in the database.
    
    Args:
        db: FirehoseDB instance to analyze
        
    Returns:
        Dictionary with statistics about collections
    """
    records = db.get_records(limit=10000)
    total_records = len(records)
    
    if total_records == 0:
        return {"error": "No records found"}
    
    stats = {
        "total_records": total_records,
        "collections": {},
        "kinds": {},
        "unique_dids": set(),
    }
    
    for record in records:
        # Track collections
        if record.collection not in stats["collections"]:
            stats["collections"][record.collection] = 0
        stats["collections"][record.collection] += 1
        
        # Track kinds
        if record.kind not in stats["kinds"]:
            stats["kinds"][record.kind] = 0
        stats["kinds"][record.kind] += 1
        
        # Track DIDs
        stats["unique_dids"].add(record.did)
    
    # Convert set to count for JSON serialization
    stats["unique_dids"] = len(stats["unique_dids"])
    
    return stats


def analyze_post_contents(db: FirehoseDB) -> Dict:
    """Analyze the contents of posts in the database.
    
    Args:
        db: FirehoseDB instance to analyze
        
    Returns:
        Dictionary with statistics about post contents
    """
    # Get post records
    records = db.get_records(collection="app.bsky.feed.post", limit=1000)
    total_posts = len(records)
    
    if total_posts == 0:
        return {"error": "No post records found"}
    
    stats = {
        "total_posts": total_posts,
        "avg_text_length": 0,
        "has_images": 0,
        "has_links": 0,
        "has_mentions": 0,
        "unique_authors": set(),
        "sample_posts": []
    }
    
    total_text_length = 0
    for record in records:
        # Parse commit data
        commit_data = json.loads(record.commit_data)
        post_data = commit_data.get("record", {})
        
        # Author tracking
        stats["unique_authors"].add(record.did)
        
        # Text analysis
        text = post_data.get("text", "")
        total_text_length += len(text)
        
        # Check for images
        if "embed" in post_data and "images" in str(post_data["embed"]):
            stats["has_images"] += 1
            
        # Check for links
        if "embed" in post_data and "external" in str(post_data["embed"]):
            stats["has_links"] += 1
            
        # Check for mentions
        if "@" in text:
            stats["has_mentions"] += 1
            
        # Store sample posts (first 5)
        if len(stats["sample_posts"]) < 5:
            stats["sample_posts"].append({
                "text": text[:100] + ("..." if len(text) > 100 else ""),
                "author": record.did,
                "created_at": post_data.get("createdAt", "unknown")
            })
    
    if total_posts > 0:
        stats["avg_text_length"] = total_text_length / total_posts
    
    # Convert set to count for JSON serialization
    stats["unique_authors"] = len(stats["unique_authors"])
    
    return stats


def analyze_likes(db: FirehoseDB) -> Dict:
    """Analyze like records in the database.
    
    Args:
        db: FirehoseDB instance to analyze
        
    Returns:
        Dictionary with statistics about likes
    """
    # Get like records
    records = db.get_records(collection="app.bsky.feed.like", limit=1000)
    total_likes = len(records)
    
    if total_likes == 0:
        return {"error": "No like records found"}
    
    stats = {
        "total_likes": total_likes,
        "liked_collections": {},
        "unique_likers": set(),
        "sample_likes": []
    }
    
    for record in records:
        # Parse commit data
        commit_data = json.loads(record.commit_data)
        like_data = commit_data.get("record", {})
        
        # Liker tracking
        stats["unique_likers"].add(record.did)
        
        # Get subject URI and parse the collection
        subject = like_data.get("subject", {})
        uri = subject.get("uri", "")
        
        # Extract collection from URI
        if uri and isinstance(uri, str):
            parts = uri.split('/')
            if len(parts) >= 4:
                collection = parts[-2]
                if collection not in stats["liked_collections"]:
                    stats["liked_collections"][collection] = 0
                stats["liked_collections"][collection] += 1
                
        # Store sample likes (first 5)
        if len(stats["sample_likes"]) < 5:
            stats["sample_likes"].append({
                "uri": uri,
                "liker": record.did,
                "created_at": like_data.get("createdAt", "unknown")
            })
    
    # Convert set to count for JSON serialization
    stats["unique_likers"] = len(stats["unique_likers"])
    
    return stats


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