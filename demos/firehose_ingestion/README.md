# Bluesky Firehose Ingestion Demo

This demo shows how to ingest and store records from the Bluesky firehose, specifically focusing on the following record types:
- `app.bsky.feed.post`
- `app.bsky.feed.like`
- `app.bsky.graph.follow`
- `app.bsky.feed.repost`

## Overview

The system connects to the Bluesky firehose and filters for specific record types. Records are batched (1000 records per batch) and stored in separate directories based on their type. All data is stored locally under the configured data directory.

## Project Structure

```
/demos/firehose_ingestion/
├── README.md
├── requirements.txt
├── main.py (main ingestion script)
├── config.py (configuration settings)
└── utils/
    ├── __init__.py
    ├── firehose.py (firehose connection handling)
    └── writer.py (batch writing logic)
```

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the ingestion script:
```bash
python main.py
```

## Data Storage

Records are stored in the following directory structure under the configured root data directory:
```
demo_firehose/
├── posts/
├── likes/
├── follows/
└── reposts/
```

Each subdirectory contains JSON files with batched records (1000 records per file).

## Features

- Async firehose connection handling
- Efficient batch processing
- Type-based record filtering
- Structured data storage
- Error handling and logging
- Graceful shutdown handling 