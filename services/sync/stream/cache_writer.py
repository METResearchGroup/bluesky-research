"""Manages real-time cache writing.

Connects to the firehose and ingests records in real time and writes to
cache.

Downstream, the batch exporter job will read the cache and export to persistent
storage.
"""
