# Get preprocessed posts used in feeds

This service gets the preprocessed posts used in feeds for a given partition date.
It then exports the posts to local storage. It partitions based on the day
that the post appeared in a feed. This means that a post can appear multiple
times across partition dates, since a post can remain in the base pool of
available posts across multiple partition dates.

Usage:
```python
python main.py
```