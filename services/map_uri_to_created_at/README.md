# Map URIs to "created_at" field

This service maps URIs to their creation timestamps. We're using "created_at"
as the main partitioning timestamp. Having a single ground source of truth
for that helps us keep track of which posts were created on what days, as well
as helps us do any repartitioning for data that was previously partitioned
using other timestamps (e.g., timestamp on when the post was classified).

Usage:

```python
python main.py
```
