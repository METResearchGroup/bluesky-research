                             +------------+
                             | Redis      |
                             | State Store|
                             +---+----+---+
                                 |    ^
                                 v    |
 +------------+    Tasks    +----+----+---+    Metrics    +------------+
 | Worker 1   |<-----------+| Coordinator |+------------->| Monitor    |
 +------------+             +------+------+               +------------+
                                   |
 +------------+                    v                      +------------+
 | Worker 2   |<------------------>+<-------------------->| Rate       |
 +------------+                                           | Limiter    |
                                                          +------------+
 +------------+
 | Worker N   |<------------------>+
 +------------+                    |
                                   v
                            +------+------+
                            | Bluesky     |
                            | API         |
                            +-------------+
```

Notes:
1. The Coordinator initializes with the full list of DIDs and creates task batches
2. Workers pull tasks from the coordinator through Redis
3. The Rate Limiter ensures all API calls respect quota limits
4. Workers report progress and metrics back to Redis
5. The Monitor reads metrics from Redis and displays real-time progress
6. All state is persisted in Redis for failure recovery 