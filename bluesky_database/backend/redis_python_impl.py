#!/usr/bin/env python3
"""
Python Redis Implementation for Learning

This is a simplified Python implementation of Redis core functionality
to help understand how Redis works internally. This is for educational
purposes only and implements the most important Redis features.
"""

import time
import threading
from typing import Dict, List, Set, Any, Optional, Tuple
from collections import defaultdict
import re
from dataclasses import dataclass
from enum import Enum


class DataType(Enum):
    """Redis data types"""

    STRING = "string"
    LIST = "list"
    SET = "set"
    HASH = "hash"
    SORTED_SET = "zset"


@dataclass
class ExpiryInfo:
    """Information about key expiration"""

    expires_at: Optional[float] = None

    def is_expired(self) -> bool:
        """Check if the key has expired"""
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at

    def ttl(self) -> int:
        """Get time to live in seconds"""
        if self.expires_at is None:
            return -1
        remaining = self.expires_at - time.time()
        return max(0, int(remaining))


class RedisDataStore:
    """
    Python implementation of Redis core functionality

    This class implements the main data structures and operations
    that make Redis powerful and fast.
    """

    def __init__(self):
        # Main storage: key -> (value, data_type, expiry_info)
        self._storage: Dict[str, Tuple[Any, DataType, ExpiryInfo]] = {}

        # Thread safety
        self._lock = threading.RLock()

        # Statistics
        self._stats = {
            "total_commands_processed": 0,
            "keyspace_hits": 0,
            "keyspace_misses": 0,
            "expired_keys": 0,
        }

    def _cleanup_expired(self):
        """Remove expired keys"""
        expired_keys = []

        for key, (value, data_type, expiry_info) in self._storage.items():
            if expiry_info.is_expired():
                expired_keys.append(key)

        for key in expired_keys:
            del self._storage[key]
            self._stats["expired_keys"] += 1

    def _get_value(self, key: str) -> Optional[Tuple[Any, DataType, ExpiryInfo]]:
        """Get value with expiration check"""
        self._cleanup_expired()

        if key in self._storage:
            self._stats["keyspace_hits"] += 1
            return self._storage[key]
        else:
            self._stats["keyspace_misses"] += 1
            return None

    def _set_value(
        self,
        key: str,
        value: Any,
        data_type: DataType,
        expiry_seconds: Optional[int] = None,
    ):
        """Set value with optional expiration"""
        expiry_info = ExpiryInfo()
        if expiry_seconds is not None:
            expiry_info.expires_at = time.time() + expiry_seconds

        self._storage[key] = (value, data_type, expiry_info)

    # ==================== STRING OPERATIONS ====================

    def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """SET key value [EX seconds]"""
        with self._lock:
            self._set_value(key, value, DataType.STRING, ex)
            self._stats["total_commands_processed"] += 1
            return True

    def get(self, key: str) -> Optional[str]:
        """GET key"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                return None

            value, data_type, expiry_info = result
            if data_type != DataType.STRING:
                return None  # Wrong type

            return value

    def incr(self, key: str) -> int:
        """INCR key"""
        with self._lock:
            current = self.get(key)
            if current is None:
                # Key doesn't exist, start with 0
                new_value = 1
            else:
                try:
                    new_value = int(current) + 1
                except ValueError:
                    return None  # Not a number

            self.set(key, str(new_value))
            return new_value

    def decr(self, key: str) -> int:
        """DECR key"""
        with self._lock:
            current = self.get(key)
            if current is None:
                # Key doesn't exist, start with 0
                new_value = -1
            else:
                try:
                    new_value = int(current) - 1
                except ValueError:
                    return None  # Not a number

            self.set(key, str(new_value))
            return new_value

    # ==================== LIST OPERATIONS ====================

    def lpush(self, key: str, *values: str) -> int:
        """LPUSH key value [value ...]"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                # Create new list
                lst = list(values)
                self._set_value(key, lst, DataType.LIST)
            else:
                value, data_type, expiry_info = result
                if data_type != DataType.LIST:
                    return 0  # Wrong type

                lst = value
                # Insert values at the beginning (left side)
                for val in reversed(values):
                    lst.insert(0, val)

            self._stats["total_commands_processed"] += 1
            return len(lst)

    def rpush(self, key: str, *values: str) -> int:
        """RPUSH key value [value ...]"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                # Create new list
                lst = list(values)
                self._set_value(key, lst, DataType.LIST)
            else:
                value, data_type, expiry_info = result
                if data_type != DataType.LIST:
                    return 0  # Wrong type

                lst = value
                # Append values to the end (right side)
                lst.extend(values)

            self._stats["total_commands_processed"] += 1
            return len(lst)

    def lpop(self, key: str) -> Optional[str]:
        """LPOP key"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                return None

            value, data_type, expiry_info = result
            if data_type != DataType.LIST:
                return None  # Wrong type

            lst = value
            if not lst:
                return None

            # Remove and return first element (left side)
            popped = lst.pop(0)

            # If list is empty, remove the key
            if not lst:
                del self._storage[key]

            self._stats["total_commands_processed"] += 1
            return popped

    def rpop(self, key: str) -> Optional[str]:
        """RPOP key"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                return None

            value, data_type, expiry_info = result
            if data_type != DataType.LIST:
                return None  # Wrong type

            lst = value
            if not lst:
                return None

            # Remove and return last element (right side)
            popped = lst.pop()

            # If list is empty, remove the key
            if not lst:
                del self._storage[key]

            self._stats["total_commands_processed"] += 1
            return popped

    def llen(self, key: str) -> int:
        """LLEN key"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                return 0

            value, data_type, expiry_info = result
            if data_type != DataType.LIST:
                return 0  # Wrong type

            return len(value)

    def lrange(self, key: str, start: int, stop: int) -> List[str]:
        """LRANGE key start stop"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                return []

            value, data_type, expiry_info = result
            if data_type != DataType.LIST:
                return []  # Wrong type

            lst = value
            # Handle negative indices like Redis
            if start < 0:
                start = len(lst) + start
            if stop < 0:
                stop = len(lst) + stop

            # Ensure bounds
            start = max(0, min(start, len(lst)))
            stop = max(0, min(stop + 1, len(lst)))  # +1 because stop is inclusive

            return lst[start:stop]

    # ==================== HASH OPERATIONS ====================

    def hset(self, key: str, field: str, value: str) -> int:
        """HSET key field value"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                # Create new hash
                hash_dict = {field: value}
                self._set_value(key, hash_dict, DataType.HASH)
                self._stats["total_commands_processed"] += 1
                return 1  # New field added
            else:
                value, data_type, expiry_info = result
                if data_type != DataType.HASH:
                    return 0  # Wrong type

                hash_dict = value
                is_new_field = field not in hash_dict
                hash_dict[field] = value

                self._stats["total_commands_processed"] += 1
                return 1 if is_new_field else 0

    def hget(self, key: str, field: str) -> Optional[str]:
        """HGET key field"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                return None

            value, data_type, expiry_info = result
            if data_type != DataType.HASH:
                return None  # Wrong type

            return value.get(field)

    def hgetall(self, key: str) -> Dict[str, str]:
        """HGETALL key"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                return {}

            value, data_type, expiry_info = result
            if data_type != DataType.HASH:
                return {}  # Wrong type

            return dict(value)  # Return a copy

    def hdel(self, key: str, *fields: str) -> int:
        """HDEL key field [field ...]"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                return 0

            value, data_type, expiry_info = result
            if data_type != DataType.HASH:
                return 0  # Wrong type

            hash_dict = value
            deleted_count = 0

            for field in fields:
                if field in hash_dict:
                    del hash_dict[field]
                    deleted_count += 1

            # If hash is empty, remove the key
            if not hash_dict:
                del self._storage[key]

            self._stats["total_commands_processed"] += 1
            return deleted_count

    def hkeys(self, key: str) -> List[str]:
        """HKEYS key"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                return []

            value, data_type, expiry_info = result
            if data_type != DataType.HASH:
                return []  # Wrong type

            return list(value.keys())

    def hvals(self, key: str) -> List[str]:
        """HVALS key"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                return []

            value, data_type, expiry_info = result
            if data_type != DataType.HASH:
                return []  # Wrong type

            return list(value.values())

    # ==================== SET OPERATIONS ====================

    def sadd(self, key: str, *members: str) -> int:
        """SADD key member [member ...]"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                # Create new set
                set_obj = set(members)
                self._set_value(key, set_obj, DataType.SET)
                self._stats["total_commands_processed"] += 1
                return len(set_obj)
            else:
                value, data_type, expiry_info = result
                if data_type != DataType.SET:
                    return 0  # Wrong type

                set_obj = value
                original_size = len(set_obj)
                set_obj.update(members)
                new_size = len(set_obj)

                self._stats["total_commands_processed"] += 1
                return new_size - original_size  # Number of new members added

    def srem(self, key: str, *members: str) -> int:
        """SREM key member [member ...]"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                return 0

            value, data_type, expiry_info = result
            if data_type != DataType.SET:
                return 0  # Wrong type

            set_obj = value
            removed_count = 0

            for member in members:
                if member in set_obj:
                    set_obj.remove(member)
                    removed_count += 1

            # If set is empty, remove the key
            if not set_obj:
                del self._storage[key]

            self._stats["total_commands_processed"] += 1
            return removed_count

    def smembers(self, key: str) -> Set[str]:
        """SMEMBERS key"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                return set()

            value, data_type, expiry_info = result
            if data_type != DataType.SET:
                return set()  # Wrong type

            return set(value)  # Return a copy

    def sismember(self, key: str, member: str) -> bool:
        """SISMEMBER key member"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                return False

            value, data_type, expiry_info = result
            if data_type != DataType.SET:
                return False  # Wrong type

            return member in value

    def scard(self, key: str) -> int:
        """SCARD key"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                return 0

            value, data_type, expiry_info = result
            if data_type != DataType.SET:
                return 0  # Wrong type

            return len(value)

    def sinter(self, *keys: str) -> Set[str]:
        """SINTER key [key ...]"""
        with self._lock:
            sets = []
            for key in keys:
                result = self._get_value(key)
                if result is None:
                    return set()  # If any key doesn't exist, intersection is empty

                value, data_type, expiry_info = result
                if data_type != DataType.SET:
                    return set()  # Wrong type

                sets.append(value)

            if not sets:
                return set()

            # Calculate intersection
            result_set = sets[0].copy()
            for s in sets[1:]:
                result_set &= s

            return result_set

    # ==================== KEY OPERATIONS ====================

    def del_key(self, *keys: str) -> int:
        """DEL key [key ...]"""
        with self._lock:
            deleted_count = 0
            for key in keys:
                if key in self._storage:
                    del self._storage[key]
                    deleted_count += 1

            self._stats["total_commands_processed"] += 1
            return deleted_count

    def exists(self, *keys: str) -> int:
        """EXISTS key [key ...]"""
        with self._lock:
            self._cleanup_expired()
            count = 0
            for key in keys:
                if key in self._storage:
                    count += 1

            return count

    def expire(self, key: str, seconds: int) -> bool:
        """EXPIRE key seconds"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                return False

            value, data_type, expiry_info = result
            expiry_info.expires_at = time.time() + seconds

            return True

    def ttl(self, key: str) -> int:
        """TTL key"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                return -2  # Key doesn't exist

            value, data_type, expiry_info = result
            return expiry_info.ttl()

    def keys(self, pattern: str) -> List[str]:
        """KEYS pattern"""
        with self._lock:
            self._cleanup_expired()

            # Simple pattern matching (convert Redis pattern to regex)
            # This is a simplified version - Redis has more complex patterns
            regex_pattern = pattern.replace("*", ".*").replace("?", ".")

            matching_keys = []
            for key in self._storage.keys():
                if re.match(regex_pattern, key):
                    matching_keys.append(key)

            return matching_keys

    def dbsize(self) -> int:
        """DBSIZE"""
        with self._lock:
            self._cleanup_expired()
            return len(self._storage)

    def flushdb(self):
        """FLUSHDB"""
        with self._lock:
            self._storage.clear()
            self._stats["total_commands_processed"] += 1

    # ==================== INFO & STATISTICS ====================

    def info(self) -> Dict[str, Any]:
        """INFO command - return statistics"""
        with self._lock:
            self._cleanup_expired()

            return {
                "total_commands_processed": self._stats["total_commands_processed"],
                "keyspace_hits": self._stats["keyspace_hits"],
                "keyspace_misses": self._stats["keyspace_misses"],
                "expired_keys": self._stats["expired_keys"],
                "db_size": len(self._storage),
                "uptime": time.time(),  # Simplified
            }

    def ping(self) -> bool:
        """PING command"""
        return True

    # ==================== UTILITY METHODS ====================

    def get_type(self, key: str) -> Optional[str]:
        """TYPE key"""
        with self._lock:
            result = self._get_value(key)
            if result is None:
                return None

            value, data_type, expiry_info = result
            return data_type.value

    def debug_info(self) -> Dict[str, Any]:
        """Debug information about the current state"""
        with self._lock:
            self._cleanup_expired()

            debug_data = {
                "total_keys": len(self._storage),
                "keys_by_type": defaultdict(int),
                "expired_keys_count": self._stats["expired_keys"],
                "sample_keys": {},
            }

            for key, (value, data_type, expiry_info) in self._storage.items():
                debug_data["keys_by_type"][data_type.value] += 1

                # Show sample of first few keys
                if len(debug_data["sample_keys"]) < 5:
                    debug_data["sample_keys"][key] = {
                        "type": data_type.value,
                        "value_preview": str(value)[:50] + "..."
                        if len(str(value)) > 50
                        else str(value),
                        "ttl": expiry_info.ttl(),
                    }

            return debug_data


# ==================== DEMO & TESTING ====================


def demo_redis_operations():
    """Demonstrate Redis operations"""
    print("🚀 Redis Python Implementation Demo")
    print("=" * 50)

    # Create Redis instance
    redis_db = RedisDataStore()

    # String operations
    print("\n📝 String Operations:")
    redis_db.set("name", "Alice")
    redis_db.set("age", "25")
    print(f"GET name: {redis_db.get('name')}")
    print(f"GET age: {redis_db.get('age')}")

    # Increment operations
    redis_db.set("counter", "0")
    print(f"INCR counter: {redis_db.incr('counter')}")
    print(f"INCR counter: {redis_db.incr('counter')}")
    print(f"DECR counter: {redis_db.decr('counter')}")

    # List operations
    print("\n📋 List Operations:")
    redis_db.lpush("fruits", "apple", "banana")
    redis_db.rpush("fruits", "orange", "grape")
    print(f"LLEN fruits: {redis_db.llen('fruits')}")
    print(f"LRANGE fruits 0 -1: {redis_db.lrange('fruits', 0, -1)}")
    print(f"LPOP fruits: {redis_db.lpop('fruits')}")
    print(f"RPOP fruits: {redis_db.rpop('fruits')}")

    # Hash operations
    print("\n🗂️ Hash Operations:")
    redis_db.hset("user:1", "name", "Bob")
    redis_db.hset("user:1", "email", "bob@example.com")
    redis_db.hset("user:1", "age", "30")
    print(f"HGET user:1 name: {redis_db.hget('user:1', 'name')}")
    print(f"HGETALL user:1: {redis_db.hgetall('user:1')}")
    print(f"HKEYS user:1: {redis_db.hkeys('user:1')}")

    # Set operations
    print("\n🔢 Set Operations:")
    redis_db.sadd("tags", "python", "redis", "database")
    redis_db.sadd("tags", "python")  # Duplicate won't be added
    print(f"SMEMBERS tags: {redis_db.smembers('tags')}")
    print(f"SISMEMBER tags python: {redis_db.sismember('tags', 'python')}")
    print(f"SCARD tags: {redis_db.scard('tags')}")

    # Key operations
    print("\n🔑 Key Operations:")
    print(f"EXISTS name: {redis_db.exists('name')}")
    print(f"EXISTS nonexistent: {redis_db.exists('nonexistent')}")
    print(f"TYPE name: {redis_db.get_type('name')}")
    print(f"TYPE fruits: {redis_db.get_type('fruits')}")

    # Expiration
    print("\n⏰ Expiration:")
    redis_db.set("temp_key", "will expire", ex=2)
    print(f"TTL temp_key: {redis_db.ttl('temp_key')}")
    print(f"EXISTS temp_key: {redis_db.exists('temp_key')}")

    # Statistics
    print("\n📊 Statistics:")
    info = redis_db.info()
    for key, value in info.items():
        print(f"  {key}: {value}")

    # Debug info
    print("\n🐛 Debug Info:")
    debug = redis_db.debug_info()
    for key, value in debug.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    demo_redis_operations()
