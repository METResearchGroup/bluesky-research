#!/usr/bin/env python3
"""
Redis Server Test Script

This script tests the Redis server setup and basic functionality.
"""

import redis
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_redis_connection():
    """Test basic Redis connection and functionality."""
    print("🚀 Redis Server Test Script")
    print("=" * 50)

    try:
        # Connect to Redis with more robust connection settings
        redis_client = redis.Redis(
            host="localhost",
            port=6379,
            db=0,
            decode_responses=True,
            socket_connect_timeout=10,
            socket_timeout=10,
            retry_on_timeout=True,
            health_check_interval=30,
        )

        # Test connection
        redis_client.ping()
        print("✅ Redis connection successful")

        # Get Redis info
        info = redis_client.info()
        print(f"📊 Redis version: {info.get('redis_version', 'Unknown')}")
        print(f"📊 Connected clients: {info.get('connected_clients', 0)}")
        print(f"📊 Used memory: {info.get('used_memory_human', 'Unknown')}")
        print(f"📊 Max memory: {info.get('maxmemory_human', 'Unknown')}")

        # Test basic operations
        print("\n🧪 Testing basic operations...")

        # Test SET/GET
        redis_client.set("test_key", "test_value")
        value = redis_client.get("test_key")
        assert value == "test_value"
        print("✅ SET/GET operations work")

        # Test DELETE
        redis_client.delete("test_key")
        value = redis_client.get("test_key")
        assert value is None
        print("✅ DELETE operations work")

        # Test Streams (for Jetstream load test)
        print("\n🧪 Testing Redis Streams...")
        stream_name = "test_stream"

        # Add message to stream
        message_id = redis_client.xadd(
            stream_name, {"test_field": "test_value"}, maxlen=10, approximate=True
        )
        print(f"✅ Added message to stream: {message_id}")

        # Read from stream
        messages = redis_client.xread({stream_name: "0"}, count=1)
        assert len(messages) > 0
        print("✅ Stream read operations work")

        # Clean up
        redis_client.delete(stream_name)
        print("✅ Stream cleanup successful")

        print("\n🎉 All Redis tests passed!")
        return True

    except Exception as e:
        print(f"❌ Redis test failed: {e}")
        logger.error(f"Redis test error: {e}")
        return False


if __name__ == "__main__":
    success = test_redis_connection()
    exit(0 if success else 1)
