#!/bin/bash

# Redis Backend - Local Development Startup Script
# This script starts Redis locally using Docker Compose for development

set -e  # Exit on any error

echo "🚀 Starting Redis Backend - Local Development"
echo "=============================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if docker-compose.yml exists
if [ ! -f "docker-compose.yml" ]; then
    echo "❌ docker-compose.yml not found. Please ensure you're in the correct directory."
    exit 1
fi

# Create data directory if it doesn't exist
mkdir -p data/redis
mkdir -p data/logs

echo "📁 Creating data directories..."
echo "   - data/redis (Redis persistence)"
echo "   - data/logs (Application logs)"

# Start Redis with Docker Compose
echo "🐳 Starting Redis container..."
docker-compose up -d redis

# Wait for Redis to be ready
echo "⏳ Waiting for Redis to be ready..."
for i in {1..30}; do
    if docker-compose exec -T redis redis-cli ping > /dev/null 2>&1; then
        echo "✅ Redis is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ Redis failed to start within 30 seconds"
        docker-compose logs redis
        exit 1
    fi
    sleep 1
done

# Display Redis information
echo ""
echo "📊 Redis Information:"
echo "====================="
docker-compose exec -T redis redis-cli info server | grep -E "(redis_version|uptime_in_seconds|connected_clients)"
docker-compose exec -T redis redis-cli info memory | grep -E "(used_memory_human|maxmemory_human)"

echo ""
echo "🔗 Redis Connection Details:"
echo "   Host: localhost"
echo "   Port: 6379"
echo "   Database: 0"
echo ""
echo "📝 Useful Commands:"
echo "   Test connection: redis-cli ping"
echo "   Monitor Redis: docker-compose logs -f redis"
echo "   Stop Redis: docker-compose down"
echo "   View Redis info: redis-cli info"
echo ""
echo "✅ Redis is running and ready for development!"
