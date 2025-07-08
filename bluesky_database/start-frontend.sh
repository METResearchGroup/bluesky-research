#!/bin/bash

# Bluesky Post Explorer Frontend - Development Server Starter
# Run this script from anywhere in the bluesky-research project

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FRONTEND_DIR="$SCRIPT_DIR/frontend"

echo "🚀 Starting Bluesky Post Explorer Frontend..."
echo "📂 Frontend directory: $FRONTEND_DIR"

# Check if frontend directory exists
if [ ! -d "$FRONTEND_DIR" ]; then
    echo "❌ Error: Frontend directory not found at $FRONTEND_DIR"
    exit 1
fi

# Check if package.json exists
if [ ! -f "$FRONTEND_DIR/package.json" ]; then
    echo "❌ Error: package.json not found. Run 'npm install' first."
    exit 1
fi

# Navigate to frontend directory and start dev server
cd "$FRONTEND_DIR"

echo "📦 Installing dependencies if needed..."
npm install

echo "🔥 Starting development server..."
echo "🌐 Open http://localhost:3000 in your browser"
echo "⏹️  Press Ctrl+C to stop the server"

npm run dev 