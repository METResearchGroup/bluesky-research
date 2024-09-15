#!/bin/bash

# Check if locust is installed
if ! command -v locust &> /dev/null
then
    echo "Locust is not installed. Please install it using 'pip install locust'"
    exit 1
fi

# Set the host (assuming your app is running locally on port 8000)
HOST="http://0.0.0.0:8000"

# Set the number of users to simulate
USERS=10

# Set the spawn rate (users spawned/second)
SPAWN_RATE=1

# Set the run time
RUN_TIME="1m"

# Run Locust
echo "Starting load testing..."
# locust -f locustfile.py --host=$HOST --users=$USERS --spawn-rate=$SPAWN_RATE --run-time=$RUN_TIME --headless --only-summary
locust -f locustfile.py # I want to do this in the console.
echo "Successfully finished load testing."
