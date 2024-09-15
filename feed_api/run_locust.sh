#!/bin/bash

# Check if locust is installed
if ! command -v locust &> /dev/null
then
    echo "Locust is not installed. Please install it using 'pip install locust'"
    exit 1
fi

# Set the host (assuming your app is running locally on port 8000)
# HOST="http://0.0.0.0:8000"
HOST="https://mindtechnologylab.com"

# Set the number of users to simulate
# USERS=1
USERS=50

# Set the spawn rate (users spawned/second)
SPAWN_RATE=1 # 1 user per second
# SPAWN_RATE=0.2 # 1 user per 5 seconds

# Set the run time
# RUN_TIME="1m"
# RUN_TIME="10s"
# RUN_TIME="60s"
RUN_TIME="180s"

# Run Locust
echo "Starting load testing..."
locust -f locustfile.py --host=$HOST --users=$USERS --spawn-rate=$SPAWN_RATE --run-time=$RUN_TIME --headless --only-summary
# locust -f locustfile.py # I want to do this in the console.
echo "Successfully finished load testing."
