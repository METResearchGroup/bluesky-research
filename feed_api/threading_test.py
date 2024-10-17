"""Test file to better understand mechanics of threading and asyncio.

The main thread runs while the backgorund thread updates a list in the background.
This example shows that the list can be updated in the background without blocking
the main thread, which is the intended behavior of our implementation.
"""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI
from fastapi.testclient import TestClient

app = FastAPI()
thread_pool = ThreadPoolExecutor(max_workers=1)
request_count = 0

long_running_list = []


# Simulating a long-running operation
def long_running_task():
    """Simulates a long-running task that updates a global list."""
    print("Starting long-running task...")
    global long_running_list
    long_running_list = []  # reset list each time.
    for i in range(50):  # Increased to 50 to make it longer
        print(f"Long-running task: {i}")
        long_running_list.append(i)
        time.sleep(0.2)  # Sleep for 0.2 seconds between each append
    print("Long-running task completed.")


async def async_long_running_task():
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(thread_pool, long_running_task)


async def periodic_task():
    while True:
        print("Running periodic task...")
        await async_long_running_task()
        await asyncio.sleep(1)  # Reduced to 1 second to run more frequently


@app.get("/")
async def root():
    global request_count
    request_count += 1
    return {
        "message": f"Hello World {request_count}",
        "list_length": len(long_running_list),
    }


# Test client
client = TestClient(app)


async def run_background_task():
    task = asyncio.create_task(periodic_task())
    await asyncio.sleep(0.1)  # Give the task a moment to start
    return task


def test_api_responsiveness():
    # Start the background task
    loop = asyncio.get_event_loop()
    background_task = loop.run_until_complete(run_background_task())

    start_time = time.time()
    requests_made = 0

    try:
        while time.time() - start_time < 20:
            response = client.get("/")
            assert response.status_code == 200
            requests_made += 1
            print(f"Request {requests_made} completed: {response.json()}")
            time.sleep(1)  # Wait 1 second before next request
    finally:
        # Allow the background task to complete its current iteration
        loop.run_until_complete(asyncio.sleep(1))
        # Cancel the background task
        background_task.cancel()
        try:
            loop.run_until_complete(background_task)
        except asyncio.CancelledError:
            pass

    end_time = time.time()
    total_time = end_time - start_time

    print(f"Test ran for {total_time:.2f} seconds")
    print(f"Total requests made: {requests_made}")
    print(f"Expected requests: {int(total_time)}")

    assert (
        abs(requests_made - int(total_time)) <= 1
    ), "Number of requests doesn't match expected count"
    assert (
        request_count == requests_made
    ), "Global request count doesn't match requests made"
    print(f"Final long_running_list length: {len(long_running_list)}")
    print(f"long_running_list (updated in thread): {long_running_list}")


if __name__ == "__main__":
    test_api_responsiveness()
