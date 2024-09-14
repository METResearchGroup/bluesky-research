"""Load testing the API, using locust.

Documentation: https://docs.locust.io/en/stable/installation.html
"""

import requests

from locust import HttpUser, task, between


class MyUser(HttpUser):
    wait_time = between(0.1, 0.5)  # Wait between 0.1 and 0.5 seconds between tasks

    @task
    def get_feed_skeleton(self):
        headers = {"Authorization": ""}
        self.client.get("/xrpc/app.bsky.feed.getFeedSkeleton", headers=headers)


url = "https://mindtechnologylab.com/xrpc/app.bsky.feed.getFeedSkeleton"
headers = {"Authorization": ""}

response = requests.get(url, headers=headers)

# Print the status code
print(f"Status Code: {response.status_code}")

# Print the response headers
print("Response Headers:")
for header, value in response.headers.items():
    print(f"{header}: {value}")

breakpoint()
