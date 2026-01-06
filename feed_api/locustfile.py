"""Load testing the API, using locust.

Documentation: https://docs.locust.io/en/stable/installation.html
"""

import json

from locust import HttpUser, task, between, events

from lib.aws.secretsmanager import get_secret

token = json.loads(get_secret("feed-api-default-test-token"))[
    "feed-api-default-test-token"
]


class MyUser(HttpUser):
    # wait_time = between(0.1, 0.5)  # Wait between 0.1 and 0.5 seconds between tasks
    # wait_time = between(1, 1)  # Wait exactly 1 second between tasks
    wait_time = between(0.5, 0.5)  # Wait exactly 0.5 seconds between tasks
    TOKEN = token

    @task
    def get_default_feed(self):
        headers = {"Authorization": f"Bearer {self.TOKEN}"}
        with self.client.get(
            "/get-default-feed", headers=headers, catch_response=True
        ) as response:
            if response.status_code == 200:
                # Optionally, you can add more checks here
                # For example, verifying the structure of the response
                if "feed" in response.json():
                    response.success()
                else:
                    response.failure("Response does not contain 'feed' key")
            else:
                response.failure(f"Got status code {response.status_code}")


# Counter for requests
request_count = 0


@events.request.add_listener
def on_request(
    _request_type,
    _name,
    _response_time,
    _response_length,
    _response,
    _context,
    _exception,
    **_kwargs,
):
    global request_count
    request_count += 1
    if request_count >= 10:
        events.quitting.fire()
