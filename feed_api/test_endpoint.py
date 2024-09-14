"""Test feed API endpoints."""

import json
import os
import requests

from lib.aws.secretsmanager import get_secret

# The URL of your API
BASE_URL = "http://0.0.0.0:8000"

# The endpoint we're testing
ENDPOINT = "get-default-feed"

token = json.loads(get_secret("feed-api-default-test-token"))[
    "feed-api-default-test-token"
]


def test_get_default_feed():
    # Construct the full URL
    url = os.path.join(BASE_URL, ENDPOINT)

    # Set up the headers with the token
    headers = {"Authorization": f"Bearer {token}"}

    # Make the GET request
    response = requests.get(url, headers=headers)

    # Print the status code
    print(f"Status Code: {response.status_code}")

    # Print the response headers
    print("Response Headers:")
    for header, value in response.headers.items():
        print(f"{header}: {value}")

    # Print the response content
    print("\nResponse Content:")
    print(response.text)

    # If the response is JSON, print it in a more readable format
    try:
        print("\nJSON Response:")
        print(response.json())
    except requests.exceptions.JSONDecodeError:
        print("Response is not in JSON format")

    # breakpoint()


if __name__ == "__main__":
    test_get_default_feed()
