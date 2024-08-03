"""Testing the app.py endpoints."""
import datetime
import os
import requests
import base64

root_url = "https://mindtechnologylab.com/manage_user"
# root_url = "http://127.0.0.1:8000/"
endpoint = "manage_user"
url = os.path.join(root_url, endpoint)
# api_key = "your_secret_api_key"
headers = {
    # "x-api-key": api_key,
    "Content-Type": "application/json"
}
test_profile_link = "https://bsky.app/profile/markptorres.bsky.social"
operation = "modify"
payload = {
    "operation": operation,
    "bluesky_user_profile_link": test_profile_link,
    "test_user": False,
    "condition": "engagement"
}

# get payloads

add_user_payload = {
    "operation": "add",
    "bluesky_user_profile_link": test_profile_link,
    "test_user": False,
    "condition": "reverse_chronological"
}

modify_user_payload = {
    "operation": "modify",
    "bluesky_user_profile_link": test_profile_link,
    "test_user": False,
    "condition": "engagement"
}

delete_user_payload = {
    "operation": "delete",
    "bluesky_user_profile_link": test_profile_link,
    "test_user": False,
    "condition": "engagement"
}

# get base64 payloads
base64_add_user_payload = base64.b64encode(
    str(add_user_payload).encode("utf-8")
).decode("utf-8")

base64_modify_user_payload = base64.b64encode(
    str(modify_user_payload).encode("utf-8")
).decode("utf-8")

base64_delete_user_payload = base64.b64encode(
    str(delete_user_payload).encode("utf-8")
).decode("utf-8")

print("Base64 add user payload:", base64_add_user_payload)
print("Base64 modify user payload:", base64_modify_user_payload)
print("Base64 delete user payload:", base64_delete_user_payload)

# response = requests.post(url, headers=headers, json=payload)
current_time = datetime.datetime.now().strftime("%I:%M%p")
print("Current time:", current_time)
# response = requests.get(url, headers=headers) # test same endpoint but GET not POST
# response = requests.get(
#     "http://127.0.0.1:8000/test-get-s3",
#     headers=headers
# )
# response = requests.post(url, headers=headers, json=payload)

# print(response.status_code)
# print(response.json())
