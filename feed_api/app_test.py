"""Testing the app.py endpoints."""
import datetime
import os
import requests
import base64

root_url = "https://mindtechnologylab.com/"
endpoint = "manage_user"
url = os.path.join(root_url, endpoint)

# api_key = "your_secret_api_key"
headers = {
    # "x-api-key": api_key,
    "Content-Type": "application/json"
}
# test_profile_link = "https://bsky.app/profile/jbouie.bsky.social" # this user is very active.
test_profile_link_2 = "https://bsky.app/profile/kilgoretrout.bsky.social"  # another active user.
operation = "add"

# get payloads
add_user_payload = {
    "operation": "add",
    "bluesky_user_profile_link": test_profile_link_2,
    "test_user": False,
    "condition": "reverse_chronological"
}

modify_user_payload = {
    "operation": "modify",
    "bluesky_user_profile_link": test_profile_link_2,
    "test_user": False,
    "condition": "engagement"
}

delete_user_payload = {
    "operation": "delete",
    "bluesky_user_profile_link": test_profile_link_2,
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

# print("Base64 add user payload:", base64_add_user_payload)
# print("Base64 modify user payload:", base64_modify_user_payload)
# print("Base64 delete user payload:", base64_delete_user_payload)

current_time = datetime.datetime.now().strftime("%I:%M%p")
print("Current time:", current_time)

operation_to_payload_map = {
    "add": base64_add_user_payload,
    "modify": base64_modify_user_payload,
    "delete": base64_delete_user_payload
}

response = requests.post(
    url=url, headers=headers, json=add_user_payload
)

# breakpoint()
print(response.status_code)
print(response.json())
