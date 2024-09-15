"""Fetch AWS IP ranges and print them."""

import requests

url = "https://ip-ranges.amazonaws.com/ip-ranges.json"
response = requests.get(url)
data = response.json()

console_ips = [
    item["ip_prefix"]
    for item in data["prefixes"]
    if item["region"] == "us-east-2" and item["service"] == "EC2_INSTANCE_CONNECT"
]

print("AWS Console IP ranges for us-east-2:")
for ip in console_ips:
    print(ip)
