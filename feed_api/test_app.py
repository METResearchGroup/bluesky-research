"""Send test request to the app and check the response."""

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization
import jwt
import datetime
import requests


test_user_did = "did:plc:w5mjarupsl6ihdrzwgnzdh4"


# Generate private key
private_key = ec.generate_private_key(ec.SECP256K1())

# Serialize private key
pem_private_key = private_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

# Generate public key
public_key = private_key.public_key()

# Serialize public key
pem_public_key = public_key.public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo,
)

# Create JWT
header = {"alg": "ES256K", "typ": "JWT"}

payload = {
    "iss": test_user_did,
    "aud": "did:example:feedGenerator",
    "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=30),
}

token = jwt.encode(payload, private_key, algorithm="ES256K", headers=header)
print(f"JWT: {token}")

# Send request to FastAPI endpoint
url = "http://127.0.0.1:8000/xrpc/app.bsky.feed.getFeedSkeleton"
headers = {
    "Authorization": f"Bearer {token}",  # Use the generated JWT
    "accept": "application/json",
}
params = {"limit": 3}

response = requests.get(url, headers=headers, params=params)
print(response.status_code)
print(response.json())
