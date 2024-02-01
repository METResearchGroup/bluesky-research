from dotenv import load_dotenv

import os

from atproto import Client, models, client_utils

from lib.endpoints import ENDPOINTS_MAP

current_file_directory = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.abspath(os.path.join(current_file_directory, "../.env"))
load_dotenv(env_path)

BLUESKY_HANDLE = os.getenv("BLUESKY_HANDLE")
BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_PASSWORD")

client = Client()
client.login(BLUESKY_HANDLE, BLUESKY_APP_PASSWORD)
client.send_post("Hello World (from Python client)")

"""
class BlueskyBaseClass:
    def __init__(self) -> None:
        self.username = BLUESKY_HANDLE
        self.password = BLUESKY_APP_PASSWORD
        self.authenticated = self.auth()

    def auth(self) -> bool:
        try:
            payload = {"identifier": self.username, "password": self.password}
            resp = requests.post(
                url=ENDPOINTS_MAP["createSession"], json=payload
            )
            resp.raise_for_status()
            session = resp.json()
            breakpoint()
            print(session["accessJwt"])
            return True
        except requests.exceptions.HTTPError as err:
            print(f"Error during auth: {err}")
            return False
"""