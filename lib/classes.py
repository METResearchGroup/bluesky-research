from dotenv import load_dotenv

import os
import requests

from lib.endpoints import ENDPOINTS_MAP

BLUESKY_HANDLE = os.getenv("BLUESKY_HANDLE")
BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_PASSWORD")

current_file_directory = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.abspath(os.path.join(current_file_directory, "../.env"))
load_dotenv(env_path)

class BlueskyBaseClass:
    def __init__(self) -> None:
        self.username = BLUESKY_HANDLE
        self.password = BLUESKY_APP_PASSWORD
        self.authenticated = self.auth()

    def auth(self) -> bool:
        try:
            resp = requests.post(
                ENDPOINTS_MAP["createSession"],
                json={"identifier": self.username, "password": self.password},
            )
            resp.raise_for_status()
            session = resp.json()
            print(session["accessJwt"])
            return True
        except requests.exceptions.HTTPError as err:
            print(f"Error during auth: {err}")
            return False
