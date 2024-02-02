from dotenv import load_dotenv
import logging
import os

from atproto import Client, models, client_utils

from lib.endpoints import ENDPOINTS_MAP

current_file_directory = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.abspath(os.path.join(current_file_directory, "../.env"))
load_dotenv(env_path)

BLUESKY_HANDLE = os.getenv("BLUESKY_HANDLE")
BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_PASSWORD")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

client = Client()
client.login(BLUESKY_HANDLE, BLUESKY_APP_PASSWORD)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())
