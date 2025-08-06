import os
from dotenv import load_dotenv

load_dotenv()

BLING_CLIENT_ID = os.getenv("BLING_CLIENT_ID")
BLING_CLIENT_SECRET = os.getenv("BLING_CLIENT_SECRET")
BLING_REFRESH_TOKEN = os.getenv("BLING_REFRESH_TOKEN")
BLING_AUTH_CODE = os.getenv("BLING_AUTH_CODE")