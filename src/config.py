from dotenv import load_dotenv
import os

load_dotenv()

APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "data/raw")
CACHE_PATH = os.getenv("CACHE_PATH", "data/cache")
PAGE_SIZE = int(os.getenv("PAGE_SIZE", 50000))

BASE_URL = "https://data.ny.gov/resource/wujg-7c2s.json"