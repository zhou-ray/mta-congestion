from dotenv import load_dotenv
import os

load_dotenv()

# Project root is always one level up from this file (src/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "raw").replace("\\", "/")
CACHE_PATH = os.path.join(PROJECT_ROOT, "data", "cache").replace("\\", "/")
PAGE_SIZE = int(os.getenv("PAGE_SIZE", 50000))

# Dataset endpoints
HISTORICAL_URL = "https://data.ny.gov/resource/wujg-7c2s.json"  # 2020-2024
CURRENT_URL = "https://data.ny.gov/resource/5wq4-mkjj.json"     # 2025+