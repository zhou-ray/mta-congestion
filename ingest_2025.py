import pandas as pd
from src.config import CURRENT_URL
from src.fetcher import fetch_and_write
from src.writer import get_watermark, set_watermark

def ingest_2025(start_date: str = '2025-01-01'):
    """
    Ingest 2025+ data incrementally using watermark.
    On first run fetches everything from start_date.
    On subsequent runs fetches only new records.
    """
    watermark = get_watermark()

    if watermark:
        where_clause = f"transit_timestamp > '{watermark}'"
        print(f"Incremental update from {watermark}...")
    else:
        where_clause = f"transit_timestamp >= '{start_date}'"
        print(f"Full ingest from {start_date}...")

    fetch_and_write(CURRENT_URL, where_clause=where_clause)

    print("Ingest complete.")

if __name__ == "__main__":
    ingest_2025()