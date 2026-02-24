import os
from src.config import RAW_DATA_PATH, HISTORICAL_URL
from src.fetcher import fetch_and_write, clean
from src.writer import write_partition

def backfill(start_year: int, end_year: int) -> None:
    ranges = generate_month_ranges(start_year, end_year)
    
    for start, end in ranges:
        year = int(start[:4])
        month = int(start[5:7])

        if month_already_fetched(year, month):
            print(f"Skipping {start} — already fetched.")
            continue

        print(f"\n--- Fetching {start} to {end} ---")
        where_clause = f"transit_timestamp >= '{start}' AND transit_timestamp < '{end}'"
        
        fetch_and_write(url=HISTORICAL_URL, where_clause=where_clause)
        print(f"Done with {start} to {end}")
# Each tuple is (start, end) for one month
def generate_month_ranges(start_year: int, end_year: int) -> list[tuple]:
    ranges = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if month == 12:
                next_year = year + 1
                next_month = 1
            else:
                next_year = year
                next_month = month + 1
            
            start = f"{year}-{month:02d}-01T00:00:00"
            end = f"{next_year}-{next_month:02d}-01T00:00:00"
            ranges.append((start, end))
    return ranges

def month_already_fetched(year: int, month: int) -> bool:
    """Check if a Parquet file already exists for this year/month."""
    path = os.path.join(RAW_DATA_PATH, f"year={year}", f"month={month}", "data.parquet")
    return os.path.exists(path)

def backfill(start_year: int, end_year: int) -> None:
    ranges = generate_month_ranges(start_year, end_year)
    
    for start, end in ranges:
        year = int(start[:4])
        month = int(start[5:7])

        if month_already_fetched(year, month):
            print(f"Skipping {start} — already fetched.")
            continue

        print(f"\n--- Fetching {start} to {end} ---")
        where_clause = f"transit_timestamp >= '{start}' AND transit_timestamp < '{end}'"
        
        fetch_and_write(url=HISTORICAL_URL, where_clause=where_clause)
        print(f"Done with {start} to {end}")
        
if __name__ == "__main__":
    backfill(start_year=2020, end_year=2024)