import os
from src.config import RAW_DATA_PATH

def month_already_fetched(year: int, month: int) -> bool:
    """Check if a Parquet file already exists for this year/month."""
    path = os.path.join(RAW_DATA_PATH, f"year={year}", f"month={month:02d}", "data.parquet")
    return os.path.exists(path)


def backfill(start_year: int, end_year: int) -> None:
    ranges = generate_month_ranges(start_year, end_year)
    
    for start, end in ranges:
        # Parse year and month from start string
        year = int(start[:4])
        month = int(start[5:7])

        if month_already_fetched(year, month):
            print(f"Skipping {start} — already fetched.")
            continue

        print(f"\n--- Fetching {start} to {end} ---")
        where_clause = f"transit_timestamp >= '{start}' AND transit_timestamp < '{end}'"
        
        df = fetch_all(url=HISTORICAL_URL, where_clause=where_clause)
        
        if df.is_empty():
            print(f"No data for {start}, skipping.")
            continue
        
        df = clean(df)
        write_partition(df)
        print(f"Done with {start} to {end}")