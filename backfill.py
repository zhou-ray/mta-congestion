import os
from src.config import RAW_DATA_PATH, HISTORICAL_URL
from src.fetcher import fetch_and_write_month

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
            ranges.append((start, end, year, month))
    return ranges

def month_already_fetched(year: int, month: int) -> bool:
    """
    Check if a Parquet file exists AND has reasonable row count.
    A complete month should have at least 500k rows across all stations.
    Avoids resuming from incomplete files.
    """
    import polars as pl
    path = os.path.join(RAW_DATA_PATH, f"year={year}", f"month={month}", "data.parquet")
    if not os.path.exists(path):
        return False
    df = pl.read_parquet(path)
    row_count = len(df)
    if row_count < 500_000:
        print(f"  Found incomplete file for {year}/{month} ({row_count} rows) — will re-fetch.")
        return False
    return True

def backfill(start_year: int, end_year: int) -> None:
    ranges = generate_month_ranges(start_year, end_year)

    for start, end, year, month in ranges:
        if month_already_fetched(year, month):
            print(f"Skipping {year}/{month} — already complete.")
            continue

        print(f"\n--- Fetching {year}/{month} ---")
        where_clause = f"transit_timestamp >= '{start}' AND transit_timestamp < '{end}'"
        fetch_and_write_month(url=HISTORICAL_URL, where_clause=where_clause, year=year, month=month)
        print(f"Done with {year}/{month}")

if __name__ == "__main__":
    backfill(start_year=2023, end_year=2024)