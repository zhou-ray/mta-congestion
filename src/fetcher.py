import requests
import time
import polars as pl
from src.config import APP_TOKEN, PAGE_SIZE


def fetch_page(offset: int, url: str, where_clause: str = None) -> list[dict]:
    """Fetch a single page of results from the SODA API."""
    params = {
        "$limit": PAGE_SIZE,
        "$offset": offset,
        "$order": "transit_timestamp ASC",
    }
    if where_clause:
        params["$where"] = where_clause
    if APP_TOKEN:
        params["$$app_token"] = APP_TOKEN

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def fetch_and_write(url: str, where_clause: str = None) -> None:
    """
    Fetch data incrementally and write to Parquet.
    Updates watermark only at the end of the full fetch.
    Used for incremental 2025+ updates only.
    """
    from src.writer import write_partition_no_watermark, set_watermark

    offset = 0
    chunk = []
    CHUNK_SIZE = 200_000
    latest_timestamp = None

    while True:
        print(f"Fetching rows {offset} to {offset + PAGE_SIZE}...")
        records = fetch_page(offset, url, where_clause)
        if not records:
            break
        chunk.extend(records)
        offset += PAGE_SIZE

        if len(chunk) >= CHUNK_SIZE:
            print(f"Writing chunk of {len(chunk)} rows...")
            df = pl.DataFrame(chunk)
            df = clean(df)
            batch_latest = df["transit_timestamp"].max()
            if latest_timestamp is None or batch_latest > latest_timestamp:
                latest_timestamp = batch_latest
            write_partition_no_watermark(df)
            chunk = []

        if len(records) < PAGE_SIZE:
            break
        time.sleep(0.5)

    if chunk:
        print(f"Writing final chunk of {len(chunk)} rows...")
        df = pl.DataFrame(chunk)
        df = clean(df)
        batch_latest = df["transit_timestamp"].max()
        if latest_timestamp is None or batch_latest > latest_timestamp:
            latest_timestamp = batch_latest
        write_partition_no_watermark(df)

    if latest_timestamp is not None:
        latest_iso = latest_timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        set_watermark(latest_iso)
        print(f"Watermark updated to {latest_iso}")

    print(f"Done. Total rows processed through offset {offset}")

def fetch_all(url: str, where_clause: str = None) -> pl.DataFrame:
    """
    Paginate through the dataset and return a single Polars DataFrame.
    """
    all_records = []
    offset = 0

    while True:
        print(f"Fetching rows {offset} to {offset + PAGE_SIZE}...")
        records = fetch_page(offset, url, where_clause)

        if not records:
            break

        all_records.extend(records)
        offset += PAGE_SIZE

        if len(records) < PAGE_SIZE:
            break

        time.sleep(0.5)

    print(f"Done. Total records fetched: {len(all_records)}")
    return pl.DataFrame(all_records)

def clean(df: pl.DataFrame) -> pl.DataFrame:
    """Cast columns to correct types and drop redundant fields."""
    return (
        df
        .with_columns([
            pl.col("transit_timestamp").str.to_datetime("%Y-%m-%dT%H:%M:%S%.f"),
            pl.col("ridership").cast(pl.Float64),
            pl.col("transfers").cast(pl.Float64),
            pl.col("latitude").cast(pl.Float64),
            pl.col("longitude").cast(pl.Float64),
        ])
        .drop("georeference")
    )
    
def fetch_and_write_month(url: str, where_clause: str, year: int, month: int) -> None:
    """
    Fetch a single month of data and write to Parquet.
    Never updates the watermark — this is for backfill only.
    Overwrites any existing incomplete file for this month.
    """
    from src.writer import write_partition_no_watermark
    import os
    from src.config import RAW_DATA_PATH
    import polars as pl

    offset = 0
    all_records = []

    while True:
        print(f"  Fetching rows {offset} to {offset + PAGE_SIZE}...")
        records = fetch_page(offset, url, where_clause)
        if not records:
            break
        all_records.extend(records)
        offset += PAGE_SIZE
        if len(records) < PAGE_SIZE:
            break
        time.sleep(0.5)

    if not all_records:
        print(f"  No records found for {year}/{month}")
        return

    print(f"  Processing {len(all_records)} records...")
    df = pl.DataFrame(all_records)
    df = clean(df)

    # Write directly to the correct partition, overwriting any incomplete file
    path = os.path.join(RAW_DATA_PATH, f"year={year}", f"month={month}")
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, "data.parquet")
    df.write_parquet(filepath)
    print(f"  Written {len(df)} rows to {filepath}")