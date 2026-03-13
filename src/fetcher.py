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


def fetch_and_write(url: str, where_clause: str = None, update_watermark: bool = True) -> None:
    """
    Fetch data page by page and write to Parquet incrementally.
    Only updates watermark at the end of the full ingest.
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
            # Track latest timestamp but don't update watermark yet
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

    # Update watermark once at the very end
    if update_watermark and latest_timestamp is not None:
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