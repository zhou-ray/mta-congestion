import requests
import time
import polars as pl
from src.config import APP_TOKEN, BASE_URL, PAGE_SIZE


def fetch_page(offset: int, where_clause: str = None) -> list[dict]:
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

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()


def fetch_all(where_clause: str = None) -> pl.DataFrame:
    """
    Paginate through the dataset and return a single Polars DataFrame.
    """
    all_records = []
    offset = 0

    while True:
        print(f"Fetching rows {offset} to {offset + PAGE_SIZE}...")
        records = fetch_page(offset, where_clause)

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