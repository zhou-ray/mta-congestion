import polars as pl
import os
import json
from src.config import RAW_DATA_PATH, CACHE_PATH


def write_partition(df: pl.DataFrame) -> None:
    """
    Split a DataFrame by year and month and write each
    partition to its own Parquet file.
    """
    # Extract year and month from timestamp
    df = df.with_columns([
        pl.col("transit_timestamp").dt.year().alias("year"),
        pl.col("transit_timestamp").dt.month().alias("month"),
    ])

    partitions = df.select(["year", "month"]).unique().to_dicts()

    for part in partitions:
        year, month = part["year"], part["month"]

        partition_df = df.filter(
            (pl.col("year") == year) & (pl.col("month") == month)
        ).drop(["year", "month"])  # don't store redundant columns

        path = os.path.join(RAW_DATA_PATH, f"year={year}", f"month={month}")
        os.makedirs(path, exist_ok=True)

        filepath = os.path.join(path, "data.parquet")
        partition_df.write_parquet(filepath)
        print(f"Written {len(partition_df)} rows to {filepath}")
    
    # Update watermark to the latest timestamp in this batch
    latest = df["transit_timestamp"].max()
    set_watermark(str(latest))
        
def get_watermark() -> str | None:
    """Read the last fetched timestamp from cache, or None if first run."""
    path = os.path.join(CACHE_PATH, "watermark.json")
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        return json.load(f)["last_timestamp"]

def set_watermark(timestamp: str) -> None:
    """Write the latest fetched timestamp to cache."""
    os.makedirs(CACHE_PATH, exist_ok=True)
    path = os.path.join(CACHE_PATH, "watermark.json")
    with open(path, "w") as f:
        json.dump({"last_timestamp": timestamp}, f)
    print(f"Watermark updated to {timestamp}")