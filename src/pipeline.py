from src.fetcher import fetch_all, clean
from src.writer import write_partition, get_watermark, set_watermark


def run_pipeline(start_date: str = None) -> None:
    """
    Run the ingestion pipeline. If a watermark exists, only fetch
    data newer than the last recorded timestamp. Otherwise fetch
    from start_date.
    """
    watermark = get_watermark()

    if watermark:
        print(f"Watermark found: {watermark}")
        where_clause = f"transit_timestamp > '{watermark}'"
    elif start_date:
        print(f"No watermark found. Fetching from {start_date}")
        where_clause = f"transit_timestamp >= '{start_date}'"
    else:
        print("No watermark or start date provided. Fetching all data.")
        where_clause = None

    df = fetch_all(where_clause=where_clause)

    if df.is_empty():
        print("No new data found.")
        return

    df = clean(df)
    write_partition(df)
    print("Pipeline complete.")


if __name__ == "__main__":
    run_pipeline(start_date="2024-01-01T00:00:00")