import os
import duckdb
import pandas as pd
from src.config import RAW_DATA_PATH

# Resolve path relative to project root, not the calling file
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PARQUET_PATH = RAW_DATA_PATH

def get_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute(f"""
        CREATE OR REPLACE VIEW ridership AS
        SELECT *
        FROM read_parquet('{PARQUET_PATH}/**/*.parquet', hive_partitioning=true)
    """)
    return conn

def get_hourly_ridership(station: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    """
    Get hourly ridership, optionally filtered by station and date range.
    Returns a DataFrame with transit_timestamp and ridership.
    """
    conn = get_connection()

    filters = []
    if station:
        filters.append(f"station_complex = '{station}'")
    if start:
        filters.append(f"transit_timestamp >= '{start}'")
    if end:
        filters.append(f"transit_timestamp < '{end}'")

    where = f"WHERE {' AND '.join(filters)}" if filters else ""

    return conn.execute(f"""
        SELECT 
            transit_timestamp,
            station_complex,
            SUM(ridership) as ridership
        FROM ridership
        {where}
        GROUP BY transit_timestamp, station_complex
        ORDER BY transit_timestamp ASC
    """).df()


def get_busiest_stations(start: str = None, end: str = None, limit: int = 10) -> pd.DataFrame:
    """Get the top stations by total ridership."""
    conn = get_connection()

    filters = []
    if start:
        filters.append(f"transit_timestamp >= '{start}'")
    if end:
        filters.append(f"transit_timestamp < '{end}'")

    where = f"WHERE {' AND '.join(filters)}" if filters else ""

    return conn.execute(f"""
        SELECT 
            station_complex,
            borough,
            SUM(ridership) as total_ridership
        FROM ridership
        {where}
        GROUP BY station_complex, borough
        ORDER BY total_ridership DESC
        LIMIT {limit}
    """).df()


def get_ridership_by_hour(start: str = None, end: str = None) -> pd.DataFrame:
    """Get average ridership by hour of day across all stations."""
    conn = get_connection()

    filters = []
    if start:
        filters.append(f"transit_timestamp >= '{start}'")
    if end:
        filters.append(f"transit_timestamp < '{end}'")

    where = f"WHERE {' AND '.join(filters)}" if filters else ""

    return conn.execute(f"""
        SELECT 
            HOUR(transit_timestamp) as hour_of_day,
            AVG(ridership) as avg_ridership,
            SUM(ridership) as total_ridership
        FROM ridership
        {where}
        GROUP BY hour_of_day
        ORDER BY hour_of_day ASC
    """).df()


def get_ridership_by_borough(start: str = None, end: str = None) -> pd.DataFrame:
    """Get total ridership broken down by borough."""
    conn = get_connection()

    filters = []
    if start:
        filters.append(f"transit_timestamp >= '{start}'")
    if end:
        filters.append(f"transit_timestamp < '{end}'")

    where = f"WHERE {' AND '.join(filters)}" if filters else ""

    return conn.execute(f"""
        SELECT
            borough,
            SUM(ridership) as total_ridership,
            COUNT(*) as records
        FROM ridership
        {where}
        GROUP BY borough
        ORDER BY total_ridership DESC
    """).df()