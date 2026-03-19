import pandas as pd
import re
from src.query import get_connection


def extract_num_lines(station_name: str) -> int:
    """
    Extract number of lines serving a station from its name.
    e.g. 'Grand Central-42 St (S,4,5,6,7)' -> 5
    """
    # Find all parenthetical groups and count comma-separated entries
    matches = re.findall(r'\(([^)]+)\)', station_name)
    if not matches:
        return 1
    # Flatten all line letters across all parenthetical groups
    lines = []
    for match in matches:
        lines.extend([l.strip() for l in match.split(',')])
    return len(lines)


def get_terminal_stations() -> set:
    """
    Return a set of known NYC subway terminal station names.
    These are endpoints where trains turn around.
    """
    return {
        'Flushing-Main St (7)',
        'Inwood-207 St (A)',
        'Far Rockaway-Mott Av (A)',
        'Rockaway Park-Beach 116 St (A,S)',
        'Pelham Bay Park (6)',
        'Wakefield-241 St (2)',
        'Flatbush Av-Brooklyn College (2,5)',
        'New Utrecht Av (N)/62 St (D)',
        'Bay Ridge-95 St (R)',
        'Coney Island-Stillwell Av (D,F,N,Q)',
        '8 Av (L)',
        'Canarsie-Rockaway Pkwy (L)',
        'Jamaica-179 St (F)',
        'Jamaica Center-Parsons/Archer (E,J,Z)',
        'Norwood-205 St (D)',
        'New Lots Av (3)',
        'Eastchester-Dyre Av (5)',
        'Nereid Av (2,5)',
        'Woodlawn (4)',
        'Ozone Park-Lefferts Blvd (A)',
        'Howard Beach-JFK Airport (A)',
        'Forest Hills-71 Av (E,F,M,R)',
        'Jamaica-Van Wyck (E)',
    }


def build_station_features() -> pd.DataFrame:
    """
    Build a DataFrame of static station-level features.
    One row per station.
    """
    conn = get_connection()

    # Get base station info and historical ridership stats
    station_df = conn.execute("""
        SELECT
            station_complex,
            station_complex_id,
            borough,
            AVG(hourly_ridership) as avg_ridership,
            STDDEV(hourly_ridership) as std_ridership,
            MAX(hourly_ridership) as max_ridership,
            COUNT(DISTINCT DATE_TRUNC('month', transit_timestamp)) as months_of_data
        FROM (
            SELECT
                station_complex,
                station_complex_id,
                borough,
                transit_timestamp,
                SUM(ridership) as hourly_ridership
            FROM ridership
            WHERE year IN (2023, 2024)
            GROUP BY station_complex, station_complex_id, borough, transit_timestamp
        ) hourly
        GROUP BY station_complex, station_complex_id, borough
    """).df()

    # Extract number of lines from station name
    station_df['num_lines'] = station_df['station_complex'].apply(extract_num_lines)

    # Terminal station flag
    terminals = get_terminal_stations()
    station_df['is_terminal'] = station_df['station_complex'].apply(
        lambda x: 1 if any(t.lower() in x.lower() for t in terminals) else 0
    )

    # Borough one-hot encoding
    for borough in ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island']:
        col = 'borough_' + borough.lower().replace(' ', '_')
        station_df[col] = (station_df['borough'] == borough).astype(int)

    # Station size tier based on avg ridership
    # This gives the model a coarse categorical sense of station scale
    station_df['station_tier'] = pd.qcut(
        station_df['avg_ridership'],
        q=4,
        labels=[1, 2, 3, 4]  # 1=smallest, 4=largest
    ).astype(int)

    print(f"Built features for {len(station_df)} stations")
    print(f"\nStation tier distribution:")
    print(station_df['station_tier'].value_counts().sort_index())
    print(f"\nLines range: {station_df['num_lines'].min()} to {station_df['num_lines'].max()}")
    print(f"\nTerminal stations found: {station_df['is_terminal'].sum()}")

    return station_df


def merge_station_features(df: pd.DataFrame, station_df: pd.DataFrame) -> pd.DataFrame:
    """Merge station features into a ridership DataFrame."""
    feature_cols = [
        'station_complex', 'num_lines', 'is_terminal',
        'avg_ridership', 'std_ridership', 'max_ridership',
        'station_tier',
        'borough_manhattan', 'borough_brooklyn', 'borough_queens',
        'borough_bronx', 'borough_staten_island'
    ]
    return df.merge(station_df[feature_cols], on='station_complex', how='left')