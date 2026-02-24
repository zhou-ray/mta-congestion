import pandas as pd
import numpy as np


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract time-based features from transit_timestamp.
    Input df should have transit_timestamp as a datetime column.
    """
    df = df.copy()
    
    dt = df['transit_timestamp']
    
    # Basic time features
    df['hour'] = dt.dt.hour
    df['day_of_week'] = dt.dt.dayofweek  # 0=Monday, 6=Sunday
    df['month'] = dt.dt.month
    df['week_of_year'] = dt.dt.isocalendar().week.astype(int)
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    
    # Shoulder days — Monday and Friday behave differently
    df['is_shoulder_day'] = df['day_of_week'].isin([0, 4]).astype(int)
    
    # One-hot encode day of week
    for i, day in enumerate(['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']):
        df[f'day_{day}'] = (df['day_of_week'] == i).astype(int)
    
    # Cyclical encoding for hour and month
    # This tells the model that hour 23 and hour 0 are adjacent
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    
    return df

def add_lag_features(df: pd.DataFrame, station_col: str = 'station_complex') -> pd.DataFrame:
    """
    Add lag and rolling window features for each station independently.
    Must be sorted by station and timestamp before calling.
    """
    df = df.copy()
    df = df.sort_values([station_col, 'transit_timestamp']).reset_index(drop=True)

    # Lag features — per station group so we don't bleed across stations
    df['lag_1'] = df.groupby(station_col)['ridership'].shift(1)
    df['lag_24'] = df.groupby(station_col)['ridership'].shift(24)
    df['lag_168'] = df.groupby(station_col)['ridership'].shift(168)

    # Rolling window features
    df['roll_mean_24'] = (
        df.groupby(station_col)['ridership']
        .transform(lambda x: x.shift(1).rolling(window=24, min_periods=12).mean())
    )
    df['roll_mean_168'] = (
        df.groupby(station_col)['ridership']
        .transform(lambda x: x.shift(1).rolling(window=168, min_periods=84).mean())
    )
    df['roll_std_24'] = (
        df.groupby(station_col)['ridership']
        .transform(lambda x: x.shift(1).rolling(window=24, min_periods=12).std())
    )

    return df

def drop_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows with any null values in feature columns."""
    feature_cols = ['lag_1', 'lag_24', 'lag_168', 'roll_mean_24', 'roll_mean_168', 'roll_std_24']
    return df.dropna(subset=feature_cols).reset_index(drop=True)

def add_congestion_label(df: pd.DataFrame, threshold: float = 0.8, 
                          horizon: int = 2, station_col: str = 'station_complex') -> pd.DataFrame:
    """
    Add a binary congestion label for N hours ahead.
    A station-hour is considered congested if ridership at 
    horizon hours in the future exceeds the 80th percentile 
    for that station.
    
    horizon: how many hours ahead to predict (default 2)
    """
    df = df.copy()

    # Compute per-station congestion threshold on current ridership
    df['congestion_threshold'] = df.groupby(station_col)['ridership'].transform(
        lambda x: x.quantile(threshold)
    )

    # Shift ridership backward by horizon to create future target
    df['future_ridership'] = df.groupby(station_col)['ridership'].shift(-horizon)

    # Label is 1 if future ridership exceeds station's threshold
    df['is_congested'] = (df['future_ridership'] >= df['congestion_threshold']).astype(int)

    df = df.drop(columns=['congestion_threshold', 'future_ridership'])

    # Drop rows where future is unknown (last N rows per station)
    df = df.dropna(subset=['is_congested']).reset_index(drop=True)

    return df