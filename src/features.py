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