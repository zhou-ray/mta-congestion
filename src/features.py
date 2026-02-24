import pandas as pd
import numpy as np
import holidays

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
                          horizon: int = 2, station_col: str = 'station_complex',
                          precomputed_thresholds: dict = None) -> pd.DataFrame:
    """
    Add a binary congestion label for N hours ahead.
    
    If precomputed_thresholds is provided (a dict of station -> threshold value),
    use those instead of computing from the current DataFrame. This prevents
    data leakage when labeling a test set.
    """
    df = df.copy()

    if precomputed_thresholds:
        df['congestion_threshold'] = df[station_col].map(precomputed_thresholds)
    else:
        df['congestion_threshold'] = df.groupby(station_col)['ridership'].transform(
            lambda x: x.quantile(threshold)
        )

    df['future_ridership'] = df.groupby(station_col)['ridership'].shift(-horizon)
    df['is_congested'] = (df['future_ridership'] >= df['congestion_threshold']).astype(int)
    df = df.drop(columns=['congestion_threshold', 'future_ridership'])
    df = df.dropna(subset=['is_congested']).reset_index(drop=True)

    return df


def compute_congestion_thresholds(df: pd.DataFrame, threshold: float = 0.8,
                                   station_col: str = 'station_complex') -> dict:
    """
    Compute per-station congestion thresholds from a DataFrame.
    Should be called on training data only, then passed to add_congestion_label
    for both train and test sets.
    """
    return df.groupby(station_col)['ridership'].quantile(threshold).to_dict()

def add_holiday_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # US federal holidays — include both Columbus Day and Indigenous Peoples Day
    us_holidays = holidays.US(state='NY', years=range(2020, 2026))
    
    # Manually add Columbus Day / Indigenous Peoples Day for NY
    # Second Monday of October
    def get_columbus_day(year):
        mondays = [d for d in pd.date_range(f'{year}-10-01', f'{year}-10-31') if d.dayofweek == 0]
        return mondays[1].date()
    
    extra_holidays = set()
    for year in range(2020, 2026):
        extra_holidays.add(get_columbus_day(year))

    all_holidays = set(us_holidays.keys()) | extra_holidays

    df['is_holiday'] = df['transit_timestamp'].dt.date.apply(
        lambda x: 1 if x in all_holidays else 0
    )

    holiday_dates = all_holidays

    df['is_holiday_eve'] = df['transit_timestamp'].dt.date.apply(
        lambda x: 1 if (pd.Timestamp(x) + pd.Timedelta(days=1)).date() in holiday_dates else 0
    )
    df['is_holiday_next'] = df['transit_timestamp'].dt.date.apply(
        lambda x: 1 if (pd.Timestamp(x) - pd.Timedelta(days=1)).date() in holiday_dates else 0
    )

    # NYC Marathon — first Sunday of November
    def is_nyc_marathon(ts):
        if ts.month != 11:
            return 0
        first_sunday = pd.Timestamp(year=ts.year, month=11, day=1)
        while first_sunday.dayofweek != 6:
            first_sunday += pd.Timedelta(days=1)
        return 1 if ts.date() == first_sunday.date() else 0

    df['is_nyc_marathon'] = df['transit_timestamp'].apply(is_nyc_marathon)

    # Thanksgiving eve
    def is_thanksgiving_eve(ts):
        if ts.month != 11:
            return 0
        thursdays = [d for d in pd.date_range(f'{ts.year}-11-01', f'{ts.year}-11-30') if d.dayofweek == 3]
        thanksgiving = thursdays[3]
        thanksgiving_eve = thanksgiving - pd.Timedelta(days=1)
        return 1 if ts.date() == thanksgiving_eve.date() else 0

    df['is_thanksgiving_eve'] = df['transit_timestamp'].apply(is_thanksgiving_eve)

    # Pre-Thanksgiving Saturday — Saturday before Thanksgiving week
    def is_pre_thanksgiving_saturday(ts):
        if ts.month != 11 or ts.dayofweek != 5:
            return 0
        thursdays = [d for d in pd.date_range(f'{ts.year}-11-01', f'{ts.year}-11-30') if d.dayofweek == 3]
        thanksgiving = thursdays[3]
        pre_sat = thanksgiving - pd.Timedelta(days=8)
        return 1 if ts.date() == pre_sat.date() else 0

    df['is_pre_thanksgiving_saturday'] = df['transit_timestamp'].apply(is_pre_thanksgiving_saturday)

    return df