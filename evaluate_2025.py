import pandas as pd
from src.query import get_hourly_ridership
from src.features import (
    add_time_features, add_lag_features, drop_nulls,
    add_holiday_features, add_congestion_label
)
from src.station_features import build_station_features, merge_station_features
from src.model import prepare_features, evaluate, load_model, load_thresholds
import gc
from datetime import datetime

if __name__ == "__main__":
    print("Loading model and thresholds...")
    model = load_model('global_congestion_model.pkl')
    thresholds = load_thresholds()

    print("Building station features...")
    station_df = build_station_features()

    end_date = (datetime.now().replace(day=1)).strftime('%Y-%m-%d')
    months = pd.date_range(start='2025-01-01', end=end_date, freq='MS')
    all_dfs = []

    for month_start in months:
        month_end = month_start + pd.offsets.MonthEnd(1)
        month_end_str = (month_end + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        month_start_str = month_start.strftime('%Y-%m-%d')

        print(f"Processing {month_start_str}...")
        df = get_hourly_ridership(start=month_start_str, end=month_end_str)

        if df.empty:
            continue

        df = add_time_features(df)
        df = add_lag_features(df)
        df = drop_nulls(df)
        df = add_holiday_features(df)
        df = merge_station_features(df, station_df)

        all_dfs.append(df)
        gc.collect()

    print("Concatenating...")
    full_df = pd.concat(all_dfs, ignore_index=True)
    full_df = full_df.sort_values(['station_complex', 'transit_timestamp']).reset_index(drop=True)

    # Label using 2023 thresholds
    full_df = add_congestion_label(full_df, horizon=2, precomputed_thresholds=thresholds)

    print(f"\n2025+ dataset shape: {full_df.shape}")
    print(f"Stations: {full_df['station_complex'].nunique()}")
    print(f"Date range: {full_df['transit_timestamp'].min()} to {full_df['transit_timestamp'].max()}")
    print(f"Congestion rate: {full_df['is_congested'].mean():.1%}")

    X, y = prepare_features(full_df)

    print("\n=== 2025+ Evaluation (Genuinely Unseen Data) ===")
    evaluate(model, X, y)