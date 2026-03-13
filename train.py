import pandas as pd
from src.query import get_connection, get_hourly_ridership
from src.features import (
    add_time_features, add_lag_features, drop_nulls,
    add_holiday_features, add_congestion_label,
    compute_congestion_thresholds
)
from src.station_features import build_station_features, merge_station_features
from src.model import prepare_features, train, evaluate, save_model, save_thresholds
import gc


def build_training_data(start: str, end: str, sample_stations: list = None) -> tuple:
    """
    Build a full feature matrix for all stations over a date range.
    Processes month by month to manage memory.
    """
    print("Building station features...")
    station_df = build_station_features()

    print("Generating month ranges...")
    months = pd.date_range(start=start, end=end, freq='MS')
    all_dfs = []

    for i, month_start in enumerate(months):
        month_end = month_start + pd.offsets.MonthEnd(1)
        month_end_str = (month_end + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        month_start_str = month_start.strftime('%Y-%m-%d')

        print(f"Processing {month_start_str}...")

        df = get_hourly_ridership(start=month_start_str, end=month_end_str)

        if sample_stations:
            df = df[df['station_complex'].isin(sample_stations)]

        if df.empty:
            continue

        df = add_time_features(df)
        df = add_lag_features(df)
        df = drop_nulls(df)
        df = add_holiday_features(df)
        df = merge_station_features(df, station_df)

        all_dfs.append(df)
        gc.collect()

    print("Concatenating all months...")
    full_df = pd.concat(all_dfs, ignore_index=True)
    full_df = full_df.sort_values(['station_complex', 'transit_timestamp']).reset_index(drop=True)

    return full_df, station_df


if __name__ == "__main__":
    sample = [
        'Grand Central-42 St (S,4,5,6,7)',
        'Times Sq-42 St (N,Q,R,W,S,1,2,3,7)/42 St (A,C,E)',
        '34 St-Herald Sq (B,D,F,M,N,Q,R,W)',
        '14 St-Union Sq (L,N,Q,R,W,4,5,6)',
        'Fulton St (A,C,J,Z,2,3,4,5)',
        'Atlantic Av-Barclays Ctr (B,D,N,Q,R,2,3,4,5)',
        'Flushing-Main St (7)',
        '74-Broadway (7)/Jackson Hts-Roosevelt Av (E,F,M,R)',
        'Bay Ridge-95 St (R)',
        'Canarsie-Rockaway Pkwy (L)',
        '161 St-Yankee Stadium (B,D,4)',
        'Fordham Rd (B,D)',
        'Jamaica Center-Parsons/Archer (E,J,Z)',
        'Forest Hills-71 Av (E,F,M,R)',
        'Astoria-Ditmars Blvd (N,W)',
        '86 St (4,5,6)',
        'Court St (R)/Borough Hall (2,3,4,5)',
        'Church Av (B,Q)',
        'Pelham Bay Park (6)',
        'Inwood-207 St (A)'
    ]

    # Train on 2023 only — post-Covid new normal
    # Validate on 2024 — out of sample but same regime
    # Final test on 2025+ — genuinely unseen future data
    print("Building training data 2023-2024...")
    df, station_df = build_training_data(
        start='2023-01-01',
        end='2024-12-31',
        sample_stations=None
    )

    print(f"\nFull dataset shape: {df.shape}")
    print(f"Stations: {df['station_complex'].nunique()}")
    print(f"Date range: {df['transit_timestamp'].min()} to {df['transit_timestamp'].max()}")

    # Temporal split — 2023 train, 2024 validate
    train_df = df[df['transit_timestamp'].dt.year == 2023].copy()
    test_df = df[df['transit_timestamp'].dt.year == 2024].copy()

    print(f"\nTrain: {len(train_df)} rows (2023)")
    print(f"Test: {len(test_df)} rows (2024)")

    # Compute thresholds on 2023 train only
    thresholds = compute_congestion_thresholds(train_df)
    save_thresholds(thresholds)

    # Label both sets with train thresholds
    train_df = add_congestion_label(train_df, horizon=2, precomputed_thresholds=thresholds)
    test_df = add_congestion_label(test_df, horizon=2, precomputed_thresholds=thresholds)

    print(f"\nCongestion rate train: {train_df['is_congested'].mean():.1%}")
    print(f"Congestion rate test: {test_df['is_congested'].mean():.1%}")

    # Drift analysis
    print("\nThreshold drift analysis:")
    train_congestion = train_df.groupby('station_complex')['is_congested'].mean()
    test_congestion = test_df.groupby('station_complex')['is_congested'].mean()
    drift = pd.DataFrame({
        'train_rate': train_congestion,
        'test_rate': test_congestion
    }).dropna()
    drift['drift'] = drift['test_rate'] - drift['train_rate']
    print(drift.sort_values('drift', ascending=False).to_string())

    # Train
    X_train, y_train = prepare_features(train_df)
    X_test, y_test = prepare_features(test_df)

    print("\nTraining global model on 2023 data...")
    model = train(X_train, y_train)
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=50)

    # Evaluate
    evaluate(model, X_test, y_test)

    # Save
    save_model(model, 'global_congestion_model.pkl')