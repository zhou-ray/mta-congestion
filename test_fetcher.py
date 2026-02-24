from src.query import get_hourly_ridership
from src.features import add_time_features, add_lag_features, drop_nulls, add_congestion_label
import pandas

df = get_hourly_ridership(
    station="Grand Central-42 St (S,4,5,6,7)",
    start="2024-01-01",
    end="2024-02-01"
)

df = add_time_features(df)
df = add_lag_features(df)
df = drop_nulls(df)
df = add_congestion_label(df, horizon=2)

print(df[['transit_timestamp', 'ridership', 'lag_1', 'lag_168', 'is_congested']].head(20))
print(f"\nCongestion rate: {df['is_congested'].mean():.1%}")
print(f"Total rows: {len(df)}")