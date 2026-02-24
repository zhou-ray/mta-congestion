from src.query import get_hourly_ridership
from src.features import add_time_features

df = get_hourly_ridership(
    station="Grand Central-42 St (S,4,5,6,7)",
    start="2024-01-01",
    end="2024-02-01"
)

df = add_time_features(df)
print(df.columns.tolist())
print(df.head())