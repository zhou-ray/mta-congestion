from src.query import (
    get_hourly_ridership,
    get_busiest_stations,
    get_ridership_by_hour,
    get_ridership_by_borough
)

print("=== Busiest Stations ===")
print(get_busiest_stations())

print("\n=== Ridership by Borough ===")
print(get_ridership_by_borough())

print("\n=== Ridership by Hour of Day ===")
print(get_ridership_by_hour())

print("\n=== Hourly Ridership - Grand Central ===")
print(get_hourly_ridership(station="Grand Central-42 St (S,4,5,6,7)").head(10))