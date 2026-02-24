from src.query import get_hourly_ridership
from src.features import (
    add_time_features, add_lag_features, drop_nulls,
    add_congestion_label, add_holiday_features,
    compute_congestion_thresholds
)
from src.model import prepare_features, train, evaluate

df = get_hourly_ridership(
    station="Grand Central-42 St (S,4,5,6,7)",
    start="2024-01-01",
    end="2024-12-01"
)

df = add_time_features(df)
df = add_lag_features(df)
df = drop_nulls(df)
df = add_holiday_features(df)

# Split BEFORE computing labels
split = int(len(df) * 0.8)
train_df = df.iloc[:split].copy()
test_df = df.iloc[split:].copy()

# Compute threshold on train only
thresholds = compute_congestion_thresholds(train_df)
print(f"Train-only threshold: {thresholds}")

# Apply same threshold to both sets
train_df = add_congestion_label(train_df, horizon=2, precomputed_thresholds=thresholds)
test_df = add_congestion_label(test_df, horizon=2, precomputed_thresholds=thresholds)

X_train, y_train = prepare_features(train_df)
X_test, y_test = prepare_features(test_df)

print(f"\nTraining on {len(X_train)} rows, testing on {len(X_test)} rows")
print(f"Congestion rate train: {y_train.mean():.1%}")
print(f"Congestion rate test: {y_test.mean():.1%}")

model = train(X_train, y_train)
model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=50)
evaluate(model, X_test, y_test)

# Check false negatives
y_prob = model.predict_proba(X_test)[:, 1]
test_df_copy = test_df.copy()
test_df_copy['predicted_prob'] = y_prob
test_df_copy['predicted'] = model.predict(X_test)
test_df_copy['actual'] = y_test.values

fn = test_df_copy[
    (test_df_copy['actual'] == 1) & 
    (test_df_copy['predicted_prob'] < 0.1)
]
print(f"\nHigh confidence false negatives: {len(fn)}")
print(fn[['transit_timestamp', 'ridership', 'hour', 'day_of_week', 'predicted_prob']])

# Check what future_ridership looks like for those false negatives
debug_df = df.iloc[split:].copy()
debug_df['future_ridership'] = debug_df.groupby('station_complex')['ridership'].shift(-2)

check = debug_df[debug_df['transit_timestamp'].dt.date.astype(str).isin([
    '2024-11-01', '2024-11-21', '2024-11-23'
])][['transit_timestamp', 'ridership', 'future_ridership']].copy()

print(check[check['transit_timestamp'].dt.hour.isin([10, 11, 12, 13, 14, 15])].to_string())