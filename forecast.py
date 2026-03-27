import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
import holidays
from src.query import get_connection
from src.station_features import build_station_features, merge_station_features
from src.model import load_model, load_thresholds, FEATURE_COLS
import re

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), 'visualizations')


def build_lag_lookup() -> pd.DataFrame:
    json_path = os.path.join(os.path.dirname(__file__), 'data', 'lag_lookup.json')

    if os.path.exists(json_path):
        print("  Loading lag lookup from pre-exported JSON...")
        with open(json_path) as f:
            lookup = json.load(f)
        rows = []
        for station, dows in lookup.items():
            for dow, hours in dows.items():
                for hour, avg in hours.items():
                    rows.append({
                        'station_complex': station,
                        'dow': int(dow),
                        'hour': int(hour),
                        'avg_ridership': avg
                    })
        df = pd.DataFrame(rows)
        print(f"  Lag lookup: {len(df)} rows across {df['station_complex'].nunique()} stations")
        return df

    print("Building lag lookup from Parquet...")
    conn = get_connection()
    df = conn.execute("""
        SELECT
            station_complex,
            DAYOFWEEK(transit_timestamp) as dow,
            HOUR(transit_timestamp) as hour,
            AVG(hourly_ridership) as avg_ridership
        FROM (
            SELECT
                station_complex,
                transit_timestamp,
                SUM(ridership) as hourly_ridership
            FROM ridership
            WHERE year = 2023
            AND latitude IS NOT NULL
            GROUP BY station_complex, transit_timestamp
        ) hourly
        GROUP BY station_complex, DAYOFWEEK(transit_timestamp), HOUR(transit_timestamp)
        ORDER BY station_complex, dow, hour
    """).df()
    print(f"  Lag lookup: {len(df)} rows across {df['station_complex'].nunique()} stations")
    return df


def build_lag_dict(lag_lookup: pd.DataFrame) -> dict:
    """
    Convert lag lookup DataFrame to nested dict for O(1) lookups.
    Structure: {station: {dow: {hour: avg_ridership}}}
    """
    lag_dict = {}
    for _, row in lag_lookup.iterrows():
        s = row['station_complex']
        d = int(row['dow'])
        h = int(row['hour'])
        if s not in lag_dict:
            lag_dict[s] = {}
        if d not in lag_dict[s]:
            lag_dict[s][d] = {}
        lag_dict[s][d][h] = float(row['avg_ridership'])
    return lag_dict


def get_station_averages(station: str, lag_dict: dict) -> dict:
    """
    Precompute per-station summary stats used for rolling features.
    Returns dict of dow -> mean, overall mean, overall std.
    """
    if station not in lag_dict:
        return {'dow_means': {}, 'overall_mean': 0.0, 'overall_std': 0.0}

    all_vals = []
    dow_means = {}
    for dow, hours in lag_dict[station].items():
        vals = list(hours.values())
        dow_means[dow] = float(np.mean(vals))
        all_vals.extend(vals)

    return {
        'dow_means': dow_means,
        'overall_mean': float(np.mean(all_vals)) if all_vals else 0.0,
        'overall_std': float(np.std(all_vals)) if all_vals else 0.0
    }


def build_feature_row_fast(station: str, dt: datetime, lag_dict: dict,
                            station_averages: dict, station_row: pd.Series,
                            us_holidays_set: set) -> dict:
    """
    Build a single feature row using precomputed dict lookups.
    """    
    hour = dt.hour
    dow = dt.weekday()  # 0=Monday, 6=Sunday
    month = dt.month
    week_of_year = dt.isocalendar()[1]

    # Convert pandas dow to DuckDB dow (0=Sun, 1=Mon ... 6=Sat)
    duckdb_dow = (dow + 1) % 7

    def lookup(d, h):
        try:
            return lag_dict[station][d][h]
        except KeyError:
            return 0.0

    lag_1 = lookup(duckdb_dow, (hour - 1) % 24)
    lag_24 = lookup((duckdb_dow - 1) % 7, hour)
    lag_168 = lookup(duckdb_dow, hour)

    avgs = station_averages.get(station,  {})
    roll_mean_24 = avgs.get('dow_means', {}).get(duckdb_dow, 0.0)
    roll_mean_168 = avgs.get('overall_mean', 0.0)
    roll_std_24 = avgs.get('overall_std', 0.0)

    date = dt.date()
    tomorrow = (dt + timedelta(days=1)).date()
    yesterday = (dt - timedelta(days=1)).date()

    is_holiday = 1 if date in us_holidays_set else 0
    is_holiday_eve = 1 if tomorrow in us_holidays_set else 0
    is_holiday_next = 1 if yesterday in us_holidays_set else 0

    is_nyc_marathon = 0
    if month == 11 and dow == 6:
        first_sunday = pd.Timestamp(year=dt.year, month=11, day=1)
        while first_sunday.dayofweek != 6:
            first_sunday += timedelta(days=1)
        if date == first_sunday.date():
            is_nyc_marathon = 1

    is_thanksgiving_eve = 0
    if month == 11:
        thursdays = [d for d in pd.date_range(f'{dt.year}-11-01', f'{dt.year}-11-30')
                     if d.dayofweek == 3]
        thanksgiving = thursdays[3]
        if date == (thanksgiving - timedelta(days=1)).date():
            is_thanksgiving_eve = 1

    is_pre_thanksgiving_saturday = 0
    if month == 11 and dow == 5:
        thursdays = [d for d in pd.date_range(f'{dt.year}-11-01', f'{dt.year}-11-30')
                     if d.dayofweek == 3]
        thanksgiving = thursdays[3]
        if date == (thanksgiving - timedelta(days=8)).date():
            is_pre_thanksgiving_saturday = 1

    return {
        'hour': hour,
        'day_of_week': dow,
        'month': month,
        'week_of_year': int(week_of_year),
        'is_weekend': 1 if dow >= 5 else 0,
        'is_shoulder_day': 1 if dow in [0, 4] else 0,
        'day_mon': 1 if dow == 0 else 0,
        'day_tue': 1 if dow == 1 else 0,
        'day_wed': 1 if dow == 2 else 0,
        'day_thu': 1 if dow == 3 else 0,
        'day_fri': 1 if dow == 4 else 0,
        'day_sat': 1 if dow == 5 else 0,
        'day_sun': 1 if dow == 6 else 0,
        'hour_sin': float(np.sin(2 * np.pi * hour / 24)),
        'hour_cos': float(np.cos(2 * np.pi * hour / 24)),
        'month_sin': float(np.sin(2 * np.pi * month / 12)),
        'month_cos': float(np.cos(2 * np.pi * month / 12)),
        'lag_1': lag_1,
        'lag_24': lag_24,
        'lag_168': lag_168,
        'roll_mean_24': roll_mean_24,
        'roll_mean_168': roll_mean_168,
        'roll_std_24': roll_std_24,
        'is_holiday': is_holiday,
        'is_holiday_eve': is_holiday_eve,
        'is_holiday_next': is_holiday_next,
        'is_nyc_marathon': is_nyc_marathon,
        'is_thanksgiving_eve': is_thanksgiving_eve,
        'is_pre_thanksgiving_saturday': is_pre_thanksgiving_saturday,
        'num_lines': int(station_row['num_lines']),
        'is_terminal': int(station_row['is_terminal']),
        'avg_ridership': float(station_row['avg_ridership']),
        'std_ridership': float(station_row['std_ridership']),
        'max_ridership': float(station_row['max_ridership']),
        'station_tier': int(station_row['station_tier']),
        'borough_manhattan': int(station_row['borough_manhattan']),
        'borough_brooklyn': int(station_row['borough_brooklyn']),
        'borough_queens': int(station_row['borough_queens']),
        'borough_bronx': int(station_row['borough_bronx']),
        'borough_staten_island': int(station_row['borough_staten_island']),
    }

def deduplicate_stations(predictions: dict, known_stations: set) -> dict:
    """
    Remove duplicate station entries caused by line renaming
    or inconsistent ordering of line letters in station names.
    Always prefers the station name that exists in the training data (known_stations).
    """

    # Manual overrides for stations where line letters changed over time
    # Key = name to remove, Value = canonical name to keep
    MANUAL_OVERRIDES = {
        'Alabama Av (J,Z)':           'Alabama Av (J)',
        'Queens Plaza (E,F,R)':        'Queens Plaza (E,M,R)',
        '5 Av/53 St (E,F)':            '5 Av/53 St (E,M)',
        'Lexington Av/63 St (F,Q)':    'Lexington Av/63 St (M,Q)',
    }

    # Step 1 — apply manual overrides
    # If the override target (canonical) is already in predictions, keep it and drop the alias
    # If only the alias exists, rename it to the canonical
    after_overrides = {}
    for station, preds in predictions.items():
        canonical = MANUAL_OVERRIDES.get(station, station)
        if canonical == station:
            # Not an override key — add normally
            if station not in after_overrides:
                after_overrides[station] = preds
        else:
            # This station is an alias — only add if canonical not already present
            if canonical not in after_overrides:
                after_overrides[canonical] = preds
            # If canonical already exists, discard this alias entry

    # Also make sure any canonical that wasn't in predictions but whose alias was
    # gets the right predictions
    for alias, canonical in MANUAL_OVERRIDES.items():
        if alias in predictions and canonical in predictions:
            # Both exist — keep canonical, which is already in after_overrides
            pass

    predictions = after_overrides

    # Step 2 — normalize station names by sorting line letters inside parentheses
    # This catches cases like (A,C,B,D,1) vs (A,B,C,D,1) — same station, different order
    def normalize_name(name):
        def sort_parens(match):
            lines = sorted([l.strip() for l in match.group(1).split(',')])
            return '(' + ','.join(lines) + ')'
        return re.sub(r'\(([^)]+)\)', sort_parens, name)

    # Group stations by normalized name
    normalized = {}
    for station in predictions:
        norm = normalize_name(station)
        if norm not in normalized:
            normalized[norm] = []
        normalized[norm].append(station)

    # Step 3 — for each group, keep the name from known_stations (training data)
    deduped = {}
    removed = []
    for norm, originals in normalized.items():
        if len(originals) == 1:
            deduped[originals[0]] = predictions[originals[0]]
        else:
            # Prefer the name that exists in training data
            known = [s for s in originals if s in known_stations]
            if known:
                best = known[0]
            elif len(known) == 0:
                # None in training data — keep longest name as tiebreaker
                best = max(originals, key=len)
            else:
                best = known[0]
            deduped[best] = predictions[best]
            removed.extend([s for s in originals if s != best])

    if removed:
        print(f"  Removed {len(removed)} duplicates: {removed}")

    # Verify all kept stations are the known ones where possible
    for norm, originals in normalized.items():
        if len(originals) > 1:
            kept = [s for s in originals if s in deduped]
            known = [s for s in originals if s in known_stations]
            if known and kept and kept[0] not in known_stations:
                print(f"  WARNING: kept {kept[0]} but training data has {known[0]}")

    return deduped
   
def run_forecast(days_ahead: int = 90):
    print(f"Running forecast for next {days_ahead} days...")

    model = load_model('global_congestion_model.pkl')
    lag_lookup = build_lag_lookup()
    station_df = build_station_features()

    print("  Building fast lookup structures...")
    lag_dict = build_lag_dict(lag_lookup)
    
    def normalize(name):
        def sort_parens(match):
            lines = sorted([l.strip() for l in match.group(1).split(',')])
            return '(' + ','.join(lines) + ')'
        return re.sub(r'\(([^)]+)\)', sort_parens, name)

    station_averages = {s: get_station_averages(s, lag_dict)
                        for s in lag_dict.keys()}

    today = datetime.now().date()
    years = list(set([today.year, (today + timedelta(days=days_ahead)).year]))
    us_holidays_lib = holidays.US(state='NY', years=years)
    us_holidays_set = set(us_holidays_lib.keys())

    station_index = station_df.set_index('station_complex')
    dates = [datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
             + timedelta(days=d) for d in range(days_ahead)]

    all_predictions = {}
    thresholds = load_thresholds()
    known_stations = set(thresholds.keys())
    stations = [s for s in station_df['station_complex'].tolist() if s in known_stations]
    print(f"  Building predictions for {len(stations)} known training stations")
    
    print(f"  Forecasting {len(stations)} stations x 24 hours x {days_ahead} days...")
    print(f"  Total predictions: {len(stations) * 24 * days_ahead:,}")

    for i, station in enumerate(stations):
        if i % 50 == 0:
            print(f"  Station {i+1}/{len(stations)}...")

        if station not in station_index.index:
            continue

        station_row = station_index.loc[station]
        station_predictions = {}

        for dt_base in dates:
            date_str = dt_base.strftime('%Y-%m-%d')
            rows = []
            for hour in range(24):
                dt = dt_base.replace(hour=hour)
                row = build_feature_row_fast(
                    station, dt, lag_dict, station_averages,
                    station_row, us_holidays_set
                )
                rows.append(row)

            X = pd.DataFrame(rows)[FEATURE_COLS]
            probs = model.predict_proba(X)[:, 1]
            station_predictions[date_str] = {
                h: round(float(p), 3) for h, p in enumerate(probs)
            }

        all_predictions[station] = station_predictions

    # Station metadata
    station_meta = {}
    for _, row in station_df.iterrows():
        s = row['station_complex']
        borough = 'Unknown'
        for b in ['manhattan', 'brooklyn', 'queens', 'bronx', 'staten_island']:
            if row.get('borough_' + b, 0) == 1:
                borough = b.replace('_', ' ').title()
                break
        station_meta[s] = {
            'borough': borough,
            'tier': int(row['station_tier'])
        }

    print("Deduplicating stations...")
    thresholds = load_thresholds()
    known_stations = set(thresholds.keys())
    all_predictions = deduplicate_stations(all_predictions, known_stations)
    station_meta = {s: station_meta[s] for s in all_predictions if s in station_meta}
    
    out = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'days_ahead': days_ahead,
        'start_date': dates[0].strftime('%Y-%m-%d'),
        'end_date': dates[-1].strftime('%Y-%m-%d'),
        'predictions': all_predictions,
        'meta': station_meta
    }

    path = os.path.join(OUTPUT_PATH, 'forecast_data.json')
    with open(path, 'w') as f:
        json.dump(out, f)

    size_mb = os.path.getsize(path) / 1024 / 1024
    print(f"Forecast saved to {path} ({size_mb:.1f} MB)")
    print(f"Generated at: {out['generated_at']}")

def export_lag_lookup_json():
    """
    Export the lag lookup table as a small JSON file.
    This is committed to the repo so GitHub Actions doesn't need to download Parquet data.
    """
    import json
    print("Exporting lag lookup to JSON...")
    df = build_lag_lookup()
    
    # Convert to nested dict: {station: {dow: {hour: avg_ridership}}}
    lookup = {}
    for _, row in df.iterrows():
        s = row['station_complex']
        d = int(row['dow'])
        h = int(row['hour'])
        if s not in lookup:
            lookup[s] = {}
        if d not in lookup[s]:
            lookup[s][d] = {}
        lookup[s][d][h] = round(float(row['avg_ridership']), 2)
    
    path = os.path.join(os.path.dirname(__file__), 'data', 'lag_lookup.json')
    with open(path, 'w') as f:
        json.dump(lookup, f, separators=(',', ':'))
    
    size_mb = os.path.getsize(path) / 1024 / 1024
    print(f"  Saved to {path} ({size_mb:.1f} MB)")
    

if __name__ == "__main__":
    run_forecast(days_ahead=90)