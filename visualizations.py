import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import os
from src.query import get_hourly_ridership
from src.features import add_time_features, add_lag_features, drop_nulls, add_holiday_features
from src.station_features import build_station_features, merge_station_features
from src.model import load_model, load_thresholds, prepare_features, FEATURE_COLS
from src.features import add_congestion_label, compute_congestion_thresholds

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), 'visualizations')
os.makedirs(OUTPUT_PATH, exist_ok=True)

DAY_LABELS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

def load_2025_eval_data():
    """Load 2025+ data with actual congestion labels for comparison visualizations."""
    import gc
    print("Loading 2025 evaluation data...")
    station_df = build_station_features()
    thresholds = load_thresholds()
    model = load_model('global_congestion_model.pkl')

    months = pd.date_range(start='2025-01-01', end='2026-02-01', freq='MS')
    all_dfs = []

    for month_start in months:
        month_end = month_start + pd.offsets.MonthEnd(1)
        month_end_str = (month_end + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        month_start_str = month_start.strftime('%Y-%m-%d')
        print(f"  {month_start_str}...")

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

    full_df = pd.concat(all_dfs, ignore_index=True)
    full_df = full_df.sort_values(['station_complex', 'transit_timestamp']).reset_index(drop=True)
    full_df = add_congestion_label(full_df, horizon=2, precomputed_thresholds=thresholds)

    X, y = prepare_features(full_df)
    full_df['predicted_prob'] = model.predict_proba(X)[:, 1]
    full_df['predicted'] = model.predict(X)
    full_df['actual'] = y.values

    print(f"  Loaded {len(full_df):,} rows across {full_df['station_complex'].nunique()} stations")
    return full_df

def load_2024_data():
    """Load and prepare 2024 validation data for visualizations."""
    print("Loading 2024 data...")
    station_df = build_station_features()
    months = pd.date_range(start='2024-01-01', end='2024-12-01', freq='MS')
    all_dfs = []

    for month_start in months:
        month_end = month_start + pd.offsets.MonthEnd(1)
        month_end_str = (month_end + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        month_start_str = month_start.strftime('%Y-%m-%d')
        print(f"  {month_start_str}...")
        df = get_hourly_ridership(start=month_start_str, end=month_end_str)
        df = add_time_features(df)
        df = add_lag_features(df)
        df = drop_nulls(df)
        df = add_holiday_features(df)
        df = merge_station_features(df, station_df)
        all_dfs.append(df)

    full_df = pd.concat(all_dfs, ignore_index=True)
    full_df = full_df.sort_values(['station_complex', 'transit_timestamp']).reset_index(drop=True)

    thresholds = load_thresholds()
    full_df = add_congestion_label(full_df, horizon=2, precomputed_thresholds=thresholds)
    return full_df


def plot_congestion_heatmap(df: pd.DataFrame):
    """
    Heatmap of congestion rate by hour of day and day of week.
    """
    print("Plotting congestion heatmap...")

    pivot = df.groupby(['day_of_week', 'hour'])['is_congested'].mean().unstack()

    fig, ax = plt.subplots(figsize=(14, 6))
    im = ax.imshow(pivot.values, aspect='auto', cmap='YlOrRd', vmin=0, vmax=0.6)

    ax.set_xticks(range(24))
    ax.set_xticklabels([f'{h:02d}:00' for h in range(24)], rotation=45, ha='right', fontsize=8)
    ax.set_yticks(range(7))
    ax.set_yticklabels(DAY_LABELS, fontsize=10)

    plt.colorbar(im, ax=ax, label='Congestion Rate')
    ax.set_title('NYC Subway Congestion Rate by Hour and Day of Week (2024)', fontsize=14, pad=15)
    ax.set_xlabel('Hour of Day', fontsize=11)
    ax.set_ylabel('Day of Week', fontsize=11)

    plt.tight_layout()
    path = os.path.join(OUTPUT_PATH, '1_congestion_heatmap.png')
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved to {path}")

def plot_drift_chart(df: pd.DataFrame):
    """
    Bar chart showing per-station congestion rate drift from 2023 to 2024.
    Highlights recovering hubs vs stable/declining local stations.
    """
    print("Plotting drift chart...")

    # Load 2023 data for comparison
    print("  Loading 2023 data for drift comparison...")
    station_df = build_station_features()
    months_2023 = pd.date_range(start='2023-01-01', end='2023-12-01', freq='MS')
    all_2023 = []

    for month_start in months_2023:
        month_end = month_start + pd.offsets.MonthEnd(1)
        month_end_str = (month_end + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        month_start_str = month_start.strftime('%Y-%m-%d')
        df_m = get_hourly_ridership(start=month_start_str, end=month_end_str)
        df_m = add_time_features(df_m)
        df_m = add_lag_features(df_m)
        df_m = drop_nulls(df_m)
        df_m = add_holiday_features(df_m)
        df_m = merge_station_features(df_m, station_df)
        all_2023.append(df_m)

    df_2023 = pd.concat(all_2023, ignore_index=True)
    thresholds = load_thresholds()
    df_2023 = add_congestion_label(df_2023, horizon=2, precomputed_thresholds=thresholds)

    # Compute drift
    rate_2023 = df_2023.groupby('station_complex')['is_congested'].mean()
    rate_2024 = df.groupby('station_complex')['is_congested'].mean()
    drift = (rate_2024 - rate_2023).dropna().sort_values()

    # Split into recovering and declining
    recovering = drift[drift > 0.02].sort_values(ascending=False).head(20)
    declining = drift[drift < -0.02].sort_values(ascending=True).head(20)
    plot_drift = pd.concat([recovering, declining]).sort_values()

    colors = ['#d73027' if x > 0 else '#4575b4' for x in plot_drift.values]

    fig, ax = plt.subplots(figsize=(12, 10))
    bars = ax.barh(range(len(plot_drift)), plot_drift.values, color=colors)
    ax.set_yticks(range(len(plot_drift)))
    ax.set_yticklabels(plot_drift.index, fontsize=8)
    ax.axvline(x=0, color='black', linewidth=0.8)
    ax.set_xlabel('Change in Congestion Rate (2023 → 2024)', fontsize=11)
    ax.set_title('Post-Covid Ridership Recovery: Top 20 Gaining and Declining Stations', fontsize=13, pad=15)

    # Legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='#d73027', label='Recovering (higher congestion in 2024)'),
        Patch(facecolor='#4575b4', label='Declining (lower congestion in 2024)')
    ]
    ax.legend(handles=legend_elements, loc='lower right', fontsize=9)

    plt.tight_layout()
    path = os.path.join(OUTPUT_PATH, '2_drift_chart.png')
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved to {path}")
    
def plot_feature_importance():
    """
    Horizontal bar chart of top 20 features by gain.
    """
    print("Plotting feature importance...")

    model = load_model('global_congestion_model.pkl')
    importance = model.get_booster().get_score(importance_type='gain')
    importance_df = pd.DataFrame({
        'feature': list(importance.keys()),
        'importance': list(importance.values())
    }).sort_values('importance', ascending=True).tail(20)

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.barh(range(len(importance_df)), importance_df['importance'].values, color='steelblue')
    ax.set_yticks(range(len(importance_df)))
    ax.set_yticklabels(importance_df['feature'].values, fontsize=9)
    ax.set_xlabel('Feature Importance (Gain)', fontsize=11)
    ax.set_title('Top 20 Features by Importance (Global Model)', fontsize=13, pad=15)

    plt.tight_layout()
    path = os.path.join(OUTPUT_PATH, '3_feature_importance.png')
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved to {path}")
    
def plot_performance_progression():
    """
    Bar chart showing AUC across evaluation stages.
    Tells the story of honest performance at each scale.
    """
    print("Plotting performance progression...")

    stages = [
        'Single Station\n(Grand Central, 2024)',
        '20 Stations\n(2024)',
        '428 Stations\n(2024 validation)',
        '428 Stations\n(2025+ final test)'
    ]
    aucs = [0.9936, 0.9858, 0.9731, 0.9520]
    colors = ['#2166ac', '#4393c3', '#92c5de', '#d6604d']

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(range(len(stages)), aucs, color=colors, width=0.6, edgecolor='white')

    # Add value labels on bars
    for bar, auc in zip(bars, aucs):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.002,
                f'{auc:.4f}', ha='center', va='bottom', fontsize=11, fontweight='bold')

    ax.set_xticks(range(len(stages)))
    ax.set_xticklabels(stages, fontsize=10)
    ax.set_ylim(0.90, 1.0)
    ax.set_ylabel('ROC-AUC Score', fontsize=11)
    ax.set_title('Model Performance Across Evaluation Stages', fontsize=13, pad=15)
    ax.axhline(y=0.95, color='gray', linewidth=0.8, linestyle='--', alpha=0.5)

    # Annotation
    ax.annotate('Trained on 2023 only\nFinal test on unseen 2025+ data',
                xy=(3, 0.9520), xytext=(2.2, 0.965),
                arrowprops=dict(arrowstyle='->', color='black'),
                fontsize=9, color='#d6604d')

    plt.tight_layout()
    path = os.path.join(OUTPUT_PATH, '4_performance_progression.png')
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved to {path}")

def plot_borough_performance(df: pd.DataFrame):
    """
    AUC score broken down by borough.
    Shows model performance varies across the system.
    """
    print("Plotting borough performance...")

    model = load_model('global_congestion_model.pkl')
    X, y = prepare_features(df)
    df_eval = df.copy()
    df_eval['predicted_prob'] = model.predict_proba(X)[:, 1]
    df_eval['actual'] = y.values

    from sklearn.metrics import roc_auc_score

    boroughs = ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island']
    borough_cols = {
        'Manhattan': 'borough_manhattan',
        'Brooklyn': 'borough_brooklyn',
        'Queens': 'borough_queens',
        'Bronx': 'borough_bronx',
        'Staten Island': 'borough_staten_island'
    }

    results = []
    for borough, col in borough_cols.items():
        subset = df_eval[df_eval[col] == 1]
        if len(subset) == 0:
            continue
        auc = roc_auc_score(subset['actual'], subset['predicted_prob'])
        congestion_rate = subset['actual'].mean()
        results.append({
            'borough': borough,
            'auc': auc,
            'congestion_rate': congestion_rate,
            'rows': len(subset)
        })

    results_df = pd.DataFrame(results).sort_values('auc', ascending=True)

    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax2 = ax1.twinx()

    x = range(len(results_df))
    bars = ax1.bar(x, results_df['auc'], color='steelblue', width=0.4, label='AUC')
    line = ax2.plot(x, results_df['congestion_rate'], color='#d73027',
                    marker='o', linewidth=2, markersize=8, label='Congestion Rate')

    for bar, auc in zip(bars, results_df['auc']):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.001,
                f'{auc:.4f}', ha='center', va='bottom', fontsize=10, fontweight='bold')

    ax1.set_xticks(x)
    ax1.set_xticklabels(results_df['borough'], fontsize=11)
    ax1.set_ylim(0.90, 1.0)
    ax1.set_ylabel('ROC-AUC Score', fontsize=11)
    ax2.set_ylabel('Congestion Rate', fontsize=11, color='#d73027')
    ax2.tick_params(axis='y', labelcolor='#d73027')
    ax1.set_title('Model Performance and Congestion Rate by Borough (2024)', fontsize=13, pad=15)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='lower right', fontsize=9)

    plt.tight_layout()
    path = os.path.join(OUTPUT_PATH, '5_borough_performance.png')
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved to {path}")

def export_station_map_data(df: pd.DataFrame):
    """
    Export per-station stats for the interactive map.
    """
    print("Exporting station map data...")

    model = load_model('global_congestion_model.pkl')
    X, y = prepare_features(df)
    df_eval = df.copy()
    df_eval['predicted_prob'] = model.predict_proba(X)[:, 1]
    df_eval['actual'] = y.values

    from sklearn.metrics import roc_auc_score
    from src.query import get_connection

    # Get lat/lon from raw data
    conn = get_connection()
    coords = conn.execute("""
        SELECT 
            station_complex,
            AVG(latitude) as lat,
            AVG(longitude) as lon
        FROM ridership
        WHERE year IN (2023, 2024)
        AND latitude IS NOT NULL
        GROUP BY station_complex
    """).df()

    # Load 2023 data for drift
    station_df = build_station_features()
    months_2023 = pd.date_range(start='2023-01-01', end='2023-12-01', freq='MS')
    all_2023 = []
    for month_start in months_2023:
        month_end = month_start + pd.offsets.MonthEnd(1)
        month_end_str = (month_end + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        month_start_str = month_start.strftime('%Y-%m-%d')
        df_m = get_hourly_ridership(start=month_start_str, end=month_end_str)
        df_m = add_time_features(df_m)
        df_m = add_lag_features(df_m)
        df_m = drop_nulls(df_m)
        df_m = add_holiday_features(df_m)
        df_m = merge_station_features(df_m, station_df)
        all_2023.append(df_m)
    df_2023 = pd.concat(all_2023, ignore_index=True)
    thresholds = load_thresholds()
    df_2023 = add_congestion_label(df_2023, horizon=2, precomputed_thresholds=thresholds)

    rate_2023 = df_2023.groupby('station_complex')['is_congested'].mean()
    rate_2024 = df_eval.groupby('station_complex')['is_congested'].mean()
    drift = (rate_2024 - rate_2023).dropna()

    # Per station AUC
    station_aucs = {}
    for station in df_eval['station_complex'].unique():
        subset = df_eval[df_eval['station_complex'] == station]
        if subset['actual'].nunique() < 2:
            continue
        try:
            auc = roc_auc_score(subset['actual'], subset['predicted_prob'])
            station_aucs[station] = round(auc, 4)
        except:
            pass

    # Build final records
    station_meta = df_eval.groupby('station_complex').agg(
        borough=('borough_manhattan', lambda x: None),  # placeholder
        congestion_rate=('actual', 'mean'),
        num_lines=('num_lines', 'first'),
        station_tier=('station_tier', 'first'),
    ).reset_index()

    # Get borough from station features
    borough_map = {}
    for _, row in station_df.iterrows():
        for b in ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island']:
            col = 'borough_' + b.lower().replace(' ', '_')
            if col in row and row[col] == 1:
                borough_map[row['station_complex']] = b
                break

    records = []
    for _, row in station_meta.iterrows():
        station = row['station_complex']
        coord_row = coords[coords['station_complex'] == station]
        if coord_row.empty:
            continue
        records.append({
            'name': station,
            'lat': round(float(coord_row['lat'].values[0]), 5),
            'lon': round(float(coord_row['lon'].values[0]), 5),
            'congestion_rate': round(float(row['congestion_rate']), 4),
            'drift': round(float(drift.get(station, 0)), 4),
            'auc': station_aucs.get(station, None),
            'num_lines': int(row['num_lines']),
            'station_tier': int(row['station_tier']),
            'borough': borough_map.get(station, 'Unknown')
        })

    import json
    output_path = os.path.join(OUTPUT_PATH, 'station_data.json')
    with open(output_path, 'w') as f:
        json.dump(records, f, indent=2)
    print(f"Exported {len(records)} stations to {output_path}")

def export_animation_data():
    """
    Export hourly ridership data for the animation.
    1. 24-hour average — average ridership by station by hour across all 2025 weekdays
    2. Representative week — Oct 6-12 2025 (no holidays, typical fall week)
    """
    print("Exporting animation data...")
    from src.query import get_connection
    from src.station_features import build_station_features
    import json

    conn = get_connection()

    # 24-hour weekday average across all of 2025
    print("  Computing 24-hour weekday average...")
    hourly_avg = conn.execute("""
        SELECT
            station_complex,
            HOUR(transit_timestamp) as hour,
            AVG(ridership) as avg_ridership,
            AVG(latitude) as lat,
            AVG(longitude) as lon
        FROM ridership
        WHERE year = 2025
        AND DAYOFWEEK(transit_timestamp) BETWEEN 2 AND 6
        AND latitude IS NOT NULL
        GROUP BY station_complex, hour
        ORDER BY station_complex, hour
    """).df()

    print(f"  Hourly avg rows: {len(hourly_avg)}")

    # System-wide max for normalization — keeps color range meaningful
    # Use 95th percentile as ceiling so color range isn't dominated by Times Square
    system_max = float(hourly_avg['avg_ridership'].quantile(0.95))
    print(f"  95th percentile max ridership: {system_max:.1f}")
    
    # Build daily data with system-wide normalization
    daily_data = {}
    for _, row in hourly_avg.iterrows():
        s = row['station_complex']
        if s not in daily_data:
            daily_data[s] = {
                'lat': round(float(row['lat']), 5),
                'lon': round(float(row['lon']), 5),
                'hours': {}
            }
        normalized = round(float(row['avg_ridership']) / system_max, 4) if system_max > 0 else 0
        daily_data[s]['hours'][str(int(row['hour']))] = {
            'r': round(float(row['avg_ridership']), 1),
            'n': normalized
        }

    print(f"  Daily data: {len(daily_data)} stations")

    # Representative week Oct 6-12 2025
    print("  Fetching representative week Oct 6-12 2025...")
    week_raw = conn.execute("""
        SELECT
            station_complex,
            transit_timestamp,
            SUM(ridership) as ridership,
            AVG(latitude) as lat,
            AVG(longitude) as lon
        FROM ridership
        WHERE transit_timestamp >= '2025-10-06'
        AND transit_timestamp < '2025-10-13'
        AND latitude IS NOT NULL
        GROUP BY station_complex, transit_timestamp
        ORDER BY transit_timestamp, station_complex
    """).df()

    print(f"  Week raw rows: {len(week_raw)}")

    # Build week data with system-wide normalization
    week_out = {}
    for _, row in week_raw.iterrows():
        ts = str(row['transit_timestamp'])
        s = row['station_complex']
        normalized = round(min(1.0, float(row['ridership']) / system_max), 4) if system_max > 0 else 0
        if ts not in week_out:
            week_out[ts] = {}
        week_out[ts][s] = {
            'r': round(float(row['ridership']), 1),
            'n': normalized
        }

    print(f"  Week timestamps: {len(week_out)}")

    # Station metadata for filters
    print("  Building station metadata...")
    station_df = build_station_features()

    import re

    def extract_lines(station_name):
        matches = re.findall(r'\(([^)]+)\)', station_name)
        lines = []
        for match in matches:
            lines.extend([l.strip() for l in match.split(',')])
        return sorted(set(lines))

    station_meta = {}
    all_lines = set()
    for _, row in station_df.iterrows():
        s = row['station_complex']
        borough = 'Unknown'
        for b in ['manhattan', 'brooklyn', 'queens', 'bronx', 'staten_island']:
            if row.get('borough_' + b, 0) == 1:
                borough = b.replace('_', ' ').title()
                break
        lines = extract_lines(s)
        all_lines.update(lines)
        station_meta[s] = {
            'borough': borough,
            'num_lines': int(row['num_lines']),
            'station_tier': int(row['station_tier']),
            'lines': lines
        }

    out = {
        'daily': daily_data,
        'week': week_out,
        'meta': station_meta,
        'all_lines': sorted(all_lines)
    }

    path = os.path.join(OUTPUT_PATH, 'animation_data.json')
    with open(path, 'w') as f:
        json.dump(out, f)

    size_mb = os.path.getsize(path) / 1024 / 1024
    print(f"  Saved to {path} ({size_mb:.1f} MB)")
    print(f"  Daily: {len(daily_data)} stations x 24 hours")
    print(f"  Week: {len(week_out)} timestamps x ~{len(week_raw) // max(len(week_out),1)} stations")
    
def plot_predicted_vs_actual_by_hour(df: pd.DataFrame):
    print("Plotting predicted vs actual by hour...")

    # Normalize ridership per station first
    df = df.copy()
    df['ridership_norm'] = df.groupby('station_complex')['ridership'].transform(
        lambda x: (x - x.min()) / (x.max() - x.min() + 1e-6)
    )

    hourly = df.groupby('hour').agg(
        predicted=('predicted_prob', 'mean'),
        actual=('ridership_norm', 'mean')
    ).reset_index()

    # Scale predicted to same range as actual for visual comparison
    pred_min, pred_max = hourly['predicted'].min(), hourly['predicted'].max()
    act_min, act_max = hourly['actual'].min(), hourly['actual'].max()
    hourly['predicted_scaled'] = (hourly['predicted'] - pred_min) / (pred_max - pred_min) * (act_max - act_min) + act_min

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(hourly['hour'], hourly['predicted_scaled'], color='#185FA5',
            linewidth=2.5, marker='o', markersize=5, label='Predicted pattern (scaled)')
    ax.plot(hourly['hour'], hourly['actual'], color='#E24B4A',
            linewidth=2.5, marker='o', markersize=5, label='Actual ridership pattern', linestyle='--')

    ax.set_xticks(range(24))
    ax.set_xticklabels([f'{h}:00' for h in range(24)], rotation=45, ha='right', fontsize=8)
    ax.set_ylabel('Normalized score', fontsize=11)
    ax.set_title('Model correctly identifies AM and PM rush hours — predictions lead actual by ~2 hours\n'
             'Predicted pattern vs actual ridership by hour of day — 2025+',
             fontsize=11, pad=15)
    ax.legend(fontsize=10)
    ax.grid(axis='y', alpha=0.3)
    ax.annotate('2-hour prediction\nhorizon offset',
            xy=(7, 0.21), xytext=(9, 0.32),
            arrowprops=dict(arrowstyle='->', color='gray'),
            fontsize=9, color='gray')

    # Annotate the peaks
    peak_pred = hourly['predicted_scaled'].idxmax()
    peak_act = hourly['actual'].idxmax()
    ax.annotate(f'Predicted peak: {hourly["hour"][peak_pred]}:00',
                xy=(hourly['hour'][peak_pred], hourly['predicted_scaled'][peak_pred]),
                xytext=(hourly['hour'][peak_pred] + 1, hourly['predicted_scaled'][peak_pred] + 0.01),
                fontsize=9, color='#185FA5')
    ax.annotate(f'Actual peak: {hourly["hour"][peak_act]}:00',
                xy=(hourly['hour'][peak_act], hourly['actual'][peak_act]),
                xytext=(hourly['hour'][peak_act] + 1, hourly['actual'][peak_act] - 0.02),
                fontsize=9, color='#E24B4A')

    plt.tight_layout()
    path = os.path.join(OUTPUT_PATH, '6_predicted_vs_actual_hour.png')
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved to {path}")

def plot_predicted_vs_actual_by_borough(df: pd.DataFrame):
    """Bar chart: predicted vs actual AUC by borough."""
    print("Plotting predicted vs actual by borough...")
    from sklearn.metrics import roc_auc_score

    borough_cols = {
        'Manhattan': 'borough_manhattan',
        'Brooklyn': 'borough_brooklyn',
        'Queens': 'borough_queens',
        'Bronx': 'borough_bronx',
        'Staten Island': 'borough_staten_island'
    }

    results = []
    for borough, col in borough_cols.items():
        subset = df[df[col] == 1]
        if len(subset) == 0 or subset['actual'].nunique() < 2:
            continue
        auc = roc_auc_score(subset['actual'], subset['predicted_prob'])
        results.append({'borough': borough, 'auc': auc, 'n': len(subset)})

    results_df = pd.DataFrame(results).sort_values('auc')
    colors = ['#185FA5' if a >= 0.97 else '#EF9F27' if a >= 0.95 else '#E24B4A'
              for a in results_df['auc']]

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(range(len(results_df)), results_df['auc'], color=colors, alpha=0.85)

    ax.set_xticks(range(len(results_df)))
    ax.set_xticklabels(results_df['borough'], fontsize=11)
    ax.set_ylim(0.90, 1.0)
    ax.set_ylabel('ROC-AUC Score', fontsize=11)
    ax.set_title('Model AUC by Borough on 2025+ Data', fontsize=13, pad=15)
    ax.axhline(y=0.95, color='gray', linewidth=0.8, linestyle='--', alpha=0.5)

    for bar, auc in zip(bars, results_df['auc']):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001,
                f'{auc:.4f}', ha='center', va='bottom', fontsize=10, fontweight='bold')

    plt.tight_layout()
    path = os.path.join(OUTPUT_PATH, '7_auc_by_borough.png')
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved to {path}")


def plot_calibration_curve(df: pd.DataFrame):
    """Calibration curve: when model says X%, does it happen X% of the time?"""
    print("Plotting calibration curve...")
    from sklearn.calibration import calibration_curve

    prob_true, prob_pred = calibration_curve(
        df['actual'], df['predicted_prob'], n_bins=10, strategy='uniform'
    )

    fig, ax = plt.subplots(figsize=(7, 7))
    ax.plot([0, 1], [0, 1], 'k--', linewidth=1, alpha=0.5, label='Perfect calibration')
    ax.plot(prob_pred, prob_true, color='#185FA5', linewidth=2,
            marker='o', markersize=8, label='Model calibration')

    ax.fill_between(prob_pred, prob_true, prob_pred,
                    alpha=0.1, color='#185FA5')

    ax.set_xlabel('Mean Predicted Probability', fontsize=11)
    ax.set_ylabel('Fraction of Positives (Actual Rate)', fontsize=11)
    ax.set_title('Calibration Curve — 2025+ Data\n(closer to diagonal = better calibrated)',
                 fontsize=12, pad=15)
    ax.legend(fontsize=10)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.grid(alpha=0.3)

    plt.tight_layout()
    path = os.path.join(OUTPUT_PATH, '8_calibration_curve.png')
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved to {path}")


def plot_performance_over_time(df: pd.DataFrame):
    """Line chart: monthly AUC from Jan 2025 to Feb 2026."""
    print("Plotting performance over time...")
    from sklearn.metrics import roc_auc_score

    df['year_month'] = df['transit_timestamp'].dt.to_period('M')
    months = sorted(df['year_month'].unique())

    results = []
    for month in months:
        subset = df[df['year_month'] == month]
        if len(subset) < 1000 or subset['actual'].nunique() < 2:
            continue
        auc = roc_auc_score(subset['actual'], subset['predicted_prob'])
        results.append({'month': str(month), 'auc': auc, 'n': len(subset)})

    results_df = pd.DataFrame(results)

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(range(len(results_df)), results_df['auc'],
            color='#185FA5', linewidth=2, marker='o', markersize=6)
    ax.fill_between(range(len(results_df)), results_df['auc'],
                    0.90, alpha=0.1, color='#185FA5')

    ax.set_xticks(range(len(results_df)))
    ax.set_xticklabels(results_df['month'], rotation=45, ha='right', fontsize=9)
    ax.set_ylim(0.90, 1.0)
    ax.set_ylabel('ROC-AUC Score', fontsize=11)
    ax.set_title('Model Performance Over Time — Monthly AUC on 2025+ Data\n(trained on 2023 only)',
                 fontsize=12, pad=15)
    ax.axhline(y=results_df['auc'].mean(), color='gray', linewidth=1,
               linestyle='--', alpha=0.7, label=f'Mean AUC: {results_df["auc"].mean():.4f}')
    ax.legend(fontsize=10)
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    path = os.path.join(OUTPUT_PATH, '9_performance_over_time.png')
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved to {path}")
    
if __name__ == "__main__":
    # df_2024 = load_2024_data()
    # plot_congestion_heatmap(df_2024)
    # plot_drift_chart(df_2024)
    # plot_feature_importance()
    # plot_performance_progression()
    # plot_borough_performance(df_2024)
    # export_station_map_data(df_2024)
    # export_animation_data()

    df_2025 = load_2025_eval_data()
    plot_predicted_vs_actual_by_hour(df_2025)
    plot_predicted_vs_actual_by_borough(df_2025)
    plot_calibration_curve(df_2025)
    plot_performance_over_time(df_2025)