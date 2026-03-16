# MTA Subway Congestion Forecasting

A machine learning pipeline that predicts NYC subway station congestion 2 hours in advance across all 428 stations in the system. Trained on 2023 post-Covid ridership data, validated on 2024, and evaluated on genuinely unseen 2025-2026 data.

**Final result: AUC 0.9520 on 3.2M rows of unseen future data across 428 stations.**

---

## Demo

![Congestion Heatmap](visualizations/1_congestion_heatmap.png)

![Post-Covid Drift](visualizations/2_drift_chart.png)

![Feature Importance](visualizations/3_feature_importance.png)

![Performance Progression](visualizations/4_performance_progression.png)

![Borough Performance](visualizations/5_borough_performance.png)

Interactive map and animation available in `visualizations/map.html` and `visualizations/animation.html`.

---

## Problem

NYC subway congestion is difficult to anticipate. Commuters have no reliable way to know whether a station will be overcrowded 2 hours from now. This project builds a binary classifier that predicts whether a given station will exceed its historical 80th percentile ridership threshold 2 hours ahead, using only information available at prediction time.

---

## Data

- **Source:** MTA Subway Hourly Ridership via NYC Open Data (SODA API)
- **Historical dataset:** 2020-2024, ~120M rows
- **Current dataset:** 2025+, updated incrementally
- **Coverage:** 428 station complexes across all 5 NYC boroughs
- **Granularity:** Hourly ridership per station per payment method

---

## Architecture

```
SODA API
   ↓
Incremental Ingestion (watermark-based)
   ↓
Partitioned Parquet Storage (year/month)
   ↓
DuckDB Query Layer
   ↓
Feature Engineering
   ↓
XGBoost Global Model
   ↓
Evaluation
```

**Storage:** Hive-partitioned Parquet files enable DuckDB partition pruning — queries only scan relevant files rather than the full dataset. The watermark system reduces incremental update data transfer by ~70% by only fetching records newer than the last ingested timestamp.

**Processing:** Month-by-month pipeline bounds memory usage for the full 120M row dataset on standard hardware.

---

## Features

**Time features (17)**
Hour, day of week, month, week of year, weekend flag, shoulder day flag (Mon/Fri), one-hot day encoding, cyclical sin/cos encoding for hour and month. Cyclical encoding ensures the model understands hour 23 and hour 0 are adjacent rather than 23 units apart.

**Lag features (6)**
Per-station lag at 1 hour, 24 hours (same hour yesterday), and 168 hours (same hour last week). Rolling 24h and 168h mean and standard deviation. All computed with groupby(station) to prevent cross-station data leakage. Rolling windows use shift(1) to ensure only past values are included.

**Holiday features (6)**
Federal holidays, holiday eve, day after holiday, NYC Marathon (first Sunday of November), Thanksgiving Eve, pre-Thanksgiving Saturday. NYC-specific events added beyond the federal calendar to capture local transit patterns.

**Station features (11)**
Number of lines serving the station (extracted from station name), terminal flag, average ridership, standard deviation, max ridership, station tier (quartile-based), borough one-hot encoding. These give the global model a fingerprint of each station's role in the network without one-hot encoding 428 station names.

---

## Target Variable

Binary classification: `is_congested = 1` if ridership 2 hours ahead exceeds the station's 80th percentile threshold.

- **Per-station threshold** normalizes for scale differences across stations
- **Threshold computed on training data only** to prevent label leakage
- **Horizon = 2 hours** — tested horizon=1 (marginal improvement) but chose horizon=2 for practical usefulness

---

## Model

**Algorithm:** XGBoost binary classifier

**Why XGBoost:** Gradient boosted trees consistently outperform linear models on tabular data, handle mixed feature types natively, provide feature importance, and are industry standard for this problem type. LSTMs would add complexity without meaningful performance gain for structured tabular data.

**Configuration:**
- n_estimators: 500
- learning_rate: 0.05
- max_depth: 6
- subsample: 0.8
- colsample_bytree: 0.8
- scale_pos_weight: auto (handles 80/20 class imbalance)
- early_stopping_rounds: 20

**Hyperparameter tuning:** Reducing learning rate to 0.02 with 1000 trees underperformed the baseline (AUC 0.9710 vs 0.9731). The performance ceiling is driven by genuine behavioral drift between training and test years rather than insufficient model capacity. Final model uses n_estimators=500, learning_rate=0.05.

---

## Training Strategy

**Train: 2023 only**
**Validate: 2024**
**Final test: 2025+ (genuinely unseen)**

The full 2020-2024 dataset was not used for training. Drift analysis revealed a structural break between Covid-era (2020-2022) and post-Covid (2023+) ridership patterns. Major hub stations show 18-24% higher congestion rates in 2024 versus 2020-2023 averages. Training on Covid-era data would introduce noise irrelevant to forecasting current and future behavior. 2023 represents the stabilized post-Covid new normal.

---

## Results

| Evaluation | AUC | Precision | Recall | Rows |
|---|---|---|---|---|
| Single station, 2024 (Grand Central) | 0.9936 | 0.97 | 0.94 | 1,575 |
| 20 stations, 2024 | 0.9858 | 0.84 | 0.92 | 134,897 |
| 428 stations, 2024 (validation) | 0.9731 | 0.71 | 0.92 | 2,819,747 |
| 428 stations, 2025+ (final test) | 0.9520 | 0.70 | 0.88 | 3,257,138 |

The gradual performance degradation as scale increases and time horizon extends is expected and honest. A model that held perfectly flat would be suspicious.

**Post-Covid drift:** Congestion rate increased from 20.1% in training (2023) to 21.0% in validation (2024) to 22.7% in final test (2025+), consistent with continued post-Covid ridership recovery. The model was trained on a lower-congestion baseline and evaluated on a higher-congestion environment — performance held well despite this structural shift.

**Borough breakdown:** AUC ranges from Queens (lowest) to Manhattan (highest). Queens performance is dragged down by high-variance stations like Howard Beach-JFK Airport (+21% drift) whose ridership is tied to air travel recovery patterns. Manhattan stations follow the most regular and predictable commute cycles.

---

## Failure Analysis

All high-confidence false negatives have identifiable external causes:

- **Holiday events** — Federal holidays and NYC-specific events (NYC Marathon, Thanksgiving Eve) break regular weekly patterns. Holiday features were added after identifying these failures, improving precision from 0.93 to 0.96 on the single-station model.
- **Unknown events** — Concerts, large gatherings, and other one-off events near stations cannot be predicted without external event data such as an events calendar API. Documented as a known limitation rather than overengineered around.
- **Horizon sensitivity** — False negatives cluster around midday hours preceding sharp afternoon peaks. At horizon=2, current ridership at noon provides insufficient signal that a spike will occur at 2pm. This is a fundamental tradeoff, not a fixable bug.

Model failures are systematic and explainable rather than random.

---

## Setup

### System dependencies

```bash
# Mac only
brew install libomp
```

### Python environment

```bash
git clone https://github.com/zhou-ray/mta-congestion.git
cd mta-congestion
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Environment variables

Copy `.env.example` to `.env` and add your Socrata API token:

```bash
cp .env.example .env
```

Get a free token at [data.ny.gov](https://data.ny.gov).

---

## Running the Pipeline

### Backfill historical data (2023-2024)

```bash
python -m backfill
```

### Ingest 2025+ data

```bash
python -m ingest_2025
```

Run periodically to keep the dataset current. Uses watermark-based incremental updates.

### Train the model

```bash
python -m train
```

### Evaluate on 2025+ data

```bash
python -m evaluate_2025
```

### Generate visualizations

```bash
python -m visualizations
python generate_map.py
python generate_animation.py
```

---

## Data Notes

**Line filter note:** The line filter in the interactive map and animation shows stations served by a selected line, but ridership figures represent total station entries — not ridership attributed to that specific line. For example, selecting the Q train at Times Square shows total Times Square ridership, not Q-train-specific boardings. The MTA's hourly ridership dataset captures station entries rather than individual train boardings, so per-line ridership breakdown is not possible with this data source.

---

## Known Limitations

- **Rare events** — Events occurring 1-2 times per year provide insufficient training signal regardless of whether a flag exists. External event data would address this.
- **Ridership trend drift** — The model is trained on 2023 baselines. As ridership continues to grow, more hours will exceed the 2023 threshold. Periodic retraining on recent data is recommended.
- **No real-time inference** — The current pipeline is batch-oriented. Productionizing for real-time prediction would require a serving layer and streaming feature computation.
- **Station entries not train boardings** — The dataset captures turnstile entries per station, not per train. Multi-line stations aggregate ridership across all lines.

---

## Project Structure

```
mta-congestion/
├── backfill.py              # Historical data ingestion (2023-2024)
├── ingest_2025.py           # Incremental 2025+ ingestion
├── train.py                 # Model training pipeline
├── evaluate_2025.py         # Final evaluation on unseen data
├── generate_map.py          # Interactive station map
├── generate_animation.py    # Congestion animation
├── visualizations.py        # Static charts and data exports
├── src/
│   ├── config.py            # Environment and path configuration
│   ├── fetcher.py           # SODA API pagination and cleaning
│   ├── writer.py            # Parquet writing and watermark management
│   ├── query.py             # DuckDB query layer
│   ├── features.py          # Feature engineering
│   ├── station_features.py  # Static station-level features
│   └── model.py             # XGBoost training and evaluation
├── notebooks/
│   └── exploration.ipynb
├── data/
│   ├── raw/                 # Partitioned Parquet files (gitignored)
│   ├── cache/               # Watermark (gitignored)
│   └── models/              # Saved model and thresholds (gitignored)
├── requirements.txt
└── .env.example
```

---

## Decision Log

A full record of architectural and modeling decisions with reasoning is maintained throughout the project. Key decisions include the post-Covid training cutoff, per-station threshold computation, horizon selection, global vs per-station model architecture, and hyperparameter tuning findings.

---

*Data source: MTA Subway Hourly Ridership, NYC Open Data. Model trained on 2023 data. Evaluated through February 2026.*
