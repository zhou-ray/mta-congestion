import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    roc_auc_score
)
import pickle
import os
from src.config import PROJECT_ROOT


# Features the model will train on
FEATURE_COLS = [
    # Time features
    'hour', 'day_of_week', 'month', 'week_of_year',
    'is_weekend', 'is_shoulder_day',
    'day_mon', 'day_tue', 'day_wed', 'day_thu', 'day_fri', 'day_sat', 'day_sun',
    'hour_sin', 'hour_cos', 'month_sin', 'month_cos',
    # Lag features
    'lag_1', 'lag_24', 'lag_168',
    'roll_mean_24', 'roll_mean_168', 'roll_std_24',
    # Holiday features
    'is_holiday', 'is_holiday_eve', 'is_holiday_next',
    'is_nyc_marathon', 'is_thanksgiving_eve', 'is_pre_thanksgiving_saturday',
    # Station features
    'num_lines', 'is_terminal', 'avg_ridership', 'std_ridership',
    'max_ridership', 'station_tier',
    'borough_manhattan', 'borough_brooklyn', 'borough_queens',
    'borough_bronx', 'borough_staten_island'
]

TARGET_COL = 'is_congested'
MODELS_PATH = os.path.join(PROJECT_ROOT, 'data', 'models')


def prepare_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Split DataFrame into feature matrix X and target y."""
    X = df[FEATURE_COLS]
    y = df[TARGET_COL]
    return X, y


def train(X_train: pd.DataFrame, y_train: pd.Series) -> xgb.XGBClassifier:
    """Train an XGBoost classifier."""
    model = xgb.XGBClassifier(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=(y_train == 0).sum() / (y_train == 1).sum(),
        random_state=42,
        eval_metric='logloss',
        early_stopping_rounds=20,
    )
    return model


def evaluate(model: xgb.XGBClassifier, X_test: pd.DataFrame, y_test: pd.Series) -> dict:
    """Evaluate model and return metrics."""
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    print("\n=== Classification Report ===")
    print(classification_report(y_test, y_pred))

    print("=== Confusion Matrix ===")
    print(confusion_matrix(y_test, y_pred))

    auc = roc_auc_score(y_test, y_prob)
    print(f"\nROC-AUC Score: {auc:.4f}")

    return {
        'auc': auc,
        'classification_report': classification_report(y_test, y_pred, output_dict=True)
    }


def save_model(model, filename: str) -> None:
    """Save XGBoost model to disk."""
    os.makedirs(MODELS_PATH, exist_ok=True)
    path = os.path.join(MODELS_PATH, filename)
    with open(path, 'wb') as f:
        pickle.dump(model, f)
    print(f"Model saved to {path}")
    
def save_thresholds(thresholds: dict, filename: str = 'congestion_thresholds.pkl') -> None:
    """Save per-station congestion thresholds to disk."""
    os.makedirs(MODELS_PATH, exist_ok=True)
    path = os.path.join(MODELS_PATH, filename)
    with open(path, 'wb') as f:
        pickle.dump(thresholds, f)
    print(f"Thresholds saved to {path} ({len(thresholds)} stations)")

def load_model(filename: str):
    """Load XGBoost model from disk."""
    path = os.path.join(MODELS_PATH, filename)
    with open(path, 'rb') as f:
        return pickle.load(f)

def load_thresholds(filename: str = 'congestion_thresholds.pkl') -> dict:
    """Load per-station congestion thresholds from disk."""
    path = os.path.join(MODELS_PATH, filename)
    with open(path, 'rb') as f:
        return pickle.load(f)