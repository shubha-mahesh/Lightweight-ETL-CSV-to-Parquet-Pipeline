"""
validate_schema.py
Simple validation: required columns, null handling, explicit dtypes.
"""
import pandas as pd

REQUIRED_COLS = {
    "user_id": "int64",
    "event_type": "string",
    "timestamp": "string",
    "value": "float64"
}

def validate_and_cast_df(df):
    # Keep only required columns (ignore extras)
    df = df.copy()
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Drop rows where essential fields missing
    initial = len(df)
    df = df.dropna(subset=['user_id','timestamp'])  # user_id and timestamp are essential
    # event_type: fill unknown
    df['event_type'] = df['event_type'].fillna('unknown')

    # Cast dtypes
    df['user_id'] = df['user_id'].astype('int64')
    df['value'] = pd.to_numeric(df['value'], errors='coerce').fillna(0.0)
    # Keep timestamp as string but ensure parseable
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp'])  # drop rows with unparseable timestamp
    df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    dropped = initial - len(df)
    return df, dropped
