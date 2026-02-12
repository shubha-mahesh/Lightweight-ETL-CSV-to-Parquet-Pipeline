"""
generate_synthetic_data.py
Generates a synthetic CSV to test the ETL pipeline.
Usage:
    python generate_synthetic_data.py --rows 100000 --out data/events.csv
"""

import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import os

def random_dates(start, n, freq='s'):
    # returns n datetimes starting from start with random offsets
    base = np.datetime64(start)
    offsets = np.random.randint(0, 60*60*24*30, size=n)  # up to 30 days
    return base + offsets.astype('timedelta64[s]')

def make_events(n_rows=100000, out_path="data/events.csv"):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    np.random.seed(42)
    user_ids = np.random.randint(1, 20000, size=n_rows)
    event_types = np.random.choice(['click','view','purchase','signup'], size=n_rows, p=[0.6,0.3,0.05,0.05])
    timestamps = random_dates(datetime.utcnow().isoformat()+'Z', n_rows)
    value = np.where(event_types=='purchase', np.round(np.random.exponential(50, n_rows),2), 0.0)

    df = pd.DataFrame({
        "user_id": user_ids,
        "event_type": event_types,
        "timestamp": pd.to_datetime(timestamps).astype(str),
        "value": value
    })

    # add some intentional nulls/noise for validation checks
    mask = np.random.rand(n_rows) < 0.002
    df.loc[mask, "event_type"] = None

    df.to_csv(out_path, index=False)
    print(f"Wrote {n_rows} rows to {out_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--out", type=str, default="data/events.csv")
    args = parser.parse_args()
    make_events(args.rows, args.out)

