"""
etl_pipeline.py
Run: python etl_pipeline.py --input data/events.csv --out-dir output/parquet --chunk-size 20000
"""
import argparse
import os
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
from validate_schema import validate_and_cast_df

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def write_partitioned_parquet(df, out_dir, partition_cols):
    """Write DataFrame to parquet partitioned by partition_cols using pyarrow."""
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(table, root_path=out_dir, partition_cols=partition_cols)

def process_csv_stream(input_csv, out_dir, chunk_size=20000, timestamp_col='timestamp'):
    ensure_dir(out_dir)
    total_rows = 0
    start_time = time.time()
    stats = {"rows":0, "rows_kept":0, "rows_dropped":0, "files_written":0}
    for chunk in tqdm(pd.read_csv(input_csv, chunksize=chunk_size)):
        stats["rows"] += len(chunk)
        # validation & casting (returns cleaned df and dropped count)
        cleaned_df, dropped = validate_and_cast_df(chunk)
        stats["rows_kept"] += len(cleaned_df)
        stats["rows_dropped"] += dropped

        # derive partitions (e.g., date)
        cleaned_df['date'] = pd.to_datetime(cleaned_df[timestamp_col]).dt.date.astype(str)
        # write per-chunk partitions (append style)
        write_partitioned_parquet(cleaned_df, out_dir, partition_cols=['date','event_type'])
        stats["files_written"] += 1

    elapsed = time.time() - start_time
    stats["elapsed_seconds"] = round(elapsed,2)
    return stats

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="input CSV file")
    parser.add_argument("--out-dir", required=True, help="output parquet root directory")
    parser.add_argument("--chunk-size", type=int, default=20000)
    args = parser.parse_args()

    stats = process_csv_stream(args.input, args.out_dir, chunk_size=args.chunk_size)
    print("Pipeline complete. Stats:")
    for k,v in stats.items():
        print(f"  {k}: {v}")
