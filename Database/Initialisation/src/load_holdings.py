"""
Bulk loader: 309k CSV files -> PostgreSQL via COPY, with parallel workers,
a resumable tracking file, and graceful PAUSE.

PAUSE:
  * Press Ctrl+C once in the terminal running this script, OR
  * create the pause file from any other terminal:  touch <PAUSE_FILE>

  Workers finish the batch they are currently processing, then stop.
  Nothing new is started. Progress is saved to the tracking file.

RESUME:
  Just run the script again. Files with status "pending" are picked up.
"""
import io
import os
import signal
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import psycopg2
from tqdm import tqdm

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "config"))
from local_config import DSN   # machine-specific


# Constants

COLS = ['cusip8', 'nameOfIssuer', 'titleOfClass', 'value', 'shrsOrPrnAmt',
        'investmentDiscretion', 'votingAuthority', 'putCall', 'cusip9', 'shares',
        'permno', 'prc', 'shrout', 'split', 'deviation', 'rdate', 'fdate', 'cik',
        'form', 'type', 'address']
FLT = ['value', 'shares', 'prc', 'shrout', 'deviation']
INT = ['permno', 'rdate', 'fdate', 'cik']
REQUIRED = ['value', 'shares']          # cusip9 or cusip8 is checked later

TABLE = "input_1.holdings_input_table"

BASE_DIR = str(ROOT / "data")                    # file list, tracking file and PAUSE file
ADDRESSES_FILE = f"{BASE_DIR}/file_adresses.csv"
TRACKING_FILE = f"{BASE_DIR}/tracking.csv"
PAUSE_FILE = f"{BASE_DIR}/PAUSE"
ADDR_COL = "address"

WORKERS = 8
FILES_PER_COPY = 20
SAVE_EVERY = 24                          # save tracking file every N finished batches


def load_csv(path):
    df = pd.read_csv(path, dtype=str)
    df = df.rename(columns={'issuerdescription': 'titleOfClass', 'issuername': 'nameOfIssuer'})
    df = df.reindex(columns=COLS)

    for c in FLT:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    for c in INT:
        df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')
    
    df = df.dropna(subset=REQUIRED)
    df = df[df['cusip9'].notna() | df['cusip8'].notna()]   # cusip check
    if df.empty:
        raise ValueError("no valid rows")
    return df


def copy_df(cur, df):
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep='')
    buf.seek(0)
    cols = ','.join(f'"{c}"' for c in COLS)
    cur.copy_expert(f"COPY {TABLE} ({cols}) FROM STDIN WITH (FORMAT csv)", buf)


def init_worker():
    # Ctrl+C results in graceful pause.
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def process_batch(batch):
    """batch = list of (idx, path).
    Returns None if paused before starting, else a list of (idx, status, error)."""
    if os.path.exists(PAUSE_FILE):
        return None

    results, frames, ok_idx = [], [], []
    for idx, path in batch:
        try:
            frames.append(load_csv(path))
            ok_idx.append((idx, path))
        except Exception as e:
            results.append((idx, "error", str(e)))

    conn = psycopg2.connect(DSN)
    try:
        cur = conn.cursor()
        try:
            if frames:
                copy_df(cur, pd.concat(frames, ignore_index=True))
                conn.commit()
            results += [(i, "done", None) for i, _ in ok_idx]
        except Exception:
            conn.rollback()
            # whole batch failed: retry file by file to isolate the bad one
            for (idx, _), df in zip(ok_idx, frames):
                try:
                    copy_df(cur, df)
                    conn.commit()
                    results.append((idx, "done", None))
                except Exception as e:
                    conn.rollback()
                    results.append((idx, "error", str(e)))
    finally:
        conn.close()
    return results


def init_tracking():
    if not os.path.exists(TRACKING_FILE):
        df = pd.read_csv(ADDRESSES_FILE)
        df["status"], df["error"] = "pending", None
        df.to_csv(TRACKING_FILE, index=False)
        print(f"Tracking file created with {len(df):,} entries.")
    else:
        print("Tracking file found, resuming...")


def save_tracker(tracker):
    tmp = TRACKING_FILE + ".tmp"
    tracker.to_csv(tmp, index=False)
    os.replace(tmp, TRACKING_FILE)       


_sigint_count = 0


def on_sigint(signum, frame):
    global _sigint_count
    _sigint_count += 1
    if _sigint_count == 1:
        Path(PAUSE_FILE).touch()
        tqdm.write("\n[PAUSE] Requested. Workers will stop after their current batch. "
                   "Please wait...")
    else:
        tqdm.write("[PAUSE] Already pausing, waiting for running batches to finish "
                   "(killing now could cause duplicate rows).")


if __name__ == "__main__":
    # a leftover pause file from a previous run would stop everything immediately
    if os.path.exists(PAUSE_FILE):
        os.remove(PAUSE_FILE)
        print("Removed stale pause file.")

    init_tracking()
    tracker = pd.read_csv(TRACKING_FILE)
    tracker["error"] = tracker["error"].astype(object)

    pending = tracker[tracker["status"] == "pending"]
    items = [(i, Path(p)) for i, p in pending[ADDR_COL].items()]
    batches = [items[i:i + FILES_PER_COPY] for i in range(0, len(items), FILES_PER_COPY)]
    print(f"{len(items):,} files pending out of {len(tracker):,} total "
          f"({len(batches):,} batches)")

    signal.signal(signal.SIGINT, on_sigint)
    skipped = 0

    try:
        with ProcessPoolExecutor(WORKERS, initializer=init_worker) as ex:
            futures = [ex.submit(process_batch, b) for b in batches]
            for n, fut in enumerate(tqdm(as_completed(futures), total=len(futures))):
                res = fut.result()
                if res is None:          # skipped because of pause
                    skipped += 1
                    continue
                for idx, status, err in res:
                    tracker.at[idx, "status"] = status
                    tracker.at[idx, "error"] = err
                if n % SAVE_EVERY == 0:
                    save_tracker(tracker)
    finally:
        save_tracker(tracker)
        if os.path.exists(PAUSE_FILE):
            os.remove(PAUSE_FILE)

    if skipped:
        print(f"\nPaused. {skipped:,} batches were not started. Run the script again to resume.")
    else:
        print("\nRun complete.")
    print(tracker["status"].value_counts())
