
#!/usr/bin/env python3
"""
ingest_clean_store.py

Minimal, production-ready ingestion for Twitter-like JSON/CSV dumps.

Goals
-----
1) Clean & normalize fields (unicode, timestamps, mentions/hashtags).
2) Deduplicate robustly.
3) Write compact columnar storage (Parquet) for downstream analysis.

Why Parquet?
------------
Parquet is a binary columnar format: fast scans, good compression, and typed
schemas—ideal for analytical workloads and ML pipelines.

Inputs
------
- One or more .json / .csv files. JSON can be a single JSON array, a dict,
  or JSON Lines (one object per line). CSV should have the expected columns.
- Example keys: username, timestamp, content, replies, retweets, likes,
  mentions, hashtags, urls, scraped_at.

Outputs
-------
- A single Parquet file (combined) with normalized schema.
- A small run metadata JSON file describing what was processed.

Notes
-----
- Optimized for simplicity and correctness; threading used for I/O-bound reads.
- For big data, prefer partitioned datasets (date/hashtag). This minimal
  version produces one file but keeps memory bounded by per-file batching.
"""

from __future__ import annotations
import argparse
import concurrent.futures as cf
import hashlib
import json
import logging
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Any

import numpy as np
import pandas as pd

import pyarrow as pa
import pyarrow.parquet as pq


# ---------------------------- Logging ---------------------------------

def setup_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


logger = logging.getLogger("ingest")


# ---------------------------- Utilities ---------------------------------

CONTROL_CHARS_RE = re.compile(r"[\u0000-\u001f\u007f-\u009f]")
WS_RE = re.compile(r"\s+")
HASHTAG_RE = re.compile(r"(?u)#\w+")
MENTION_RE = re.compile(r"(?u)@\w+")

EXPECTED_COLUMNS = [
    "username", "timestamp", "content",
    "replies", "retweets", "likes",
    "mentions", "hashtags", "urls",
    "scraped_at"
]

def normalize_text(s: Any) -> str:
    """Unicode-normalize (NFC), strip control chars and collapse whitespace."""
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return ""
    s = str(s)
    s = unicodedata.normalize("NFC", s)
    s = CONTROL_CHARS_RE.sub(" ", s)
    s = WS_RE.sub(" ", s).strip()
    return s

def parse_listish(x: Any) -> List[str]:
    """Parse hashtags/mentions that may come as 'a, b, c' or ['a','b'] or ''."""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return []
    if isinstance(x, (list, tuple, set)):
        return [normalize_text(i).lower() for i in x if str(i).strip()]
    s = normalize_text(x)
    if not s:
        return []
    # split on commas or spaces while preserving #/@ if present
    parts = [p.strip() for p in re.split(r"[,\s]+", s) if p.strip()]
    return [p.lower() for p in parts]

def extract_hashtags(text: str, existing: List[str]) -> List[str]:
    found = [h.lower() for h in HASHTAG_RE.findall(text or "")]
    return sorted(set(existing + found))

def extract_mentions(text: str, existing: List[str]) -> List[str]:
    found = [m.lower() for m in MENTION_RE.findall(text or "")]
    return sorted(set(existing + found))

def coerce_int(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0

def to_utc(ts: Any) -> pd.Timestamp:
    """Best-effort parse to UTC tz-aware timestamp."""
    try:
        t = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.isna(t):
            return pd.NaT
        return t
    except Exception:
        return pd.NaT

def stable_uid(username: str, timestamp: pd.Timestamp, content: str) -> str:
    raw = f"{username}|{timestamp}|{content}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

def standardize_df(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    # Normalize columns
    for c in df.columns:
        if c not in EXPECTED_COLUMNS:
            # keep extra fields but ensure they don't collide
            pass

    # Create missing expected columns with defaults
    for c in EXPECTED_COLUMNS:
        if c not in df.columns:
            df[c] = None

    # Normalize text fields
    df["username"] = df["username"].map(normalize_text)
    df["content"] = df["content"].map(normalize_text)
    df["urls"] = df["urls"].map(normalize_text)

    # Numeric engagements
    df["replies"] = df["replies"].map(coerce_int)
    df["retweets"] = df["retweets"].map(coerce_int)
    df["likes"] = df["likes"].map(coerce_int)

    # Mentions/hashtags
    m_list = df["mentions"].map(parse_listish)
    h_list = df["hashtags"].map(parse_listish)
    df = df.assign(_mentions_list=m_list, _hashtags_list=h_list)

    # Extract from content too
    df["_mentions_list"] = [
        extract_mentions(txt, ms) for txt, ms in zip(df["content"], df["_mentions_list"])
    ]
    df["_hashtags_list"] = [
        extract_hashtags(txt, hs) for txt, hs in zip(df["content"], df["_hashtags_list"])
    ]

    # Timestamps
    df["timestamp"] = df["timestamp"].map(to_utc)
    df["scraped_at"] = df["scraped_at"].map(to_utc)

    # Core identifiers
    df["uid"] = [
        stable_uid(u, t, c) for u, t, c in zip(df["username"], df["timestamp"], df["content"])
    ]

    # Derivations
    df["hashtag_primary"] = [hs[0] if hs else "" for hs in df["_hashtags_list"]]
    df["date"] = df["timestamp"].dt.date.astype("string")

    # Reorder columns
    core = [
        "uid", "username", "timestamp", "content",
        "replies", "retweets", "likes",
        "_mentions_list", "_hashtags_list", "urls",
        "scraped_at", "hashtag_primary", "date"
    ]
    others = [c for c in df.columns if c not in core]
    df = df[core + others]
    df["source_file"] = source_file

    # Drop exact duplicates by uid
    before = len(df)
    df = df.drop_duplicates(subset=["uid"])
    deduped = before - len(df)
    if deduped:
        logger.info("Dropped %d duplicate rows in %s", deduped, source_file)
    return df


# ---------------------------- Loaders ---------------------------------

def load_json_file(path: Path) -> pd.DataFrame:
    # Try different JSON shapes: array, object, or JSON Lines
    try:
        obj = json.loads(Path(path).read_text(encoding="utf-8"))
        if isinstance(obj, dict):
            obj = [obj]
        return pd.DataFrame(obj)
    except Exception:
        # Try JSON lines with pandas (chunked for memory efficiency)
        try:
            reader = pd.read_json(path, lines=True, chunksize=5000, encoding="utf-8")
            chunks = [chunk for chunk in reader]
            if not chunks:
                return pd.DataFrame()
            return pd.concat(chunks, ignore_index=True)
        except Exception as e:
            logger.exception("Failed to parse JSON: %s", path)
            raise

def load_csv_file(path: Path) -> pd.DataFrame:
    # robust CSV load
    try:
        return pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="utf-8", engine="python", errors="ignore")


def load_and_standardize(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext == ".json":
        df = load_json_file(path)
    elif ext == ".csv":
        df = load_csv_file(path)
    else:
        logger.warning("Skipping unsupported file type: %s", path)
        return pd.DataFrame()
    if df.empty:
        return df
    return standardize_df(df, source_file=str(path.name))


# ---------------------------- Parquet Writer ---------------------------

def write_parquet_stream(dfs: Iterable[pd.DataFrame], out_path: Path) -> int:
    """
    Stream-write concatenated DataFrames to a single Parquet file.

    Notes:
    - ParquetWriter allows writing multiple row groups to the SAME file within
      a run. We do NOT append to an already closed file.
    """
    count = 0
    writer = None
    try:
        for df in dfs:
            if df is None or df.empty:
                continue
            table = pa.Table.from_pandas(df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(out_path, table.schema, compression="snappy")
            writer.write_table(table)
            count += len(df)
        return count
    finally:
        if writer is not None:
            writer.close()


# ---------------------------- CLI / Main -------------------------------

def discover_files(input_dir: Path) -> List[Path]:
    files = []
    for ext in ("*.json", "*.csv"):
        files.extend(input_dir.glob(ext))
    return sorted(files)

def main():
    ap = argparse.ArgumentParser(description="Ingest Twitter-like dumps -> clean Parquet")
    ap.add_argument("--input_dir", type=Path, required=True, help="Directory with JSON/CSV files")
    ap.add_argument("--out_dir", type=Path, required=True, help="Output directory")
    ap.add_argument("--workers", type=int, default=4, help="Thread workers for file I/O")
    ap.add_argument("--outfile", type=str, default="tweets_combined.parquet", help="Parquet filename")
    ap.add_argument("-v", "--verbose", action="count", default=0)
    args = ap.parse_args()

    setup_logging(args.verbose)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.out_dir / args.outfile

    files = discover_files(args.input_dir)
    if not files:
        logger.error("No input files found under %s", args.input_dir)
        return 2

    logger.info("Found %d files", len(files))

    # Concurrent reading + standardization (I/O bound -> threads are fine)
    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        dfs_iter = ex.map(load_and_standardize, files)
        total_rows = write_parquet_stream(dfs_iter, out_path)

    # Write a small run metadata
    meta = {
        "processed_files": [f.name for f in files],
        "rows_written": total_rows,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "parquet_path": str(out_path),
        "note": "Single-file parquet; for large-scale use partitioned datasets."
    }
    (args.out_dir / "ingest_metadata.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    logger.info("Wrote %d rows to %s", total_rows, out_path)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
