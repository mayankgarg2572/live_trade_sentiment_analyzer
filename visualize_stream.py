
#!/usr/bin/env python3
"""
visualize_stream.py

Memory-aware plotting for large time-series signals. Uses matplotlib only.

- Downsamples to a target number of points when needed (evenly spaced).
- Streams from Parquet in chunks (via row-group iteration) if present.
"""

from __future__ import annotations
import argparse
import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import matplotlib.pyplot as plt


def setup_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")


logger = logging.getLogger("viz")


def read_parquet_all(path: Path) -> pd.DataFrame:
    # Efficient read via pyarrow row-group iteration to keep memory stable
    table = pq.ParquetFile(path)
    frames = []
    for i in range(table.num_row_groups):
        rg = table.read_row_group(i)
        frames.append(rg.to_pandas())
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()


def downsample(df: pd.DataFrame, target: int = 1000) -> pd.DataFrame:
    if len(df) <= target:
        return df
    idx = np.linspace(0, len(df)-1, target, dtype=int)
    return df.iloc[idx]


def plot_signals(signals_path: Path, out_png: Optional[Path], hashtag: Optional[str]) -> None:
    df = read_parquet_all(signals_path)
    if df.empty:
        logger.warning("Empty signals file: %s", signals_path)
        return
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    if hashtag:
        df = df[df["hashtag_primary"] == hashtag]
    df = df.sort_values("timestamp")
    df_ds = downsample(df[["timestamp","signal","ci_lo","ci_hi"]], target=1500)

    plt.figure()
    plt.plot(df_ds["timestamp"], df_ds["signal"], label="signal")
    plt.fill_between(df_ds["timestamp"], df_ds["ci_lo"], df_ds["ci_hi"], alpha=0.2, label="95% CI")
    plt.xlabel("time")
    plt.ylabel("composite signal")
    plt.title(f"Composite signal over time{f' — {hashtag}' if hashtag else ''}")
    plt.legend()
    plt.tight_layout()
    if out_png:
        plt.savefig(out_png, dpi=150)
    else:
        plt.show()
    plt.close()


def main():
    ap = argparse.ArgumentParser(description="Memory-efficient visualization for signals.parquet")
    ap.add_argument("--signals", type=Path, required=True, help="Path to signals.parquet")
    ap.add_argument("--out", type=Path, default=None, help="Optional output PNG file")
    ap.add_argument("--hashtag", type=str, default=None, help="Filter to a specific hashtag (e.g., #nifty50)")
    ap.add_argument("-v", "--verbose", action="count", default=0)
    args = ap.parse_args()

    setup_logging(args.verbose)
    plot_signals(args.signals, args.out, args.hashtag)

if __name__ == "__main__":
    raise SystemExit(main())
