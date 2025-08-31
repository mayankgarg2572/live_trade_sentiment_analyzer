
#!/usr/bin/env python3
"""
features_signals.py

Feature extraction and signal construction on top of the cleaned Parquet file.

Outputs
-------
- features.parquet : per-tweet engineered features
- signals.parquet  : aggregated composite signals by time window + hashtag

Design
------
- Keep memory footprint small: select necessary columns; sparse TF-IDF;
  dimensionality reduction via TruncatedSVD (linear, works with sparse).
- No external APIs. All open-source libraries only.
"""

from __future__ import annotations
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.preprocessing import StandardScaler
import pyarrow as pa
import pyarrow.parquet as pq


BULL_WORDS = {
    "buy","bull","long","breakout","rally","support","up","green","accumulate","entry"
}
BEAR_WORDS = {
    "sell","bear","short","breakdown","resistance","down","red","exit","dump"
}
URGENCY_WORDS = {
    "now","today","alert","immediate","intraday","urgent","live"
}


def setup_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")


logger = logging.getLogger("features")


def load_minimal(parquet_path: Path) -> pd.DataFrame:
    cols = ["uid","timestamp","content","likes","retweets","replies","_hashtags_list","hashtag_primary","username"]
    df = pd.read_parquet(parquet_path, columns=cols)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.dropna(subset=["timestamp","content"])
    return df


def lexicon_polarity(texts: List[str]) -> np.ndarray:
    """Simple polarity score = (bull - bear) / tokens (case-insensitive)."""
    scores = np.empty(len(texts), dtype=float)
    for i, t in enumerate(texts):
        toks = [w.lower() for w in t.split()]
        if not toks:
            scores[i] = 0.0
            continue
        bull = sum(w in BULL_WORDS for w in toks)
        bear = sum(w in BEAR_WORDS for w in toks)
        scores[i] = (bull - bear) / max(1, len(toks))
    return scores


def urgency_score(texts: List[str]) -> np.ndarray:
    scores = np.empty(len(texts), dtype=float)
    for i, t in enumerate(texts):
        toks = [w.lower() for w in t.split()]
        u = sum(w in URGENCY_WORDS for w in toks)
        scores[i] = u / max(1, len(toks))
    return scores


def build_tfidf_svd(texts: List[str], max_features: int = 5000, n_components: int = 8) -> Tuple[np.ndarray, Dict]:
    """
    Build TF-IDF -> Truncated SVD components.
    Returns dense array [n_samples, n_components] and fitted artifacts.
    """
    vectorizer = TfidfVectorizer(max_features=max_features, ngram_range=(1,2), token_pattern=r"(?u)\b\w+\b")
    X = vectorizer.fit_transform(texts)  # sparse
    svd = TruncatedSVD(n_components=n_components, random_state=42)
    Z = svd.fit_transform(X)  # dense small matrix
    artifacts = {"vectorizer": vectorizer, "svd": svd}
    return Z, artifacts


def zscore(a: np.ndarray) -> np.ndarray:
    if a.ndim == 1:
        a = a[:, None]
    scaler = StandardScaler()
    return scaler.fit_transform(a).squeeze()


def bootstrap_ci(x: np.ndarray, iters: int = 200, alpha: float = 0.05, rng: np.random.Generator | None = None) -> Tuple[float, float]:
    """Non-parametric bootstrap CI for the mean."""
    if rng is None:
        rng = np.random.default_rng(42)
    x = np.asarray(x, dtype=float)
    if x.size == 0:
        return (np.nan, np.nan)
    means = np.empty(iters, dtype=float)
    n = x.size
    for i in range(iters):
        idx = rng.integers(0, n, size=n)
        means[i] = float(np.mean(x[idx]))
    lo = np.quantile(means, alpha/2)
    hi = np.quantile(means, 1 - alpha/2)
    return (float(lo), float(hi))


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    texts = df["content"].astype(str).tolist()

    # Lightweight lexical features
    df["polarity"] = lexicon_polarity(texts)
    df["urgency"]  = urgency_score(texts)

    # Engagement
    df["engagement"] = np.log1p(df["likes"] + df["retweets"] + df["replies"])

    # TF-IDF + SVD
    Z, artifacts = build_tfidf_svd(texts, max_features=5000, n_components=6)
    for i in range(Z.shape[1]):
        df[f"svd_{i+1}"] = Z[:, i]

    # Composite signal (z-scored components)
    comp = 0.4 * zscore(df["polarity"].to_numpy()) \
         + 0.3 * zscore(df["engagement"].to_numpy()) \
         + 0.3 * zscore(df["svd_1"].to_numpy())
    df["composite_signal"] = comp

    return df[["uid","timestamp","username","hashtag_primary","_hashtags_list","polarity","urgency","engagement","svd_1","composite_signal"]]


def aggregate_signals(features: pd.DataFrame, freq: str = "1H") -> pd.DataFrame:
    """Aggregate to time buckets and estimate bootstrap CIs."""
    features = features.copy()
    features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
    features = features.set_index("timestamp")
    grouped = features.groupby(pd.Grouper(freq=freq))

    agg = grouped["composite_signal"].agg(["mean","count"]).reset_index().rename(columns={"mean":"signal","count":"n"})
    # Compute confidence intervals per group
    ci_lo = []
    ci_hi = []
    for ts, sub in grouped:
        lo, hi = bootstrap_ci(sub["composite_signal"].to_numpy(), iters=200, alpha=0.05)
        ci_lo.append(lo)
        ci_hi.append(hi)
    agg["ci_lo"] = ci_lo
    agg["ci_hi"] = ci_hi
    return agg


def write_parquet(df: pd.DataFrame, out_path: Path) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path, compression="snappy")


def main():
    ap = argparse.ArgumentParser(description="Build features and composite trading signals")
    ap.add_argument("--parquet", type=Path, required=True, help="Input parquet from ingest phase")
    ap.add_argument("--out_dir", type=Path, required=True, help="Output directory for features & signals")
    ap.add_argument("--freq", type=str, default="1H", help="Aggregation frequency (e.g., 15T, 1H)")
    ap.add_argument("-v", "--verbose", action="count", default=0)
    args = ap.parse_args()

    setup_logging(args.verbose)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    df = load_minimal(args.parquet)
    logger.info("Loaded %d rows", len(df))

    feats = compute_features(df)
    write_parquet(feats, args.out_dir / "features.parquet")
    logger.info("Wrote per-tweet features -> %s", args.out_dir / "features.parquet")

    sigs = aggregate_signals(feats, freq=args.freq)
    write_parquet(sigs, args.out_dir / "signals.parquet")
    logger.info("Wrote aggregated signals -> %s", args.out_dir / "signals.parquet")

if __name__ == "__main__":
    raise SystemExit(main())
