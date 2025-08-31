# Technical Documentation

## Pipeline Overview

1. **Scraping (`scraper.py`)**
   - Uses Playwright with stealth and anti-detection settings.
   - Human-like delays, mouse movements, and session persistence.
   - Extracts tweet data for specified hashtags.

2. **Ingestion & Cleaning (`ingest_clean_store.py`)**
   - Loads raw CSV/JSON files.
   - Normalizes text, timestamps, hashtags, mentions.
   - Deduplicates using a stable hash of username, timestamp, and content.
   - Outputs a single Parquet file and run metadata.

3. **Feature Engineering & Signal Construction (`features_signals.py`)**
   - Lexical polarity (bull/bear), urgency, engagement.
   - TF-IDF vectorization + SVD for semantic features.
   - Composite signal combines multiple features.
   - Aggregates signals by time window, computes bootstrap CIs.

4. **Visualization (`visualize_stream.py`)**
   - Reads signals from Parquet in memory-efficient chunks.
   - Downsamples for plotting.
   - Outputs PNG plots for analysis.

## Data Flow

- **Raw Data**: `twitter_data/*.csv`
- **Cleaned Data**: `data_out/tweets_combined.parquet`
- **Features**: `data_out/features.parquet`
- **Signals**: `data_out/signals.parquet`
- **Plots**: `plots/*.png`

## Key Algorithms

- **Anti-bot Scraping**: Randomized browser fingerprint, human-like interaction, session reuse.
- **Deduplication**: SHA-1 hash of key fields.
- **Feature Extraction**: Lexicon-based scoring, TF-IDF/SVD, composite signal.
- **Aggregation**: Time-based grouping, bootstrap confidence intervals.

## Extensibility

- Add new hashtags in `scraper.py`.
- Change aggregation frequency in `features_signals.py`.
- Extend feature engineering with additional NLP/statistical methods.

## Limitations

- Manual login required for scraping.
- Twitter/X anti-bot measures may change.
