# Advanced Twitter/X Scrapper & Signal Analysis

## Overview

This repository provides a robust pipeline for scraping Twitter/X data, cleaning and storing it efficiently, extracting features, generating composite trading signals, and visualizing results. The project is designed for research and analytical workflows, with strong anti-bot measures and memory-efficient processing.

**Main Features:**
- Advanced Playwright-based Twitter/X scraper with anti-detection techniques
- Data cleaning, normalization, and deduplication
- Feature engineering (lexical, engagement, TF-IDF/SVD)
- Composite signal construction and aggregation
- Memory-aware visualization of time-series signals
- Sample output data and analysis results included

---

## Project Structure

Even if you don't see some folders which are mentioned here, once you run the files in the order as explained here you will likely going to have the final same structure.

```
live_trade_sentiment_analyzer/
│
├── scraper.py                # Advanced Twitter/X scraper (Playwright, anti-bot)
├── ingest_clean_store.py      # Cleans, deduplicates, and stores data as Parquet
├── features_signals.py        # Feature extraction and signal aggregation
├── visualize_stream.py        # Memory-efficient plotting of signals
├── unit_test_signal.py        # Example unit test for feature extraction
├── Important_commands.txt     # Quick reference for main commands
├── requirements.txt           # Python dependencies
├── setup.bat                  # Windows setup script
├── LICENSE                    # MIT License
├── __init__.py                # Package marker
│
├── data_out/                  # Output: features, signals, metadata
│   ├── features.parquet
│   ├── signals.parquet
│   ├── ingest_metadata.json
│   └── tweets_combined.parquet
│ 
├── docs/                  
│   └── TECHNICAL_DOC.md
│
├── twitter_data/              # Sample raw scraped tweets (CSV)
├── plots/                     # Output plots (PNG)
└── venv/                  # Python virtual environment (excluded from git)
```

---

## Setup Instructions

### 1. Clone the Repository

```sh
git clone https://github.com/mayankgarg2572/live_trade_sentiment_analyzer.git
cd live_trade_sentiment_analyzer
```

### 2. Create and Activate Virtual Environment

On Windows:
```bat
setup.bat
```

Or manually:
```sh
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

### 3. Scrape Twitter/X Data

Run the scraper (manual login required):

```sh
python scrapper.py
```

### 4. Ingest and Clean Data

```sh
python ingest_clean_store.py ^
  --input_dir twitter_data ^
  --out_dir data_out ^
  --outfile tweets_combined.parquet ^
  --workers 4 ^
  -v
```

### 5. Feature Extraction & Signal Aggregation

```sh
python features_signals.py ^
  --parquet data_out/tweets_combined.parquet ^
  --out_dir data_out ^
  --freq 2h ^
  -v
```

### 6. Visualization

```sh
python visualize_stream.py ^
  --signals data_out/signals.parquet ^
  --out plots/signal_all.png ^
  -v
```

---

## Sample Output Data & Analysis

- **data_out/features.parquet**: Engineered features per tweet
- **data_out/signals.parquet**: Aggregated composite signals (time-series)
- **plots/signal_all.png**: Example signal plot
- **twitter_data/**: Example raw scraped tweets (CSV)

---

## Technical Documentation

### Approach

1. **Scraping**:  
   - Uses Playwright with advanced browser fingerprinting and human-like interaction to evade detection.
   - Supports session persistence and manual login for reliability.
   - Extracts tweet content, engagement metrics, hashtags, mentions, and URLs.

2. **Ingestion & Cleaning**:  
   - Cleans and normalizes text, timestamps, hashtags, and mentions.
   - Deduplicates tweets robustly using a stable hash.
   - Stores data in Parquet format for efficient downstream analysis.

3. **Feature Engineering**:  
   - Lexical polarity (bull/bear words), urgency scoring, engagement metrics.
   - TF-IDF vectorization and dimensionality reduction (TruncatedSVD).
   - Composite signal combines polarity, engagement, and semantic features.

4. **Signal Aggregation**:  
   - Aggregates signals over time windows (configurable frequency).
   - Computes bootstrap confidence intervals for robust analysis.

5. **Visualization**:  
   - Memory-aware plotting using chunked Parquet reads and downsampling.
   - Supports filtering by hashtag and outputs PNG plots.

### Testing

- Example unit test provided in `unit_test_signal.py` for feature extraction.

### Environment

- All dependencies are listed in `requirements.txt`.
- Virtual environment setup via `setup.bat` (Windows).

### Notes

- The `venv/` virtual environment, `data_out/`, `plots/`, and `twitter_data/` folders are excluded from git via `.gitignore`.
- Manual login is required for scraping due to Twitter/X anti-bot measures.

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

## Contact

For questions or contributions, please open an issue or pull request on GitHub.
