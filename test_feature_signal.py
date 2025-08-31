import pandas as pd

def test_load_minimal():
    from features_signals import load_minimal
    # Use a small sample parquet for testing
    df = load_minimal("data_out/tweets_combined.parquet")
    assert not df.empty
    assert "content" in df.columns